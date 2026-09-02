using System.Text;
using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.Services;

public sealed class SchemaDiffer
{
    public DiffResult Diff(
        DatabaseSnapshot source,
        DatabaseSnapshot target,
        bool includeDrops,
        bool includeTableDrops,
        bool allowTableRebuild,
        bool addOnly)
    {
        var sourceByKey = source.Objects.ToDictionary(x => x.Key, StringComparer.OrdinalIgnoreCase);
        var targetByKey = target.Objects.ToDictionary(x => x.Key, StringComparer.OrdinalIgnoreCase);

        var added = 0;
        var changed = 0;
        var removed = 0;
        var skipped = 0;
        var addedObjects = new List<string>();
        var changedObjects = new List<string>();
        var removedObjects = new List<string>();
        var deferredCreates = new List<PendingCreate>();
        var createInfoStatements = new List<string>();
        var dropStatements = new List<string>();
        var alterStatements = new List<string>();
        var emittedObjects = new List<DbSchemaObject>();
        var tableDiffer = new TableDiffer();

        foreach(var sourceObject in source.Objects.OrderBy(GetCreateOrder).ThenBy(x => x.Key, StringComparer.OrdinalIgnoreCase))
        {
            if(!targetByKey.TryGetValue(sourceObject.Key, out var targetObject))
            {
                deferredCreates.Add(new PendingCreate(sourceObject, BuildCreateScript(sourceObject, sourceObject.Definition)));
                emittedObjects.Add(sourceObject);
                added++;
                addedObjects.Add(sourceObject.Identifier);
                continue;
            }

            // Tables with structured models on both sides: decide "changed" by the actual
            // structural diff, not by text. This way cosmetic-only differences (e.g. the
            // same columns in a different physical order) are NOT reported as drift, since
            // reordering columns would require a destructive table rebuild.
            if(sourceObject.Type == DbObjectType.Table
                && !allowTableRebuild
                && sourceObject.Table is not null
                && targetObject.Table is not null)
            {
                var alter = tableDiffer.Diff(sourceObject.Table, targetObject.Table, includeDrops);
                if(!alter.HasChanges && alter.WarningCount == 0)
                    continue; // structurally identical; ignore cosmetic text differences

                changed++;
                changedObjects.Add(sourceObject.Identifier);
                if(addOnly)
                {
                    skipped++;
                    continue;
                }

                alterStatements.Add(alter.Script);
                emittedObjects.Add(sourceObject);
                continue;
            }

            // Programmable objects, table rebuilds, and legacy tables without a structured
            // model fall back to normalized-text comparison.
            var sourceNormalized = SchemaTextNormalizer.Normalize(sourceObject.Definition);
            var targetNormalized = SchemaTextNormalizer.Normalize(targetObject.Definition);
            if(string.Equals(sourceNormalized, targetNormalized, StringComparison.Ordinal))
                continue;

            changed++;
            changedObjects.Add(sourceObject.Identifier);
            if(addOnly)
            {
                skipped++;
                continue;
            }

            if(sourceObject.Type == DbObjectType.Table)
            {
                if(allowTableRebuild)
                {
                    dropStatements.Add(BuildDropStatement(sourceObject, includeIfExists: true));
                    deferredCreates.Add(new PendingCreate(sourceObject, BuildCreateScript(sourceObject, sourceObject.Definition)));
                    emittedObjects.Add(sourceObject);
                }
                else
                {
                    // No structured model available (e.g. legacy snapshot): fall back to skip.
                    skipped++;
                    createInfoStatements.Add($"-- WARNING: table changed and was skipped: {sourceObject.Identifier}");
                    createInfoStatements.Add("-- Use --allow-table-rebuild to generate DROP/CREATE (can cause data loss).");
                    createInfoStatements.Add(string.Empty);
                }

                continue;
            }

            deferredCreates.Add(new PendingCreate(sourceObject, BuildCreateScript(sourceObject, ToCreateOrAlter(sourceObject))));
            emittedObjects.Add(sourceObject);
        }

        if(includeDrops && !addOnly)
        {
            foreach(var targetObject in target.Objects.OrderBy(GetDropOrder).ThenBy(x => x.Key, StringComparer.OrdinalIgnoreCase))
            {
                if(sourceByKey.ContainsKey(targetObject.Key))
                    continue;

                if(targetObject.Type == DbObjectType.Table && !includeTableDrops)
                {
                    skipped++;
                    dropStatements.Add($"-- WARNING: table exists only on target and was not dropped: {targetObject.Identifier}");
                    dropStatements.Add("-- Use --include-table-drops to generate DROP TABLE.");
                    dropStatements.Add(string.Empty);
                    continue;
                }

                dropStatements.Add(BuildDropStatement(targetObject, includeIfExists: true));
                removed++;
                removedObjects.Add(targetObject.Identifier);
            }
        }
        else if(includeDrops && addOnly)
        {
            skipped++;
            createInfoStatements.Add("-- INFO: --include-drops ignored because --add-only was specified.");
            createInfoStatements.Add(string.Empty);
        }

        var createStatements = new List<string>();
        createStatements.AddRange(createInfoStatements);
        createStatements.AddRange(OrderCreateStatementsByDependencies(deferredCreates));
        createStatements.AddRange(alterStatements);

        var prerequisites = BuildPrerequisites(source, emittedObjects);
        var script = ComposeScript(source, target, prerequisites, createStatements, dropStatements);
        return new DiffResult
        {
            Script = script,
            Added = added,
            Changed = changed,
            Removed = removed,
            Skipped = skipped,
            AddedObjects = addedObjects,
            ChangedObjects = changedObjects,
            RemovedObjects = removedObjects
        };
    }

    /// <summary>
    /// Schemas and alias types the emitted objects depend on. A table in a schema
    /// the target does not have, or typed with an alias type it does not know,
    /// cannot be created at all — so these go out first, each guarded so the script
    /// stays re-runnable.
    /// </summary>
    private static List<string> BuildPrerequisites(DatabaseSnapshot source, List<DbSchemaObject> emitted)
    {
        if(emitted.Count == 0)
            return new List<string>();

        var usedTypes = source.Types
            .Where(type => emitted.Any(o => o.Table is not null && o.Table.Columns.Any(column =>
                column.IsUserDefinedType &&
                string.Equals(column.TypeSchema, type.Schema, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(column.TypeName, type.Name, StringComparison.OrdinalIgnoreCase))))
            .OrderBy(x => x.Schema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var schemas = emitted.Select(x => x.Schema)
            .Concat(usedTypes.Select(x => x.Schema))
            .Where(x => !string.IsNullOrWhiteSpace(x) && !string.Equals(x, "dbo", StringComparison.OrdinalIgnoreCase))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase);

        var statements = new List<string>();
        statements.AddRange(schemas.Select(SqlRender.BuildSchemaCreate));
        statements.AddRange(usedTypes.Select(SqlRender.BuildAliasTypeCreate));
        return statements;
    }

    private static string ComposeScript(
        DatabaseSnapshot source,
        DatabaseSnapshot target,
        List<string> prerequisites,
        List<string> creates,
        List<string> drops)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"-- SQLDiff source: [{source.DatabaseName}]");
        sb.AppendLine($"-- SQLDiff target: [{target.DatabaseName}]");
        sb.AppendLine($"-- Generated (UTC): {DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine();
        sb.AppendLine(SqlRender.SessionOptionsPreamble);
        sb.AppendLine("GO");
        sb.AppendLine();

        if(prerequisites.Count > 0)
        {
            sb.AppendLine("-- Prerequisites (schemas and user-defined types)");
            sb.AppendLine("GO");
            foreach(var statement in prerequisites)
                AppendBatch(sb, statement);
            sb.AppendLine();
        }

        if(drops.Count > 0)
        {
            sb.AppendLine("-- Drops");
            sb.AppendLine("GO");
            foreach(var statement in drops)
                AppendBatch(sb, statement);
            sb.AppendLine();
        }

        if(creates.Count > 0)
        {
            sb.AppendLine("-- Creates/Alters");
            sb.AppendLine("GO");
            foreach(var statement in creates)
                AppendBatch(sb, statement);
        }

        return sb.ToString();
    }

    /// <summary>
    /// Appends a statement block and guarantees it ends on its own <c>GO</c> so it
    /// executes as an isolated batch. This is critical for CREATE VIEW/PROCEDURE/
    /// FUNCTION: SQL Server stores the entire batch text as the object definition,
    /// so any preceding comment would be baked into sys.sql_modules and cause the
    /// object to diff forever. Isolating each batch keeps definitions clean.
    /// </summary>
    private static void AppendBatch(StringBuilder sb, string block)
    {
        if(string.IsNullOrWhiteSpace(block))
        {
            sb.AppendLine();
            return;
        }

        sb.AppendLine(block);

        var lastLine = block.Replace("\r\n", "\n").TrimEnd().Split('\n').Last().Trim();
        if(!string.Equals(lastLine, "GO", StringComparison.OrdinalIgnoreCase))
            sb.AppendLine("GO");
    }

    private static List<string> OrderCreateStatementsByDependencies(List<PendingCreate> pendingCreates)
    {
        if(pendingCreates.Count == 0)
            return new List<string>();

        var order = DependencyOrder.Sort(
            pendingCreates,
            x => x.Object.Key,
            x => x.Object.Dependencies,
            GetCreateOrder);

        var placedCount = order.Ordered.Count - order.CycleMembers.Count;
        var result = order.Ordered.Take(placedCount).Select(x => x.Script).ToList();
        if(!order.HasCycle)
            return result;

        result.Add("-- WARNING: dependency cycle detected. Remaining objects were appended in fallback order.");
        result.Add(string.Empty);
        result.AddRange(order.CycleMembers.Select(x => x.Script));
        return result;
    }

    /// <summary>
    /// The text to run for one object, wrapped in the <c>SET</c> options the module
    /// was created with when those are not the script defaults, and closed by its
    /// own <c>GO</c>.
    /// </summary>
    private static string BuildCreateScript(DbSchemaObject schemaObject, string sql) =>
        EnsureTrailingGo(SqlRender.WrapWithModuleSessionOptions(
            sql,
            schemaObject.UsesAnsiNulls,
            schemaObject.UsesQuotedIdentifier));

    private static string EnsureTrailingGo(string sql)
    {
        var trimmed = sql.TrimEnd();
        if(trimmed.EndsWith("\nGO", StringComparison.OrdinalIgnoreCase) ||
            trimmed.EndsWith("\r\nGO", StringComparison.OrdinalIgnoreCase))
        {
            return trimmed + Environment.NewLine;
        }

        return trimmed + Environment.NewLine + "GO" + Environment.NewLine;
    }

    private static string ToCreateOrAlter(DbSchemaObject schemaObject)
    {
        if(schemaObject.Type is not (DbObjectType.Function or DbObjectType.StoredProcedure or DbObjectType.View))
            return schemaObject.Definition;

        return SqlModuleRewriter.ToCreateOrAlter(schemaObject.Definition);
    }

    private static string BuildDropStatement(DbSchemaObject schemaObject, bool includeIfExists)
    {
        var objectKind = schemaObject.Type switch
        {
            DbObjectType.Table => "TABLE",
            DbObjectType.View => "VIEW",
            DbObjectType.StoredProcedure => "PROCEDURE",
            DbObjectType.Function => "FUNCTION",
            _ => throw new InvalidOperationException($"Unsupported object type: {schemaObject.Type}")
        };

        if(includeIfExists)
        {
            return
                $"IF OBJECT_ID(N'{schemaObject.Identifier}') IS NOT NULL{Environment.NewLine}" +
                $"    DROP {objectKind} {schemaObject.Identifier};{Environment.NewLine}GO{Environment.NewLine}";
        }

        return $"DROP {objectKind} {schemaObject.Identifier};{Environment.NewLine}GO{Environment.NewLine}";
    }

    private static int GetCreateOrder(DbSchemaObject schemaObject) => schemaObject.Type switch
    {
        DbObjectType.Table => 0,
        DbObjectType.Function => 1,
        DbObjectType.View => 2,
        DbObjectType.StoredProcedure => 3,
        _ => 99
    };

    private static int GetCreateOrder(PendingCreate schemaObject) => GetCreateOrder(schemaObject.Object);

    private static int GetDropOrder(DbSchemaObject schemaObject) => schemaObject.Type switch
    {
        DbObjectType.View => 0,
        DbObjectType.StoredProcedure => 1,
        DbObjectType.Function => 2,
        DbObjectType.Table => 3,
        _ => 99
    };

    private sealed record PendingCreate(DbSchemaObject Object, string Script);
}
