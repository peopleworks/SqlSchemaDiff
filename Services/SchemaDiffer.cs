using System.Text;
using System.Text.RegularExpressions;
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
        var tableDiffer = new TableDiffer();

        foreach(var sourceObject in source.Objects.OrderBy(GetCreateOrder).ThenBy(x => x.Key, StringComparer.OrdinalIgnoreCase))
        {
            if(!targetByKey.TryGetValue(sourceObject.Key, out var targetObject))
            {
                deferredCreates.Add(new PendingCreate(sourceObject, EnsureTrailingGo(sourceObject.Definition)));
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
                    deferredCreates.Add(new PendingCreate(sourceObject, EnsureTrailingGo(sourceObject.Definition)));
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

            deferredCreates.Add(new PendingCreate(sourceObject, EnsureTrailingGo(ToCreateOrAlter(sourceObject))));
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

        var script = ComposeScript(source, target, createStatements, dropStatements);
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

    private static string ComposeScript(
        DatabaseSnapshot source,
        DatabaseSnapshot target,
        List<string> creates,
        List<string> drops)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"-- SQLDiff source: [{source.DatabaseName}]");
        sb.AppendLine($"-- SQLDiff target: [{target.DatabaseName}]");
        sb.AppendLine($"-- Generated (UTC): {DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine();

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

        var nodes = pendingCreates
            .GroupBy(x => x.Object.Key, StringComparer.OrdinalIgnoreCase)
            .Select(x => x.First())
            .ToDictionary(x => x.Object.Key, StringComparer.OrdinalIgnoreCase);

        var adjacency = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var inDegree = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach(var key in nodes.Keys)
        {
            adjacency[key] = new List<string>();
            inDegree[key] = 0;
        }

        foreach(var node in nodes.Values)
        {
            foreach(var dependency in node.Object.Dependencies.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                if(!nodes.ContainsKey(dependency))
                    continue;

                if(string.Equals(dependency, node.Object.Key, StringComparison.OrdinalIgnoreCase))
                    continue;

                adjacency[dependency].Add(node.Object.Key);
                inDegree[node.Object.Key]++;
            }
        }

        var ready = nodes.Values
            .Where(x => inDegree[x.Object.Key] == 0)
            .OrderBy(GetCreateOrder)
            .ThenBy(x => x.Object.Key, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var ordered = new List<PendingCreate>();
        while(ready.Count > 0)
        {
            var next = ready[0];
            ready.RemoveAt(0);
            ordered.Add(next);

            foreach(var adjacentKey in adjacency[next.Object.Key])
            {
                inDegree[adjacentKey]--;
                if(inDegree[adjacentKey] != 0)
                    continue;

                var adjacentNode = nodes[adjacentKey];
                InsertInOrder(ready, adjacentNode);
            }
        }

        var result = ordered.Select(x => x.Script).ToList();
        if(ordered.Count == nodes.Count)
            return result;

        result.Add("-- WARNING: dependency cycle detected. Remaining objects were appended in fallback order.");
        result.Add(string.Empty);

        var remaining = nodes.Values
            .Where(x => !ordered.Any(y => string.Equals(y.Object.Key, x.Object.Key, StringComparison.OrdinalIgnoreCase)))
            .OrderBy(GetCreateOrder)
            .ThenBy(x => x.Object.Key, StringComparer.OrdinalIgnoreCase);

        result.AddRange(remaining.Select(x => x.Script));
        return result;
    }

    private static void InsertInOrder(List<PendingCreate> ready, PendingCreate candidate)
    {
        var index = ready.FindIndex(x => CompareCreateNodes(candidate, x) < 0);
        if(index < 0)
            ready.Add(candidate);
        else
            ready.Insert(index, candidate);
    }

    private static int CompareCreateNodes(PendingCreate left, PendingCreate right)
    {
        var byType = GetCreateOrder(left).CompareTo(GetCreateOrder(right));
        if(byType != 0)
            return byType;

        return string.Compare(left.Object.Key, right.Object.Key, StringComparison.OrdinalIgnoreCase);
    }

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

        var definition = schemaObject.Definition.TrimStart();
        return Regex.Replace(
            definition,
            @"^\s*CREATE\s+",
            "CREATE OR ALTER ",
            RegexOptions.IgnoreCase);
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
