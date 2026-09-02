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

            // Sequences: compared through the structured model so that the captured
            // current value — which moves with every use and is not drift — stays out
            // of the comparison. ALTER where SQL Server allows it, recreate where not.
            if(sourceObject.Type == DbObjectType.Sequence
                && sourceObject.Sequence is not null
                && targetObject.Sequence is not null)
            {
                var sequenceDiff = SequenceDiffer.Diff(sourceObject.Sequence, targetObject.Sequence);
                if(!sequenceDiff.HasChanges)
                    continue;

                changed++;
                changedObjects.Add(sourceObject.Identifier);
                if(addOnly)
                {
                    skipped++;
                    continue;
                }

                createInfoStatements.AddRange(sequenceDiff.Warnings);
                if(sequenceDiff.RequiresRecreate)
                {
                    dropStatements.Add(BuildDropStatement(sourceObject, includeIfExists: true));
                    deferredCreates.Add(new PendingCreate(sourceObject, EnsureTrailingGo(sourceObject.Definition)));
                }
                else
                {
                    alterStatements.Add(sequenceDiff.Script!);
                }

                emittedObjects.Add(sourceObject);
                continue;
            }

            // Table types have no ALTER of any kind, so any difference at all means
            // DROP TYPE + CREATE TYPE — and that fails while a module still uses it.
            if(sourceObject.Type == DbObjectType.TableType
                && sourceObject.TableType is not null
                && targetObject.TableType is not null)
            {
                if(TableTypesEqual(sourceObject.TableType, targetObject.TableType))
                    continue;

                changed++;
                changedObjects.Add(sourceObject.Identifier);
                if(addOnly)
                {
                    skipped++;
                    continue;
                }

                createInfoStatements.AddRange(BuildTableTypeRecreateWarnings(source, sourceObject));
                dropStatements.Add(BuildDropStatement(sourceObject, includeIfExists: true));
                deferredCreates.Add(new PendingCreate(sourceObject, EnsureTrailingGo(sourceObject.Definition)));
                emittedObjects.Add(sourceObject);
                continue;
            }

            // Programmable objects, table rebuilds, and legacy tables without a structured
            // model fall back to normalized-text comparison.
            var sourceNormalized = SchemaTextNormalizer.Normalize(sourceObject.Definition);
            var targetNormalized = SchemaTextNormalizer.Normalize(targetObject.Definition);
            var definitionsMatch = string.Equals(sourceNormalized, targetNormalized, StringComparison.Ordinal);

            // A trigger's enabled state lives outside its module text, so identical
            // definitions are not enough to call it unchanged.
            var triggerStateChange = BuildTriggerStateChange(sourceObject, targetObject);

            if(definitionsMatch && triggerStateChange is null)
                continue;

            changed++;
            changedObjects.Add(sourceObject.Identifier);
            if(addOnly)
            {
                skipped++;
                continue;
            }

            if(definitionsMatch)
            {
                // Only the enabled state moved: DISABLE/ENABLE, no need to re-run the module.
                alterStatements.Add(triggerStateChange!);
                emittedObjects.Add(sourceObject);
                continue;
            }

            if(sourceObject.Type == DbObjectType.Table)
            {
                if(allowTableRebuild)
                {
                    dropStatements.Add(BuildDropStatement(sourceObject, includeIfExists: true));
                    deferredCreates.Add(new PendingCreate(sourceObject, EnsureTrailingGo(sourceObject.Definition)));
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
        createStatements.AddRange(BuildSchemaOwnerWarnings(source, target));
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

        // A table type's columns can be alias-typed too, so both kinds are searched.
        var usedTypes = source.Types
            .Where(type => emitted.SelectMany(SqlServerSchemaExtractor.AliasTypedColumns).Any(column =>
                string.Equals(column.TypeSchema, type.Schema, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(column.TypeName, type.Name, StringComparison.OrdinalIgnoreCase)))
            .OrderBy(x => x.Schema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var schemas = emitted.Select(x => x.Schema)
            .Concat(usedTypes.Select(x => x.Schema))
            .Where(x => !string.IsNullOrWhiteSpace(x) && !string.Equals(x, "dbo", StringComparison.OrdinalIgnoreCase))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase);

        var owners = ToCaseInsensitive(source.SchemaOwners);
        var statements = new List<string>();
        statements.AddRange(schemas.Select(schema =>
            SqlRender.BuildSchemaCreate(schema, owners.GetValueOrDefault(schema))));
        statements.AddRange(usedTypes.Select(SqlRender.BuildAliasTypeCreate));
        return statements;
    }

    /// <summary>
    /// Schema ownership differences, reported and never acted on. Changing a
    /// schema's owner is <c>ALTER AUTHORIZATION</c>: a permissions change, whose
    /// principal may not even exist on the target. A diff tool says so; it does not
    /// decide it.
    /// </summary>
    private static List<string> BuildSchemaOwnerWarnings(DatabaseSnapshot source, DatabaseSnapshot target)
    {
        var warnings = new List<string>();
        if(source.SchemaOwners is null || target.SchemaOwners is null)
            return warnings;

        var targetOwners = ToCaseInsensitive(target.SchemaOwners);
        foreach(var (schema, sourceOwner) in source.SchemaOwners.OrderBy(x => x.Key, StringComparer.OrdinalIgnoreCase))
        {
            if(!targetOwners.TryGetValue(schema, out var targetOwner) ||
                string.Equals(sourceOwner, targetOwner, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            warnings.Add($"-- WARNING: schema [{schema}] is owned by [{sourceOwner}] on source and " +
                         $"[{targetOwner}] on target; ownership is reported, not changed.");
        }

        if(warnings.Count > 0)
            warnings.Add(string.Empty);

        return warnings;
    }

    /// <summary>
    /// A case-insensitive copy. Built key by key rather than through the dictionary
    /// copy constructor, which throws when a JSON-deserialized (ordinal) dictionary
    /// happens to hold two keys that differ only by case.
    /// </summary>
    private static Dictionary<string, string> ToCaseInsensitive(Dictionary<string, string>? source)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if(source is null)
            return result;

        foreach(var pair in source)
            result[pair.Key] = pair.Value;

        return result;
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
        if(schemaObject.Type is not (DbObjectType.Function or DbObjectType.StoredProcedure
            or DbObjectType.View or DbObjectType.Trigger))
        {
            return schemaObject.Definition;
        }

        return SqlModuleRewriter.ToCreateOrAlter(schemaObject.Definition);
    }

    /// <summary>
    /// The CREATE batch for an object, plus anything that has to follow it. Today
    /// that is a trigger's disabled state: it is not part of the module text, and
    /// <c>ALTER TRIGGER</c> re-enables a trigger it touches, so the DISABLE has to be
    /// re-applied after every create or alter of a disabled trigger.
    /// </summary>
    private static string BuildCreateScript(DbSchemaObject schemaObject, string definition)
    {
        var script = EnsureTrailingGo(definition);
        if(schemaObject.Type != DbObjectType.Trigger || schemaObject.Trigger is not { IsDisabled: true } trigger)
            return script;

        return script +
               SqlRender.BuildTriggerDisable(schemaObject.Schema, schemaObject.Name, trigger) +
               Environment.NewLine + "GO" + Environment.NewLine;
    }

    /// <summary>
    /// The <c>DISABLE</c>/<c>ENABLE TRIGGER</c> needed to make the target's enabled
    /// state match the source's, or null when it already does (or when either side
    /// has no structured trigger model, as on a pre-1.6 snapshot).
    /// </summary>
    private static string? BuildTriggerStateChange(DbSchemaObject source, DbSchemaObject target)
    {
        if(source.Type != DbObjectType.Trigger || source.Trigger is null || target.Trigger is null)
            return null;

        if(source.Trigger.IsDisabled == target.Trigger.IsDisabled)
            return null;

        return source.Trigger.IsDisabled
            ? SqlRender.BuildTriggerDisable(source.Schema, source.Name, source.Trigger)
            : SqlRender.BuildTriggerEnable(source.Schema, source.Name, source.Trigger);
    }

    private static bool TableTypesEqual(TableTypeModel source, TableTypeModel target) =>
        string.Equals(
            SchemaTextNormalizer.Normalize(SqlRender.BuildTableTypeCreateScript(source)),
            SchemaTextNormalizer.Normalize(SqlRender.BuildTableTypeCreateScript(target)),
            StringComparison.Ordinal);

    private static List<string> BuildTableTypeRecreateWarnings(DatabaseSnapshot source, DbSchemaObject tableType)
    {
        var dependents = source.Objects
            .Where(x => x.Dependencies.Contains(tableType.Key, StringComparer.OrdinalIgnoreCase))
            .Select(x => x.Identifier)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var warnings = new List<string>
        {
            $"-- WARNING: table type {tableType.Identifier} changed and is recreated; a table type cannot be altered.",
            "-- DROP TYPE fails while any module still references it: drop those modules first and recreate them after."
        };

        if(dependents.Count > 0)
            warnings.Add($"-- Referenced by: {string.Join(", ", dependents)}");

        warnings.Add(string.Empty);
        return warnings;
    }

    private static string BuildDropStatement(DbSchemaObject schemaObject, bool includeIfExists)
    {
        // A table type is a type, not an object: OBJECT_ID never finds one, so both
        // the guard and the DROP are spelled differently from every other kind.
        if(schemaObject.Type == DbObjectType.TableType)
        {
            return includeIfExists
                ? $"IF TYPE_ID(N'{schemaObject.Identifier}') IS NOT NULL{Environment.NewLine}" +
                  $"    DROP TYPE {schemaObject.Identifier};{Environment.NewLine}GO{Environment.NewLine}"
                : $"DROP TYPE {schemaObject.Identifier};{Environment.NewLine}GO{Environment.NewLine}";
        }

        var objectKind = schemaObject.Type switch
        {
            DbObjectType.Table => "TABLE",
            DbObjectType.View => "VIEW",
            DbObjectType.StoredProcedure => "PROCEDURE",
            DbObjectType.Function => "FUNCTION",
            DbObjectType.Trigger => "TRIGGER",
            DbObjectType.Sequence => "SEQUENCE",
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

    /// <summary>
    /// The tie-breaker the topological sort falls back on when two objects have no
    /// dependency between them. Sequences and table types come before tables and
    /// modules because a column default or a table-valued parameter cannot name one
    /// that does not exist yet; triggers come last because they need their parent
    /// table and everything the trigger body touches.
    /// </summary>
    private static int GetCreateOrder(DbSchemaObject schemaObject) => schemaObject.Type switch
    {
        DbObjectType.Sequence => 0,
        DbObjectType.TableType => 1,
        DbObjectType.Table => 2,
        DbObjectType.Function => 3,
        DbObjectType.View => 4,
        DbObjectType.StoredProcedure => 5,
        DbObjectType.Trigger => 6,
        _ => 99
    };

    private static int GetCreateOrder(PendingCreate schemaObject) => GetCreateOrder(schemaObject.Object);

    /// <summary>
    /// Drops run the other way round, so nothing is dropped while something that
    /// needs it is still there: triggers first, then modules and tables, and the
    /// sequences and table types they lean on last.
    /// </summary>
    private static int GetDropOrder(DbSchemaObject schemaObject) => schemaObject.Type switch
    {
        DbObjectType.Trigger => 0,
        DbObjectType.View => 1,
        DbObjectType.StoredProcedure => 2,
        DbObjectType.Function => 3,
        DbObjectType.Table => 4,
        DbObjectType.TableType => 5,
        DbObjectType.Sequence => 6,
        _ => 99
    };

    private sealed record PendingCreate(DbSchemaObject Object, string Script);
}
