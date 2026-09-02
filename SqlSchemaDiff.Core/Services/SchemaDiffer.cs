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

        // Which tables have to be rebuilt is worked out before anything is emitted,
        // because a rebuild reaches outside its own table: it drops the foreign keys
        // pointing at it and re-creates the triggers on it, and the objects on the
        // other end of that have to know not to do the same work again.
        var rebuilds = PlanRebuilds(source, target, targetByKey, includeDrops, allowTableRebuild, tableDiffer);

        foreach(var sourceObject in source.Objects.OrderBy(GetCreateOrder).ThenBy(x => x.Key, StringComparer.OrdinalIgnoreCase))
        {
            // A rebuild drops its table, and DROP TABLE takes every trigger on it, so
            // the rebuild re-creates them from the source. Whatever this trigger's own
            // diff would say, the rebuild has already said it.
            if(rebuilds.HandlesTrigger(sourceObject.Key))
                continue;

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
            // reordering columns would require a table rebuild.
            //
            // A table the planner marked for rebuild skips the ALTER entirely and emits
            // the rebuild instead. The rebuild goes out with the creates rather than the
            // alters so the dependency sort places it: it has to run before any new
            // table that points a foreign key at it, or the DROP inside it would find
            // that key in the way.
            if(sourceObject.Type == DbObjectType.Table
                && sourceObject.Table is not null
                && targetObject.Table is not null)
            {
                var rebuild = rebuilds.Find(sourceObject.Key);
                TableDiffer.TableAlterResult? alter = null;

                if(rebuild is null)
                {
                    alter = tableDiffer.Diff(sourceObject.Table, targetObject.Table, includeDrops,
                        rebuilds.OptionsFor(sourceObject.Key));
                    if(!alter.HasChanges && alter.WarningCount == 0)
                        continue; // structurally identical; ignore cosmetic text differences
                }

                changed++;
                changedObjects.Add(sourceObject.Identifier);
                if(addOnly)
                {
                    skipped++;
                    continue;
                }

                if(rebuild is not null)
                    deferredCreates.Add(new PendingCreate(sourceObject, rebuild.Script));
                else
                    alterStatements.Add(alter!.Script);

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

            // Programmable objects, and legacy tables without a structured model on one
            // side or the other, fall back to normalized-text comparison.
            var definitionsMatch = DefinitionsMatch(sourceObject, targetObject);

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
                var fallbackRebuild = rebuilds.Find(sourceObject.Key);
                if(fallbackRebuild is not null)
                {
                    // The source has a model even though the target does not, which is
                    // enough to build the new table and copy what the two have in common.
                    deferredCreates.Add(new PendingCreate(sourceObject, fallbackRebuild.Script));
                    emittedObjects.Add(sourceObject);
                }
                else if(allowTableRebuild)
                {
                    // Neither side has a model: there is no shape to build a copy from
                    // and no column list to copy through, so all that is left is the
                    // destructive form — said out loud rather than done quietly.
                    createInfoStatements.Add($"-- WARNING: {sourceObject.Identifier} is dropped and recreated, and its rows are lost.");
                    createInfoStatements.Add("-- The snapshot holds no structured model for it, so there is nothing to copy the rows into.");
                    createInfoStatements.Add(string.Empty);
                    dropStatements.Add(BuildDropStatement(sourceObject, includeIfExists: true));
                    deferredCreates.Add(new PendingCreate(sourceObject, BuildCreateScript(sourceObject, sourceObject.Definition)));
                    emittedObjects.Add(sourceObject);
                }
                else
                {
                    // No structured model available (e.g. legacy snapshot): fall back to skip.
                    skipped++;
                    createInfoStatements.Add($"-- WARNING: table changed and was skipped: {sourceObject.Identifier}");
                    createInfoStatements.Add("-- Use --allow-table-rebuild to rebuild the table around its rows.");
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
    /// Works out, before a single statement is emitted, which tables cannot be
    /// reconciled in place and have to be built again around their rows.
    /// <para>
    /// This runs first because a rebuild is not a local change. It drops every foreign
    /// key pointing at the table so the <c>DROP TABLE</c> can go through, puts them
    /// back from the source afterwards, and re-creates the triggers the drop took with
    /// it. The tables and triggers on the other end of all that are diffed later in the
    /// same run, and they need to know the work is already accounted for — otherwise
    /// they add a foreign key that is already there, or drop one that is already gone.
    /// </para>
    /// <para>
    /// Nothing is planned unless the caller asked for it: without
    /// <c>allowTableRebuild</c> a table the differ cannot express is reported and left
    /// alone, which is the whole point of the flag.
    /// </para>
    /// </summary>
    private static RebuildPlan PlanRebuilds(
        DatabaseSnapshot source,
        DatabaseSnapshot target,
        Dictionary<string, DbSchemaObject> targetByKey,
        bool includeDrops,
        bool allowTableRebuild,
        TableDiffer tableDiffer)
    {
        var plan = new RebuildPlan();
        if(!allowTableRebuild)
            return plan;

        foreach(var sourceObject in source.Objects
                    .Where(x => x.Type == DbObjectType.Table && x.Table is not null)
                    .OrderBy(x => x.Key, StringComparer.OrdinalIgnoreCase))
        {
            // Missing on the target: a plain CREATE, with no rows to preserve.
            if(!targetByKey.TryGetValue(sourceObject.Key, out var targetObject))
                continue;

            List<string> reasons;
            if(targetObject.Table is not null)
            {
                var alter = tableDiffer.Diff(sourceObject.Table!, targetObject.Table, includeDrops);
                if(!alter.RequiresRebuild)
                    continue;

                reasons = alter.RebuildReasons.ToList();
            }
            else
            {
                if(DefinitionsMatch(sourceObject, targetObject))
                    continue;

                reasons = new List<string>
                {
                    "the target snapshot holds no structured model for this table, so the change could not be read column by column"
                };
            }

            plan.Add(sourceObject.Key, TableRebuilder.Build(
                sourceObject.Table!, targetObject.Table, source, target, reasons, includeDrops));
        }

        return plan;
    }

    private static bool DefinitionsMatch(DbSchemaObject source, DbSchemaObject target) =>
        string.Equals(
            SchemaTextNormalizer.Normalize(source.Definition),
            SchemaTextNormalizer.Normalize(target.Definition),
            StringComparison.Ordinal);

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
    /// The CREATE batch for an object, plus anything that has to surround or follow
    /// it. A module created under non-default <c>ANSI_NULLS</c> or
    /// <c>QUOTED_IDENTIFIER</c> is wrapped in the matching <c>SET</c> batches, because
    /// SQL Server records those settings at create time and re-applies them on every
    /// run. A disabled trigger gets its <c>DISABLE</c> re-applied after every create
    /// or alter, since the state is not part of the module text and
    /// <c>ALTER TRIGGER</c> re-enables what it touches.
    /// </summary>
    private static string BuildCreateScript(DbSchemaObject schemaObject, string definition)
    {
        var script = EnsureTrailingGo(SqlRender.WrapWithModuleSessionOptions(
            definition,
            schemaObject.UsesAnsiNulls,
            schemaObject.UsesQuotedIdentifier));
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

    /// <summary>
    /// The rebuilds one run will emit, and the work they take off everything else's
    /// hands.
    /// </summary>
    private sealed class RebuildPlan
    {
        private readonly Dictionary<string, TableRebuildResult> _rebuilds = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, HashSet<string>> _foreignKeys = new(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _triggers = new(StringComparer.OrdinalIgnoreCase);

        public void Add(string tableKey, TableRebuildResult rebuild)
        {
            _rebuilds[tableKey] = rebuild;

            foreach(var foreignKey in rebuild.ForeignKeys)
            {
                if(!_foreignKeys.TryGetValue(foreignKey.TableKey, out var keys))
                    _foreignKeys[foreignKey.TableKey] = keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                keys.Add(foreignKey.MatchKey);
            }

            foreach(var trigger in rebuild.TriggerKeys)
                _triggers.Add(trigger);
        }

        /// <summary>The rebuild planned for a table, or null when it is diffed normally.</summary>
        public TableRebuildResult? Find(string tableKey) => _rebuilds.GetValueOrDefault(tableKey);

        /// <summary>True when a rebuild re-creates this trigger, so its own diff has nothing to say.</summary>
        public bool HandlesTrigger(string triggerKey) => _triggers.Contains(triggerKey);

        /// <summary>
        /// What a table's own diff should leave alone, or null when a rebuild elsewhere
        /// did not touch it — which is the case for all but a handful of tables, and
        /// keeps the common path allocating nothing.
        /// </summary>
        public TableDiffOptions? OptionsFor(string tableKey) =>
            _foreignKeys.TryGetValue(tableKey, out var keys)
                ? new TableDiffOptions { ForeignKeysHandledElsewhere = keys }
                : null;
    }
}
