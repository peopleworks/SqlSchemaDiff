using System.Text;
using System.Text.RegularExpressions;
using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.Services;

/// <summary>
/// Turns a snapshot into a script that can actually be run from top to bottom.
/// <para>
/// The work is done by <see cref="ComposePhases"/>, which splits the schema into
/// dependency-ordered phases — schemas, types, sequences, tables, indexes,
/// checks, foreign keys, modules, triggers, finalize. Foreign keys are deferred
/// to their own phase so a table can reference one that is created later, and
/// everything else is ordered by <see cref="DbSchemaObject.Dependencies"/> rather
/// than by object type alone, so a view on a view or a function used by a view
/// comes out in a runnable order.
/// </para>
/// <para>
/// <see cref="ComposeFullScript"/> is that same plan concatenated into one file.
/// </para>
/// </summary>
public static class ScriptComposer
{
    private static readonly (PhaseId Id, string Name, string FileName)[] Phases =
    {
        (PhaseId.Schemas, "schemas", "010_schemas.sql"),
        (PhaseId.Types, "types", "020_types.sql"),
        (PhaseId.Sequences, "sequences", "030_sequences.sql"),
        (PhaseId.Tables, "tables", "040_tables.sql"),
        (PhaseId.Indexes, "indexes", "050_indexes.sql"),
        (PhaseId.Checks, "checks", "060_checks.sql"),
        (PhaseId.ForeignKeys, "foreign_keys", "070_foreignkeys.sql"),
        (PhaseId.Modules, "modules", "080_modules.sql"),
        (PhaseId.Triggers, "triggers", "085_triggers.sql"),
        (PhaseId.Finalize, "finalize", "090_finalize.sql")
    };

    /// <summary>An <c>ALTER TABLE ... FOREIGN KEY</c> batch, whatever else it says.</summary>
    private static readonly Regex ForeignKeyBatch = new(
        @"^\s*ALTER\s+TABLE\b.*\bFOREIGN\s+KEY\b",
        RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant);

    /// <summary>
    /// The whole snapshot as one script: header, session options, prerequisites,
    /// then every phase in order, each batch isolated by <c>GO</c>.
    /// </summary>
    public static string ComposeFullScript(DatabaseSnapshot snapshot)
    {
        var phases = ComposePhases(snapshot, ComposeOptions.Default);

        var sb = new StringBuilder();
        sb.AppendLine($"-- Snapshot database: [{snapshot.DatabaseName}]");
        sb.AppendLine($"-- Generated (UTC): {snapshot.GeneratedAtUtc:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine();
        sb.AppendLine(SqlRender.SessionOptionsPreamble);
        sb.AppendLine("GO");
        sb.AppendLine();

        // Schemas and alias types come first: every table below may depend on them,
        // and each statement is guarded so the script can be re-run safely.
        var prerequisites = phases
            .Where(x => x.Name is "schemas" or "types")
            .SelectMany(x => x.Batches)
            .ToList();

        if(prerequisites.Count > 0)
        {
            sb.AppendLine("-- Prerequisites (schemas and user-defined types)");
            sb.AppendLine("GO");
            foreach(var batch in prerequisites)
                sb.AppendLine(SqlRender.EnsureTrailingGo(batch.Sql));
            sb.AppendLine();
        }

        foreach(var phase in phases.Where(x => x.Name is not ("schemas" or "types")))
        {
            foreach(var batch in phase.Batches)
            {
                // GO after the comment so it is not stored as part of a
                // CREATE VIEW/PROCEDURE/FUNCTION definition in sys.sql_modules.
                sb.AppendLine($"-- {batch.Describe}");
                sb.AppendLine("GO");
                sb.AppendLine(SqlRender.EnsureTrailingGo(batch.Sql));
                sb.AppendLine();
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// Plans the snapshot as an ordered list of phases. Every phase is always
    /// returned, in the same order, even when it has no batches, so a caller can
    /// write one file per phase without special cases.
    /// </summary>
    public static IReadOnlyList<ScriptPhase> ComposePhases(DatabaseSnapshot snapshot, ComposeOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        options ??= ComposeOptions.Default;

        var batches = Phases.ToDictionary(x => x.Id, _ => new List<ScriptBatch>());
        void Add(PhaseId phase, ScriptBatch batch) => batches[phase].Add(batch);

        foreach(var schema in snapshot.Schemas)
        {
            Add(PhaseId.Schemas, new ScriptBatch
            {
                Describe = $"Schema {SqlRender.Quote(schema)}",
                Sql = SqlRender.BuildSchemaCreate(schema)
            });
        }

        foreach(var type in snapshot.Types)
        {
            Add(PhaseId.Types, new ScriptBatch
            {
                Describe = $"Type {SqlRender.Quote(type.Schema, type.Name)}",
                Sql = SqlRender.BuildAliasTypeCreate(type)
            });
        }

        var byPhase = snapshot.Objects
            .Where(x => x is not null)
            .GroupBy(x => PhaseForTypeName(x.Type.ToString()));

        foreach(var group in byPhase)
        {
            if(group.Key == PhaseId.Tables)
            {
                // Foreign keys are deferred, so a cycle between tables is no longer a
                // problem worth warning about; the order only has to be stable and to
                // respect the other edges (a computed column calling a function, say).
                foreach(var table in DependencyOrder.Sort(group, RankForObject).Ordered)
                    AddTableBatches(table, options, Add);

                continue;
            }

            AddDefinitionBatches(group.Key, group, Add);
        }

        return Phases
            .Select(x => new ScriptPhase
            {
                Name = x.Name,
                FileName = x.FileName,
                Batches = batches[x.Id]
            })
            .ToList();
    }

    /// <summary>
    /// Objects whose text we emit verbatim: modules, triggers, sequences, table
    /// types, and anything of a type this build does not know about.
    /// </summary>
    private static void AddDefinitionBatches(
        PhaseId phase,
        IEnumerable<DbSchemaObject> objects,
        Action<PhaseId, ScriptBatch> add)
    {
        var order = DependencyOrder.Sort(objects, RankForObject);
        var firstCycleIndex = order.Ordered.Count - order.CycleMembers.Count;

        for(var i = 0; i < order.Ordered.Count; i++)
        {
            if(order.HasCycle && i == firstCycleIndex)
            {
                // Nothing can order these correctly. Say so, and let the retry pass
                // of a restore (or a second run of the script) settle it.
                add(phase, new ScriptBatch
                {
                    Describe = "WARNING: dependency cycle detected",
                    Sql = "-- The objects below are in fallback order: they depend on each other, so no order is correct."
                });
            }

            var schemaObject = order.Ordered[i];
            add(phase, new ScriptBatch
            {
                Describe = $"{schemaObject.Type} {schemaObject.Identifier}",
                Sql = SqlRender.WrapWithModuleSessionOptions(
                    schemaObject.Definition.TrimEnd(),
                    schemaObject.UsesAnsiNulls,
                    schemaObject.UsesQuotedIdentifier),

                // A module can call a function or read a view that a later batch of
                // the same phase creates when the catalog did not record the edge
                // (dynamic SQL, a deferred name resolution). Re-running it works.
                Retryable = phase == PhaseId.Modules
            });
        }
    }

    private static void AddTableBatches(DbSchemaObject schemaObject, ComposeOptions options, Action<PhaseId, ScriptBatch> add)
    {
        var table = schemaObject.Table;
        if(table is null)
        {
            AddUnmodelledTableBatches(schemaObject, add);
            return;
        }

        var identifier = schemaObject.Identifier;
        var keyPhase = options.ConstraintsAfterData ? PhaseId.Indexes : PhaseId.Tables;
        var checkPhase = options.ConstraintsAfterData ? PhaseId.Checks : PhaseId.Tables;
        var indexPhase = options.ConstraintsAfterData ? PhaseId.Indexes : PhaseId.Tables;

        add(PhaseId.Tables, new ScriptBatch
        {
            Describe = $"Table {identifier}",
            Sql = SqlRender.BuildTableCreateOnly(table),

            // A computed column can call a scalar function that only exists after
            // the modules phase, so the table may need a second attempt.
            Retryable = table.Columns.Any(x => x.IsComputed)
        });

        foreach(var keyConstraint in table.KeyConstraints)
        {
            var kind = keyConstraint.TypeCode == "PK" ? "Primary key" : "Unique constraint";
            add(keyPhase, new ScriptBatch
            {
                Describe = Describe(kind, keyConstraint.Name, keyConstraint.IsSystemNamed, identifier),
                Sql = SqlRender.BuildKeyConstraintAdd(table, keyConstraint)
            });
        }

        foreach(var check in table.CheckConstraints)
        {
            add(checkPhase, new ScriptBatch
            {
                Describe = Describe("Check constraint", check.Name, check.IsSystemNamed, identifier),
                Sql = SqlRender.BuildCheckConstraintAdd(table, check),

                // Same reason as a computed column: the predicate can call a function.
                Retryable = true
            });

            if(check.IsDisabled && !check.IsSystemNamed)
            {
                add(checkPhase, new ScriptBatch
                {
                    Describe = Describe("Disable check constraint", check.Name, check.IsSystemNamed, identifier),
                    Sql = SqlRender.BuildConstraintNoCheck(table, check.Name)
                });
            }
        }

        foreach(var index in table.Indexes)
        {
            add(indexPhase, new ScriptBatch
            {
                Describe = $"Index {SqlRender.Quote(index.Name)} on {identifier}",
                Sql = SqlRender.BuildIndexCreate(table, index)
            });

            if(index.IsDisabled)
            {
                add(indexPhase, new ScriptBatch
                {
                    Describe = $"Disable index {SqlRender.Quote(index.Name)} on {identifier}",
                    Sql = SqlRender.BuildIndexDisable(table, index)
                });
            }
        }

        foreach(var foreignKey in table.ForeignKeys)
        {
            add(PhaseId.ForeignKeys, new ScriptBatch
            {
                Describe = Describe("Foreign key", foreignKey.Name, foreignKey.IsSystemNamed, identifier),
                Sql = SqlRender.BuildForeignKeyAdd(table, foreignKey)
            });

            if(foreignKey.IsDisabled && !foreignKey.IsSystemNamed)
            {
                add(PhaseId.ForeignKeys, new ScriptBatch
                {
                    Describe = Describe("Disable foreign key", foreignKey.Name, foreignKey.IsSystemNamed, identifier),
                    Sql = SqlRender.BuildConstraintNoCheck(table, foreignKey.Name)
                });
            }
        }
    }

    /// <summary>
    /// A table captured without a structured model — a snapshot from an older
    /// build, or one produced by another tool. All we have is its script, so it is
    /// split on <c>GO</c> and only the foreign keys are pulled out;
    /// <see cref="ComposeOptions.ConstraintsAfterData"/> cannot be honoured without
    /// the model.
    /// </summary>
    private static void AddUnmodelledTableBatches(DbSchemaObject schemaObject, Action<PhaseId, ScriptBatch> add)
    {
        foreach(var batch in SqlBatchSplitter.Split(schemaObject.Definition))
        {
            var isForeignKey = ForeignKeyBatch.IsMatch(batch);
            add(isForeignKey ? PhaseId.ForeignKeys : PhaseId.Tables, new ScriptBatch
            {
                Describe = isForeignKey
                    ? $"Foreign key on {schemaObject.Identifier}"
                    : $"Table {schemaObject.Identifier}",
                Sql = batch
            });
        }
    }

    private static string Describe(string kind, string name, bool isSystemNamed, string identifier) =>
        isSystemNamed || string.IsNullOrWhiteSpace(name)
            ? $"{kind} on {identifier}"
            : $"{kind} {SqlRender.Quote(name)} on {identifier}";

    private static int RankForObject(DbSchemaObject schemaObject) => RankForTypeName(schemaObject.Type.ToString());

    /// <summary>
    /// A coarse creation rank, used only to break ties between objects that no
    /// dependency edge separates. It is keyed on the name of the type so that
    /// object types added to <see cref="DbObjectType"/> after this build still land
    /// in a sensible place; anything unrecognised is ranked just after tables.
    /// </summary>
    internal static int RankForTypeName(string typeName) => typeName switch
    {
        "Sequence" => 10,
        "TableType" => 10,
        "Table" => 20,
        "Function" => 30,
        "View" => 40,
        "StoredProcedure" => 50,
        "Trigger" => 60,
        _ => 25
    };

    /// <summary>
    /// Which phase an object belongs to, again keyed on the name of the type so a
    /// new <see cref="DbObjectType"/> needs no change here. An unrecognised type
    /// goes last: we cannot know what it references, so we run it once everything
    /// it could reference exists.
    /// </summary>
    internal static PhaseId PhaseForTypeName(string typeName) => typeName switch
    {
        "Sequence" => PhaseId.Sequences,
        "TableType" => PhaseId.Types,
        "Table" => PhaseId.Tables,
        "Function" => PhaseId.Modules,
        "View" => PhaseId.Modules,
        "StoredProcedure" => PhaseId.Modules,
        "Trigger" => PhaseId.Triggers,
        _ => PhaseId.Finalize
    };
}

/// <summary>The phases of a schema script, in the order they must run.</summary>
internal enum PhaseId
{
    Schemas,
    Types,
    Sequences,
    Tables,
    Indexes,
    Checks,
    ForeignKeys,
    Modules,
    Triggers,
    Finalize
}
