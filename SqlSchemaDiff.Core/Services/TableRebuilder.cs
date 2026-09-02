using System.Text;
using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.Services;

/// <summary>
/// Rewrites a table that cannot be altered where it stands, and keeps its rows.
/// <para>
/// Some changes have no <c>ALTER TABLE</c> at all — adding or removing an identity,
/// moving its seed — and the only way to make them is to build the table again. The
/// naive version of that is <c>DROP TABLE</c> followed by <c>CREATE TABLE</c>, which
/// throws away every row, fails outright the moment another table has a foreign key
/// pointing at this one, and silently takes the triggers with it. This is the
/// careful version, the one SSMS generates behind its designer: a second table under
/// a temporary name, the rows copied into it, the original dropped, and the copy
/// renamed into its place.
/// </para>
/// <para>
/// What comes back across: columns and their defaults, the rows themselves (identity
/// values included), keys, checks, indexes, the table's own foreign keys, the foreign
/// keys other tables point at it, and its triggers. What does not: permissions,
/// extended properties, change tracking and full-text indexes. The generated script
/// says so at the top, because a rebuild that quietly drops a <c>GRANT</c> is worse
/// than one that refuses to run.
/// </para>
/// <para>
/// The script is only safe as one unit: between the <c>DROP TABLE</c> and the
/// <c>sp_rename</c> the table does not exist. <see cref="SqlBatchExecutor"/> wraps a
/// whole script in one transaction by default, which is exactly what this needs.
/// </para>
/// </summary>
public static class TableRebuilder
{
    /// <summary>
    /// The prefix the temporary table and its temporary default constraints carry.
    /// Greppable on purpose: a run that is killed half way through leaves objects
    /// with this in their name and nothing else does.
    /// </summary>
    public const string TempPrefix = "tmp_sqldiff_";

    /// <summary>SQL Server's limit on a regular identifier, which the temp names have to stay inside.</summary>
    private const int MaxIdentifierLength = 128;

    /// <summary>
    /// Builds the rebuild script for one table.
    /// </summary>
    /// <param name="source">The shape the table has to end up with.</param>
    /// <param name="target">
    /// The shape it has now, which is what decides which columns can be copied. Null
    /// when the target snapshot has no structured model for the table (a snapshot
    /// written before tables were modelled): the copy then uses the source's columns
    /// and the script says so.
    /// </param>
    /// <param name="sourceSnapshot">Where the inbound foreign keys and triggers to restore come from.</param>
    /// <param name="targetSnapshot">Where the inbound foreign keys to drop come from.</param>
    /// <param name="reasons">Why the table cannot be altered in place; printed at the top.</param>
    /// <param name="includeDrops">
    /// Whether the caller asked for destructive changes. A foreign key that points at
    /// this table and exists only on the target has to come down for the
    /// <c>DROP TABLE</c> either way; this decides whether it goes back up afterwards.
    /// </param>
    public static TableRebuildResult Build(
        TableModel source,
        TableModel? target,
        DatabaseSnapshot sourceSnapshot,
        DatabaseSnapshot targetSnapshot,
        IReadOnlyList<string> reasons,
        bool includeDrops)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(sourceSnapshot);
        ArgumentNullException.ThrowIfNull(targetSnapshot);

        var tableId = SqlRender.TableIdentifier(source);
        var (temp, defaultRenames) = BuildTemporaryModel(source);
        var tempId = SqlRender.TableIdentifier(temp);

        var copied = CopiedColumns(source, target);
        var identity = copied.FirstOrDefault(x => x.IsIdentity);

        var inboundFromTarget = InboundForeignKeys(targetSnapshot, source).ToList();
        var inboundFromSource = InboundForeignKeys(sourceSnapshot, source)
            .Where(x => Exists(targetSnapshot, x.Owner))
            .ToList();
        // Keys pointing at the table that the source knows nothing about. They come
        // down for the DROP either way; whether they go back up is the caller's call.
        var targetOnly = TargetOnly(inboundFromTarget, sourceSnapshot);
        var orphaned = includeDrops ? new List<InboundForeignKey>() : targetOnly;
        var triggers = TriggersOn(sourceSnapshot, source);

        var sb = new StringBuilder();
        void Batch(string sql) => AppendBatch(sb, sql);

        // ---- the header, and the CREATE in the same batch: a CREATE TABLE's text is
        // ---- not stored anywhere, so a comment in front of it costs nothing.
        Batch(BuildHeader(source, target, reasons, copied, inboundFromTarget, targetOnly, includeDrops, triggers)
              + SqlRender.BuildTableCreateOnly(temp));

        // ---- copy the rows ----
        if(copied.Count == 0)
        {
            Batch($"-- No column of {tableId} survives into the new shape, so no rows are copied.");
        }
        else
        {
            var columnList = string.Join(", ", copied.Select(x => SqlRender.Quote(x.Name)));
            if(identity is not null)
                Batch($"SET IDENTITY_INSERT {tempId} ON;");

            // HOLDLOCK TABLOCKX: one exclusive lock held to the end of the
            // transaction, so nothing writes a row into the old table after the copy
            // has read past it and before the DROP takes the table away.
            Batch($"INSERT INTO {tempId} ({columnList}){Environment.NewLine}" +
                  $"    SELECT {columnList} FROM {tableId} WITH (HOLDLOCK TABLOCKX);");

            if(identity is not null)
                Batch($"SET IDENTITY_INSERT {tempId} OFF;");
        }

        // ---- clear the way for the DROP ----
        foreach(var inbound in inboundFromTarget)
            Batch(SqlRender.BuildForeignKeyDropIfExists(inbound.Table, inbound.ForeignKey.Name));

        Batch($"DROP TABLE {tableId};");
        Batch(SqlRender.BuildObjectRename(source.Schema, temp.Name, source.Name));

        // ---- the defaults came across under temporary names; give them back ----
        foreach(var (tempName, finalName) in defaultRenames)
            Batch(SqlRender.BuildObjectRename(source.Schema, tempName, finalName, "OBJECT"));

        // ---- everything CREATE TABLE left out, in the order BuildTableCreateScript
        // ---- uses: a key before the indexes that may share its columns, and the
        // ---- foreign keys last, when there is something for them to point at.
        foreach(var key in source.KeyConstraints)
            Batch(SqlRender.BuildKeyConstraintAdd(source, key));

        foreach(var check in source.CheckConstraints)
        {
            Batch(SqlRender.BuildCheckConstraintAdd(source, check));
            if(check.IsDisabled && !check.IsSystemNamed)
                Batch(SqlRender.BuildConstraintNoCheck(source, check.Name));
        }

        foreach(var index in source.Indexes)
        {
            Batch(SqlRender.BuildIndexCreate(source, index));
            if(index.IsDisabled)
                Batch(SqlRender.BuildIndexDisable(source, index));
        }

        foreach(var foreignKey in source.ForeignKeys)
            AppendForeignKey(Batch, source, foreignKey);

        // ---- and the keys pointing back at it ----
        foreach(var inbound in inboundFromSource)
            AppendForeignKey(Batch, inbound.Table, inbound.ForeignKey);

        foreach(var inbound in orphaned)
            AppendForeignKey(Batch, inbound.Table, inbound.ForeignKey);

        // ---- DROP TABLE took the triggers with it ----
        foreach(var trigger in triggers)
        {
            Batch(SqlRender.WrapWithModuleSessionOptions(
                trigger.Definition.TrimEnd(), trigger.UsesAnsiNulls, trigger.UsesQuotedIdentifier));

            if(trigger.Trigger is { IsDisabled: true } disabled)
                Batch(SqlRender.BuildTriggerDisable(trigger.Schema, trigger.Name, disabled));
        }

        return new TableRebuildResult
        {
            Script = sb.ToString(),
            ForeignKeys = HandledForeignKeys(inboundFromTarget, inboundFromSource),
            TriggerKeys = triggers.Select(x => x.Key).ToList()
        };
    }

    // -------------------------------------------------------------- the header

    private static string BuildHeader(
        TableModel source, TableModel? target, IReadOnlyList<string> reasons,
        List<ColumnModel> copied, List<InboundForeignKey> inbound, List<InboundForeignKey> targetOnly,
        bool includeDrops, List<DbSchemaObject> triggers)
    {
        var rule = "-- " + new string('-', 74);
        var sb = new StringBuilder();
        sb.AppendLine(rule);
        sb.AppendLine($"-- REBUILD {SqlRender.TableIdentifier(source)}");
        foreach(var reason in reasons)
            sb.AppendLine($"--   * {reason}");
        if(reasons.Count == 0)
            sb.AppendLine("--   * the change cannot be expressed as an ALTER TABLE");
        sb.AppendLine("--");
        sb.AppendLine("-- The table is created again under a temporary name, its rows are copied");
        sb.AppendLine("-- across, the original is dropped and the copy is renamed into its place.");
        sb.AppendLine($"-- Rows are preserved ({copied.Count} column(s) copied" +
                      (copied.Any(x => x.IsIdentity) ? ", identity values included)." : ")."));

        if(target is null)
        {
            sb.AppendLine("-- The target snapshot carries no structured model for this table, so the");
            sb.AppendLine("-- copied columns are the source's: the INSERT fails if the target is missing");
            sb.AppendLine("-- one of them.");
        }

        if(inbound.Count > 0)
        {
            sb.AppendLine($"-- Foreign keys pointing at it are dropped and re-created: " +
                          $"{string.Join(", ", inbound.Select(x => x.ForeignKey.Name))}.");
        }

        if(targetOnly.Count > 0)
        {
            var names = string.Join(", ", targetOnly.Select(x => x.ForeignKey.Name));
            sb.AppendLine($"-- The source knows nothing about {names}.");
            sb.AppendLine(includeDrops
                ? "-- Drops were asked for, so it comes down for the rebuild and stays down."
                : "-- It is put back as the target had it, because dropping it was not asked for;");
            if(!includeDrops)
                sb.AppendLine("-- re-run with drops enabled to be rid of it.");
        }

        if(triggers.Count > 0)
            sb.AppendLine($"-- Triggers re-created: {string.Join(", ", triggers.Select(x => x.Identifier))}.");

        sb.AppendLine("--");
        sb.AppendLine("-- NOT carried over: permissions (GRANT/DENY/REVOKE), extended properties,");
        sb.AppendLine("-- change tracking and full-text indexes. Re-apply those by hand.");
        sb.AppendLine("-- Run the whole script in one transaction: between the DROP and the rename");
        sb.AppendLine("-- the table does not exist.");
        sb.AppendLine(rule);
        return sb.ToString();
    }

    // --------------------------------------------------------- the temp table

    /// <summary>
    /// The source table under its temporary name, and the default constraints that
    /// have to be renamed back afterwards.
    /// <para>
    /// A named default constraint is an object in the table's schema, and two objects
    /// in one schema cannot share a name — so creating the temporary table with the
    /// original's default constraint names fails with "there is already an object
    /// named ..." before a single row is copied. The temporary table therefore carries
    /// temporary constraint names too, and each is renamed back once the original is
    /// out of the way. The defaults have to be there for the copy itself: a column the
    /// source adds and the target does not have gets no value from the INSERT, and
    /// without its default a NOT NULL one would reject every row.
    /// </para>
    /// </summary>
    private static (TableModel Temp, List<(string TempName, string FinalName)> Renames) BuildTemporaryModel(TableModel source)
    {
        var renames = new List<(string, string)>();
        var columns = new List<ColumnModel>(source.Columns.Count);

        foreach(var column in source.Columns)
        {
            // A server-named default is rendered without a name at all, so it cannot
            // collide and there is nothing to rename back.
            if(column.DefaultIsSystemNamed ||
               string.IsNullOrWhiteSpace(column.DefaultDefinition) ||
               string.IsNullOrWhiteSpace(column.DefaultName))
            {
                columns.Add(column);
                continue;
            }

            var temporary = column.Clone();
            temporary.DefaultName = TempName(column.DefaultName!);
            columns.Add(temporary);
            renames.Add((temporary.DefaultName, column.DefaultName!));
        }

        var temp = source.Clone();
        temp.Name = TempName(source.Name);
        temp.Columns = columns;
        return (temp, renames);
    }

    /// <summary>
    /// <see cref="TempPrefix"/> in front of a name, trimmed to what SQL Server accepts
    /// as an identifier. Only a name already within 12 characters of the limit is
    /// touched, and the rename back uses the original, so the trim never reaches the
    /// finished table.
    /// </summary>
    private static string TempName(string name)
    {
        var candidate = TempPrefix + name;
        return candidate.Length <= MaxIdentifierLength ? candidate : candidate[..MaxIdentifierLength];
    }

    // ------------------------------------------------------------- the columns

    /// <summary>
    /// The columns the INSERT can carry: on both sides, not computed — SQL Server
    /// works those out for itself and refuses a value — and not a rowversion, which
    /// the engine stamps and no statement is allowed to write.
    /// </summary>
    private static List<ColumnModel> CopiedColumns(TableModel source, TableModel? target)
    {
        var targetColumns = target is null
            ? null
            : new HashSet<string>(target.Columns.Select(x => x.Name), StringComparer.OrdinalIgnoreCase);

        return source.Columns
            .Where(x => !x.IsComputed && !IsRowVersion(x))
            .Where(x => targetColumns is null || targetColumns.Contains(x.Name))
            .ToList();
    }

    private static bool IsRowVersion(ColumnModel column) =>
        !column.IsUserDefinedType &&
        (string.Equals(column.TypeName, "timestamp", StringComparison.OrdinalIgnoreCase) ||
         string.Equals(column.TypeName, "rowversion", StringComparison.OrdinalIgnoreCase));

    // ------------------------------------------------------ the keys and triggers

    /// <summary>
    /// Every foreign key in a snapshot that points at this table from another one, in
    /// a stable order. A key the table points at itself is left out: <c>DROP TABLE</c>
    /// takes it away with the table, and the rebuild re-creates it with the table's
    /// own keys.
    /// </summary>
    private static IEnumerable<InboundForeignKey> InboundForeignKeys(DatabaseSnapshot snapshot, TableModel table)
    {
        foreach(var owner in snapshot.Objects
                    .Where(x => x.Type == DbObjectType.Table && x.Table is not null)
                    .OrderBy(x => x.Key, StringComparer.OrdinalIgnoreCase))
        {
            if(Same(owner.Schema, table.Schema) && Same(owner.Name, table.Name))
                continue;

            foreach(var foreignKey in owner.Table!.ForeignKeys)
            {
                if(Same(foreignKey.ReferencedSchema, table.Schema) && Same(foreignKey.ReferencedTable, table.Name))
                    yield return new InboundForeignKey(owner, owner.Table!, foreignKey);
            }
        }
    }

    /// <summary>
    /// The inbound keys the target has and the source does not. They still have to
    /// come down for the <c>DROP TABLE</c>; putting them back is what stops a rebuild
    /// from quietly deleting a constraint nobody asked to delete.
    /// </summary>
    private static List<InboundForeignKey> TargetOnly(List<InboundForeignKey> fromTarget, DatabaseSnapshot sourceSnapshot)
    {
        var sourceTables = sourceSnapshot.Objects
            .Where(x => x.Type == DbObjectType.Table && x.Table is not null)
            .ToDictionary(x => x.Key, x => x.Table!, StringComparer.OrdinalIgnoreCase);

        return fromTarget
            .Where(x => !sourceTables.TryGetValue(x.Owner.Key, out var sourceTable) ||
                        sourceTable.ForeignKeys.All(fk =>
                            !string.Equals(
                                TableDiffer.ForeignKeyMatchKey(fk),
                                TableDiffer.ForeignKeyMatchKey(x.ForeignKey),
                                StringComparison.OrdinalIgnoreCase)))
            .ToList();
    }

    private static List<DbSchemaObject> TriggersOn(DatabaseSnapshot snapshot, TableModel table) =>
        snapshot.Objects
            .Where(x => x.Type == DbObjectType.Trigger && x.Trigger is not null &&
                        Same(x.Trigger.ParentSchema, table.Schema) &&
                        Same(x.Trigger.ParentName, table.Name))
            .OrderBy(x => x.Key, StringComparer.OrdinalIgnoreCase)
            .ToList();

    /// <summary>
    /// Everything the rebuild took responsibility for, so the tables on the other end
    /// of those keys leave them alone. Both sides go in: the source's key is the one
    /// re-created, the target's is the one dropped, and for a server-named key those
    /// are not the same identity when the shape moved.
    /// </summary>
    private static List<HandledForeignKey> HandledForeignKeys(
        List<InboundForeignKey> fromTarget, List<InboundForeignKey> fromSource) =>
        fromTarget.Concat(fromSource)
            .Select(x => new HandledForeignKey(x.Owner.Key, TableDiffer.ForeignKeyMatchKey(x.ForeignKey)))
            .Distinct()
            .ToList();

    private static void AppendForeignKey(Action<string> batch, TableModel table, ForeignKeyModel foreignKey)
    {
        batch(SqlRender.BuildForeignKeyAdd(table, foreignKey));

        // ADD leaves a foreign key switched on, whatever WITH CHECK / WITH NOCHECK
        // said about validating the rows already there.
        if(foreignKey.IsDisabled && !foreignKey.IsSystemNamed)
            batch(SqlRender.BuildConstraintNoCheck(table, foreignKey.Name));
    }

    // ------------------------------------------------------------------ plumbing

    private static bool Exists(DatabaseSnapshot snapshot, DbSchemaObject candidate) =>
        snapshot.Objects.Any(x => string.Equals(x.Key, candidate.Key, StringComparison.OrdinalIgnoreCase));

    private static bool Same(string? a, string? b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Appends one statement and makes sure it ends on its own <c>GO</c>, so a
    /// <c>CREATE TRIGGER</c> — which SQL Server insists is the first statement of its
    /// batch, and whose whole batch text it stores as the module's definition — never
    /// shares a batch with anything else.
    /// </summary>
    private static void AppendBatch(StringBuilder sb, string sql)
    {
        var text = sql.TrimEnd();
        if(text.Length == 0)
            return;

        sb.AppendLine(text);

        var lastLine = text.Replace("\r\n", "\n").Split('\n')[^1].Trim();
        if(!string.Equals(lastLine, "GO", StringComparison.OrdinalIgnoreCase))
            sb.AppendLine("GO");
    }

    private sealed record InboundForeignKey(DbSchemaObject Owner, TableModel Table, ForeignKeyModel ForeignKey);
}

/// <summary>What <see cref="TableRebuilder.Build"/> produced, and what it took on.</summary>
public sealed class TableRebuildResult
{
    /// <summary>The rebuild, as a <c>GO</c>-separated script.</summary>
    public string Script { get; init; } = string.Empty;

    /// <summary>
    /// Foreign keys on <em>other</em> tables that this rebuild dropped and put back.
    /// The differ hands each owning table its own entries through
    /// <see cref="TableDiffOptions.ForeignKeysHandledElsewhere"/>, so the same key is
    /// not added a second time — or dropped, when it is already gone.
    /// </summary>
    public IReadOnlyList<HandledForeignKey> ForeignKeys { get; init; } = Array.Empty<HandledForeignKey>();

    /// <summary>
    /// <see cref="DbSchemaObject.Key"/> of every trigger this rebuild re-created.
    /// <c>DROP TABLE</c> took them with the table, so the rebuild owns them and the
    /// triggers' own diff has nothing left to do.
    /// </summary>
    public IReadOnlyList<string> TriggerKeys { get; init; } = Array.Empty<string>();
}

/// <summary>
/// One foreign key a rebuild dealt with: the <see cref="DbSchemaObject.Key"/> of the
/// table it belongs to, and the key's own
/// <see cref="TableDiffer.ForeignKeyMatchKey"/>.
/// </summary>
public sealed record HandledForeignKey(string TableKey, string MatchKey);
