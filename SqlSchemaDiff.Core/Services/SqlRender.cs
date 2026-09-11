using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.RegularExpressions;
using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.Services;

/// <summary>
/// Shared SQL text rendering for table models. Used both by the extractor (to
/// produce the full CREATE TABLE script) and by the column-level differ (to
/// render ALTER fragments), so both sides emit byte-identical column syntax.
/// </summary>
public static class SqlRender
{
    /// <summary>
    /// Prefix every generated script with the SET options SQL Server requires for
    /// filtered indexes, indexed views and persisted computed columns. The .NET
    /// client sets these already, but a script run through sqlcmd or SSMS does not
    /// inherit them, and QUOTED_IDENTIFIER OFF makes those objects fail to create.
    /// </summary>
    public const string SessionOptionsPreamble = "SET ANSI_NULLS ON;\r\nSET QUOTED_IDENTIFIER ON;";

    /// <summary>
    /// Guarantees the text ends on its own <c>GO</c>, so it executes as an isolated
    /// batch. Critical for CREATE VIEW/PROCEDURE/FUNCTION: SQL Server stores the
    /// whole batch as the object definition.
    /// </summary>
    public static string EnsureTrailingGo(string sql)
    {
        var trimmed = sql.TrimEnd();
        if(Regex.IsMatch(trimmed, @"(^|\r?\n)\s*GO\s*$", RegexOptions.IgnoreCase))
            return trimmed;

        return $"{trimmed}{Environment.NewLine}GO";
    }

    /// <summary>
    /// Wraps a module in the <c>SET</c> options it was created with, when they are
    /// not the defaults the script already establishes.
    /// <para>
    /// SQL Server records <c>ANSI_NULLS</c> and <c>QUOTED_IDENTIFIER</c> per module
    /// and re-applies them whenever the module runs, so a module created with one
    /// of them OFF behaves differently from the same text created with it ON. The
    /// options take effect when the *next* batch is parsed, which is why the
    /// wrapper is <c>GO</c>-separated rather than part of the module's own batch.
    /// The flags are re-set to ON afterwards so the rest of the script keeps the
    /// preamble's defaults.
    /// </para>
    /// <para>
    /// A null flag means "unknown" (a snapshot taken before these were captured)
    /// and is treated as ON, so the text is returned unchanged.
    /// </para>
    /// </summary>
    public static string WrapWithModuleSessionOptions(string moduleSql, bool? usesAnsiNulls, bool? usesQuotedIdentifier)
    {
        var ansiNullsOff = usesAnsiNulls == false;
        var quotedIdentifierOff = usesQuotedIdentifier == false;
        if(!ansiNullsOff && !quotedIdentifierOff)
            return moduleSql;

        var sb = new StringBuilder();
        if(ansiNullsOff)
            sb.AppendLine("SET ANSI_NULLS OFF;");
        if(quotedIdentifierOff)
            sb.AppendLine("SET QUOTED_IDENTIFIER OFF;");
        sb.AppendLine("GO");
        sb.AppendLine(EnsureTrailingGo(moduleSql));
        if(ansiNullsOff)
            sb.AppendLine("SET ANSI_NULLS ON;");
        if(quotedIdentifierOff)
            sb.AppendLine("SET QUOTED_IDENTIFIER ON;");
        sb.Append("GO");
        return sb.ToString();
    }

    public static string Quote(string name) => $"[{name.Replace("]", "]]")}]";

    public static string Quote(string schema, string name) => $"{Quote(schema)}.{Quote(name)}";

    public static string TableIdentifier(TableModel table) => Quote(table.Schema, table.Name);

    /// <summary>Escapes a string for embedding inside a single-quoted T-SQL literal.</summary>
    public static string Literal(string value) => value.Replace("'", "''");

    /// <summary>
    /// A guarded <c>CREATE SCHEMA</c>. <c>CREATE SCHEMA</c> must be the first
    /// statement in its batch, so it is wrapped in <c>EXEC</c> to stay inside the
    /// <c>IF</c>.
    /// </summary>
    public static string BuildSchemaCreate(string schema) => BuildSchemaCreate(schema, null);

    /// <summary>
    /// A guarded <c>CREATE SCHEMA</c> that names the owner when one is known.
    /// <para>
    /// <c>AUTHORIZATION</c> is omitted for <c>dbo</c>: that is the default owner, and
    /// naming a principal the target database may not have turns a harmless preamble
    /// into a hard failure.
    /// </para>
    /// </summary>
    public static string BuildSchemaCreate(string schema, string? owner)
    {
        var create = $"CREATE SCHEMA {Quote(schema)}";
        if(string.IsNullOrWhiteSpace(owner) || string.Equals(owner, "dbo", StringComparison.OrdinalIgnoreCase))
        {
            return
                $"IF SCHEMA_ID(N'{Literal(schema)}') IS NULL{Environment.NewLine}" +
                $"    EXEC(N'{Literal(create)}');";
        }

        // The owner is named only when the target has the principal. A restore into
        // an empty database, or into one whose users were never scripted, still gets
        // its schema; the drift report then says who owns it on each side.
        return
            $"IF SCHEMA_ID(N'{Literal(schema)}') IS NULL{Environment.NewLine}" +
            $"BEGIN{Environment.NewLine}" +
            $"    IF DATABASE_PRINCIPAL_ID(N'{Literal(owner)}') IS NOT NULL{Environment.NewLine}" +
            $"        EXEC(N'{Literal($"{create} AUTHORIZATION {Quote(owner)}")}'){Environment.NewLine}" +
            $"    ELSE{Environment.NewLine}" +
            $"        EXEC(N'{Literal(create)}'){Environment.NewLine}" +
            $"END";
    }

    /// <summary>A guarded <c>CREATE TYPE</c> for a user-defined alias type.</summary>
    public static string BuildAliasTypeCreate(AliasTypeModel type)
    {
        var baseType = BuildTypeName(type.BaseTypeName, type.MaxLength, type.Precision, type.Scale);
        var nullability = type.IsNullable ? "NULL" : "NOT NULL";
        return
            $"IF TYPE_ID(N'{Literal(type.Schema)}.{Literal(type.Name)}') IS NULL{Environment.NewLine}" +
            $"    CREATE TYPE {Quote(type.Schema, type.Name)} FROM {baseType} {nullability};";
    }

    public static string BuildType(ColumnModel column)
    {
        if(column.IsUserDefinedType)
            return $"{Quote(column.TypeSchema)}.{Quote(column.TypeName)}";

        return BuildTypeName(column.TypeName, column.MaxLength, column.Precision, column.Scale);
    }

    /// <summary>
    /// Renders a system type with its length/precision facets. <paramref name="maxLength"/>
    /// is the <c>sys.columns.max_length</c> convention: bytes, with <c>-1</c> meaning MAX.
    /// </summary>
    public static string BuildTypeName(string typeName, short maxLength, byte precision, byte scale) =>
        typeName.ToLowerInvariant() switch
        {
            "varchar" or "char" or "varbinary" or "binary" =>
                $"{typeName}({(maxLength == -1 ? "MAX" : maxLength.ToString())})",
            "nvarchar" or "nchar" =>
                $"{typeName}({(maxLength == -1 ? "MAX" : (maxLength / 2).ToString())})",
            "decimal" or "numeric" =>
                $"{typeName}({precision},{scale})",
            "datetime2" or "datetimeoffset" or "time" =>
                $"{typeName}({scale})",
            _ => typeName
        };

    public static string BuildColumnDefinition(ColumnModel column)
    {
        if(column.IsComputed)
        {
            var persisted = column.IsPersisted ? " PERSISTED" : string.Empty;
            return $"{Quote(column.Name)} AS {column.ComputedDefinition}{persisted}";
        }

        var sb = new StringBuilder();
        sb.Append(Quote(column.Name));
        sb.Append(' ');
        sb.Append(BuildType(column));

        // An alias type carries its own collation; restating it is a hard error
        // ("COLLATE clause cannot be used on user-defined data types").
        if(!string.IsNullOrWhiteSpace(column.CollationName) && !column.IsUserDefinedType)
            sb.Append($" COLLATE {column.CollationName}");

        // SPARSE sits between COLLATE and IDENTITY, and a column definition that
        // leaves it out clears it - so a rewriting ALTER COLUMN has to restate it,
        // which is what TableDiffer does.
        if(column.IsSparse)
            sb.Append(" SPARSE");

        if(column.IsIdentity)
        {
            var seed = string.IsNullOrWhiteSpace(column.IdentitySeed) ? "1" : column.IdentitySeed;
            var increment = string.IsNullOrWhiteSpace(column.IdentityIncrement) ? "1" : column.IdentityIncrement;
            sb.Append($" IDENTITY({seed},{increment})");
        }

        if(column.IsRowGuid)
            sb.Append(" ROWGUIDCOL");

        // GENERATED ALWAYS sits between IDENTITY and the nullability, and HIDDEN
        // immediately after it. Both are part of the column, not of the PERIOD: a
        // period column that loses them stops being one, and the table stops being
        // system-versionable.
        if(BuildGeneratedAlwaysClause(column) is { } generatedAlways)
        {
            sb.Append(generatedAlways);
            if(column.IsHidden)
                sb.Append(" HIDDEN");
        }

        sb.Append(column.IsNullable ? " NULL" : " NOT NULL");

        if(!string.IsNullOrWhiteSpace(column.DefaultDefinition))
        {
            sb.Append(BuildConstraintNameClause(column.DefaultName, column.DefaultIsSystemNamed));
            sb.Append($" DEFAULT {column.DefaultDefinition}");
        }

        return sb.ToString();
    }

    /// <summary>
    /// <c> GENERATED ALWAYS AS ROW START</c> / <c>... ROW END</c> for a
    /// <c>SYSTEM_TIME</c> period column, and null for everything else — including the
    /// ledger columns that share <c>sys.columns.generated_always_type</c> and would
    /// need syntax this engine does not emit.
    /// </summary>
    public static string? BuildGeneratedAlwaysClause(ColumnModel column) => column.GeneratedAlwaysType switch
    {
        1 => " GENERATED ALWAYS AS ROW START",
        2 => " GENERATED ALWAYS AS ROW END",
        _ => null
    };

    /// <summary>
    /// Renders <c>CONSTRAINT [name]</c>, or nothing when SQL Server generated the
    /// name. Auto-generated names carry a per-database random suffix
    /// (<c>PK__Orders__3214EC07CF883821</c>), so reusing one on another database
    /// bakes in a name that will never match — and reads as drift forever. Letting
    /// the target generate its own keeps both sides comparable by shape.
    /// </summary>
    public static string BuildConstraintNameClause(string? name, bool isSystemNamed) =>
        isSystemNamed || string.IsNullOrWhiteSpace(name) ? string.Empty : $" CONSTRAINT {Quote(name)}";

    public static string BuildIndexColumnExpression(IndexColumnModel column)
    {
        if(column.IsIncluded)
            return Quote(column.Name);

        return $"{Quote(column.Name)} {(column.IsDescending ? "DESC" : "ASC")}";
    }

    public static string? ToReferentialAction(string action) => action.ToUpperInvariant() switch
    {
        "NO_ACTION" => null,
        "CASCADE" => "CASCADE",
        "SET_NULL" => "SET NULL",
        "SET_DEFAULT" => "SET DEFAULT",
        _ => null
    };

    public static string BuildKeyConstraintAdd(TableModel table, KeyConstraintModel keyConstraint)
    {
        var columnsSql = string.Join(", ", keyConstraint.Columns.Select(BuildIndexColumnExpression));
        var constraintKind = keyConstraint.TypeCode == "PK" ? "PRIMARY KEY" : "UNIQUE";
        var indexKind = IsClustered(keyConstraint.IndexTypeDesc) ? "CLUSTERED" : "NONCLUSTERED";
        var nameClause = BuildConstraintNameClause(keyConstraint.Name, keyConstraint.IsSystemNamed);

        return $"ALTER TABLE {TableIdentifier(table)} ADD{nameClause} " +
               $"{constraintKind} {indexKind} ({columnsSql}){BuildIndexOptionsClause(keyConstraint)};";
    }

    /// <summary>
    /// True only for a clustered index. Note that <c>"NONCLUSTERED".Contains("CLUSTERED")</c>
    /// is also true, which is why this compares the whole descriptor.
    /// </summary>
    public static bool IsClustered(string indexTypeDesc) =>
        string.Equals(indexTypeDesc?.Trim(), "CLUSTERED", StringComparison.OrdinalIgnoreCase);

    /// <summary>True for either kind of columnstore index (<c>sys.indexes.type</c> 5 or 6).</summary>
    public static bool IsColumnstore(string? indexTypeDesc) =>
        indexTypeDesc?.Contains("COLUMNSTORE", StringComparison.OrdinalIgnoreCase) == true;

    /// <summary>True only for a clustered columnstore index, which owns the table's storage.</summary>
    public static bool IsClusteredColumnstore(string? indexTypeDesc) =>
        string.Equals(indexTypeDesc?.Trim(), "CLUSTERED COLUMNSTORE", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// True when nothing owns the table's row storage: no clustered PRIMARY KEY or
    /// UNIQUE constraint, no clustered rowstore index and no clustered columnstore
    /// index. Only a heap scripts its compression on the table itself; every other
    /// table carries it on whichever index holds the rows.
    /// </summary>
    public static bool IsHeap(TableModel table) =>
        !table.KeyConstraints.Any(x => IsClustered(x.IndexTypeDesc)) &&
        !table.Indexes.Any(x => IsClustered(x.TypeDesc) || IsClusteredColumnstore(x.TypeDesc));

    /// <summary>True for a hash index (<c>sys.indexes.type</c> 7), which only a memory-optimized table can have.</summary>
    public static bool IsHash(string? indexTypeDesc) =>
        indexTypeDesc?.Contains("HASH", StringComparison.OrdinalIgnoreCase) == true;

    /// <summary>
    /// True when the table is system-versioned, i.e. SQL Server keeps every previous
    /// version of every row in a history table of its own.
    /// </summary>
    public static bool IsSystemVersioned(TableModel table) =>
        !string.IsNullOrWhiteSpace(table.TemporalType) &&
        table.TemporalType.Contains("SYSTEM_VERSIONED", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// True when the table declares a <c>SYSTEM_TIME</c> period. A period can exist
    /// without system versioning — that is exactly the state a restore leaves the
    /// table in until the finalize phase turns versioning on — so this is a separate
    /// question from <see cref="IsSystemVersioned"/>.
    /// </summary>
    public static bool HasSystemTimePeriod(TableModel table) =>
        !string.IsNullOrWhiteSpace(table.PeriodStartColumn) &&
        !string.IsNullOrWhiteSpace(table.PeriodEndColumn);

    /// <summary>
    /// The history table's two-part name, or null when the table names none. A
    /// system-versioned table always has one: SQL Server creates it if the
    /// <c>HISTORY_TABLE</c> clause names a table that does not exist yet, which is
    /// why the extractor skips history tables rather than scripting them.
    /// </summary>
    public static string? HistoryTableIdentifier(TableModel table) =>
        string.IsNullOrWhiteSpace(table.HistoryTableName)
            ? null
            : Quote(
                string.IsNullOrWhiteSpace(table.HistoryTableSchema) ? table.Schema : table.HistoryTableSchema,
                table.HistoryTableName);

    /// <summary>
    /// <c>PERIOD FOR SYSTEM_TIME ([start], [end])</c>, or null when the table has no
    /// period. It is the last element inside <c>CREATE TABLE</c>, after every column.
    /// </summary>
    public static string? BuildPeriodClause(TableModel table) =>
        HasSystemTimePeriod(table)
            ? $"PERIOD FOR SYSTEM_TIME ({Quote(table.PeriodStartColumn!)}, {Quote(table.PeriodEndColumn!)})"
            : null;

    /// <summary>
    /// The same period as an <c>ALTER TABLE ... ADD PERIOD FOR SYSTEM_TIME</c>, for a
    /// table whose rows are already loaded, followed by the <c>ADD HIDDEN</c> of every
    /// hidden period column. Null when the table declares no period.
    /// <para>
    /// This is the statement that closes the hole a restore used to leave in a
    /// temporal table's timeline. It converts the two plain <c>datetime2</c> columns
    /// to <c>GENERATED ALWAYS</c> in place, keeping the values already in them, so the
    /// rows keep the <c>ValidFrom</c> they had on the source instead of being stamped
    /// with the instant of the restore.
    /// </para>
    /// </summary>
    /// <remarks>
    /// Measured against SQL Server 2025. <c>ADD PERIOD</c> refuses the table unless:
    /// every row's end column holds the maximum value <i>for its own scale</i> —
    /// <c>9999-12-31 23:59:59.9999999</c> at <c>datetime2(7)</c>, <c>…59.999</c> at
    /// <c>datetime2(3)</c> — or the statement fails with error 13575; no row's start
    /// column is in the future (13542); both columns are <c>datetime2</c> of any scale
    /// (13501 for <c>datetime</c>); both are <c>NOT NULL</c> (13587); and neither is
    /// already <c>HIDDEN</c> — <c>HIDDEN</c> can only be set on a column that is
    /// already <c>GENERATED ALWAYS</c> (13735), which is why it is added here, after
    /// the period, and not on the column in <c>CREATE TABLE</c>. Every one of those
    /// errors names the table and the condition, so nothing is guarded here that the
    /// server does not already diagnose better. An empty table is accepted, and so is
    /// a memory-optimized one.
    /// </remarks>
    /// <summary>
    /// The largest value a <c>datetime2</c> of the given scale can hold, as a literal.
    /// <para>
    /// A period's end column must hold exactly this on every row, and "exactly" is per
    /// scale: <c>ADD PERIOD</c> refuses a <c>datetime2(3)</c> column holding
    /// <c>…59.9999999</c> with error 13575, and refuses <c>…59.997</c> just the same.
    /// Anything that has to write an end value — a restore filling the column, or the
    /// differ giving a new column a default so it can be added to a table that already
    /// has rows — needs this and has no business computing it itself.
    /// </para>
    /// </summary>
    /// <summary>
    /// The earliest value a <c>datetime2</c> can hold, as a literal. Exact at every
    /// scale, so it needs none.
    /// <para>
    /// This is what a period's start column is given for rows that existed before the
    /// table was versioned. Their real start is not knowable — the table was not
    /// recording it — and this says so, rather than claiming they began at the moment
    /// the migration happened to run.
    /// </para>
    /// </summary>
    /// <remarks>
    /// It also has to be a value in the past, and <c>SYSUTCDATETIME()</c> is not
    /// reliably one: <c>ADD PERIOD</c> refuses a table whose open rows start in the
    /// future (13542), and a default evaluated as the statement writes the rows can
    /// land after the instant the check compares against. That race is invisible on one
    /// machine and reproducible on another — it passed on SQL Server 2025 locally and
    /// failed on a 2022 container in CI. A constant cannot lose it.
    /// </remarks>
    public static string MinDateTime2Literal => "'0001-01-01 00:00:00'";

    public static string MaxDateTime2Literal(byte scale)
    {
        if(scale > 7)
            throw new ArgumentOutOfRangeException(nameof(scale), scale, "datetime2 has a scale of 0 to 7.");

        var fraction = scale == 0 ? string.Empty : "." + new string('9', scale);
        return $"'9999-12-31 23:59:59{fraction}'";
    }

    public static string? BuildPeriodAdd(TableModel table)
    {
        if(BuildPeriodClause(table) is not { } period)
            return null;

        var sb = new StringBuilder();
        sb.Append($"ALTER TABLE {TableIdentifier(table)} ADD {period};");

        foreach(var column in PeriodColumns(table).Where(x => x.IsHidden))
            sb.Append($"{Environment.NewLine}ALTER TABLE {TableIdentifier(table)} ALTER COLUMN {Quote(column.Name)} ADD HIDDEN;");

        return sb.ToString();
    }

    /// <summary>
    /// The table's <c>SYSTEM_TIME</c> period columns, identified by the
    /// <c>GENERATED ALWAYS</c> they carry rather than by name — which is the same set
    /// <see cref="BuildGeneratedAlwaysClause"/> renders, and so the exact set that has
    /// to lose it when the period is deferred.
    /// </summary>
    private static IEnumerable<ColumnModel> PeriodColumns(TableModel table) =>
        table.Columns.Where(x => BuildGeneratedAlwaysClause(x) is not null);

    /// <summary>Turns system versioning off, which is what lets the table be dropped or its period changed.</summary>
    public static string BuildSystemVersioningOff(TableModel table) =>
        $"ALTER TABLE {TableIdentifier(table)} SET (SYSTEM_VERSIONING = OFF);";

    /// <summary>
    /// Turns system versioning on, naming the history table when the model has one.
    /// Without a name SQL Server invents one containing the table's object id, which
    /// would differ on every database and read as drift for ever, so a model that
    /// names no history table is scripted as an anonymous <c>ON</c> only as a last
    /// resort — see the caller, which warns.
    /// </summary>
    public static string BuildSystemVersioningOn(TableModel table) =>
        $"ALTER TABLE {TableIdentifier(table)} SET ({BuildSystemVersioningClause(table)});";

    /// <summary>
    /// The <c>SYSTEM_VERSIONING = ON (...)</c> option itself, shared by the inline
    /// <c>WITH</c> on CREATE TABLE and the <c>ALTER TABLE ... SET</c> a restore defers
    /// to the finalize phase, so the two can never drift apart.
    /// </summary>
    private static string BuildSystemVersioningClause(TableModel table) =>
        HistoryTableIdentifier(table) is { } history
            ? $"SYSTEM_VERSIONING = ON (HISTORY_TABLE = {history})"
            : "SYSTEM_VERSIONING = ON";

    /// <summary>
    /// True when a <c>data_compression_desc</c> is the implicit one for its index
    /// kind and so does not need scripting: nothing captured, NONE for rowstore, or
    /// COLUMNSTORE for a columnstore index, which is compressed by definition.
    /// </summary>
    public static bool IsDefaultCompression(string? dataCompression) =>
        string.IsNullOrWhiteSpace(dataCompression) ||
        dataCompression.Trim().Equals("NONE", StringComparison.OrdinalIgnoreCase) ||
        dataCompression.Trim().Equals("COLUMNSTORE", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Compares two compression descriptors, treating every spelling of "the default"
    /// as one value so a snapshot taken before compression was captured does not read
    /// as drift against a freshly extracted uncompressed index.
    /// </summary>
    public static bool CompressionEqual(string? a, string? b) =>
        (IsDefaultCompression(a) && IsDefaultCompression(b)) ||
        string.Equals(a?.Trim(), b?.Trim(), StringComparison.OrdinalIgnoreCase);

    private static string OnOff(bool value) => value ? "ON" : "OFF";

    /// <summary>
    /// Renders an index's <c>WITH (...)</c> clause, listing only the options that are
    /// not already what SQL Server would do on its own. An index created with the
    /// defaults therefore scripts with no clause at all, and reads back identical.
    /// </summary>
    /// <param name="isColumnstore">
    /// A columnstore index has no B-tree pages to fill or lock, and SQL Server
    /// rejects FILLFACTOR, PAD_INDEX, IGNORE_DUP_KEY, ALLOW_ROW_LOCKS and
    /// ALLOW_PAGE_LOCKS on one outright; only DATA_COMPRESSION survives.
    /// </param>
    /// <param name="isMemoryOptimized">
    /// An index on a memory-optimized table lives in memory: there are no pages to
    /// fill, lock or compress, and SQL Server rejects every one of those options.
    /// A hash index's <c>BUCKET_COUNT</c> is the one thing such an index does carry,
    /// and it is handled ahead of the rest because it is not optional.
    /// </param>
    public static string BuildIndexOptionsClause(
        IIndexStorageOptions options, bool isColumnstore = false, bool isMemoryOptimized = false)
    {
        if(options.BucketCount > 0)
            return $" WITH (BUCKET_COUNT = {options.BucketCount})";

        if(isMemoryOptimized)
            return string.Empty;

        var parts = new List<string>();

        if(!isColumnstore)
        {
            if(options.FillFactor > 0)
                parts.Add($"FILLFACTOR = {options.FillFactor}");
            if(options.IsPadded)
                parts.Add("PAD_INDEX = ON");
            if(options.IgnoreDupKey)
                parts.Add("IGNORE_DUP_KEY = ON");
            if(!options.AllowRowLocks)
                parts.Add("ALLOW_ROW_LOCKS = OFF");
            if(!options.AllowPageLocks)
                parts.Add("ALLOW_PAGE_LOCKS = OFF");
        }

        if(!IsDefaultCompression(options.DataCompression))
            parts.Add($"DATA_COMPRESSION = {options.DataCompression!.Trim().ToUpperInvariant()}");

        return parts.Count == 0 ? string.Empty : $" WITH ({string.Join(", ", parts)})";
    }

    /// <summary>
    /// Renders the CREATE TABLE <c>WITH (...)</c> clause. Compression only appears
    /// here for a heap; on any other table the rows belong to an index and
    /// <see cref="BuildIndexOptionsClause"/> writes the setting there.
    /// </summary>
    /// <remarks>
    /// <c>SYSTEM_VERSIONING</c> is deliberately not here. It can only be turned on for
    /// a table that already has a primary key, and this renderer always attaches the
    /// key with an <c>ALTER TABLE</c> of its own — so versioning is a separate
    /// statement too, which is also what the restore shape needs: rows have to be in
    /// before SQL Server starts writing the period columns.
    /// </remarks>
    public static string BuildTableOptionsClause(TableModel table)
    {
        var parts = new List<string>();

        // MEMORY_OPTIMIZED comes first: the rest of the clause reads as its
        // qualifiers, and a memory-optimized table has no compression to state.
        if(table.IsMemoryOptimized)
        {
            parts.Add("MEMORY_OPTIMIZED = ON");
            if(!string.IsNullOrWhiteSpace(table.Durability))
                parts.Add($"DURABILITY = {table.Durability.Trim().ToUpperInvariant()}");
        }
        else if(IsHeap(table) && !IsDefaultCompression(table.DataCompression))
        {
            parts.Add($"DATA_COMPRESSION = {table.DataCompression!.Trim().ToUpperInvariant()}");
        }

        return parts.Count == 0 ? string.Empty : $" WITH ({string.Join(", ", parts)})";
    }

    /// <summary>
    /// The statements that move an existing index from <paramref name="target"/>'s
    /// storage options to <paramref name="source"/>'s without dropping it, or an
    /// empty list when the two already agree. Dropping and re-creating an index costs
    /// a full sort of the key; a rebuild costs only the rebuild, and the lock options
    /// are pure metadata and cost nothing at all, so they go through SET. The caller
    /// is responsible for having checked that the index's shape (its columns,
    /// uniqueness, kind and filter) is unchanged.
    /// </summary>
    public static List<string> BuildIndexOptionsAlter(
        TableModel table, string indexName,
        IIndexStorageOptions source, IIndexStorageOptions target, bool isColumnstore)
    {
        var statements = new List<string>();
        var alterIndex = $"ALTER INDEX {Quote(indexName)} ON {TableIdentifier(table)}";

        var settable = new List<string>();
        var rebuildable = new List<string>();

        if(!isColumnstore)
        {
            if(source.AllowRowLocks != target.AllowRowLocks)
                settable.Add($"ALLOW_ROW_LOCKS = {OnOff(source.AllowRowLocks)}");
            if(source.AllowPageLocks != target.AllowPageLocks)
                settable.Add($"ALLOW_PAGE_LOCKS = {OnOff(source.AllowPageLocks)}");
            if(source.IgnoreDupKey != target.IgnoreDupKey)
                settable.Add($"IGNORE_DUP_KEY = {OnOff(source.IgnoreDupKey)}");

            // FILLFACTOR 0 is how sys.indexes reports "never set". On the way back in
            // it has to be written as 100, which means the same thing and is the only
            // value SQL Server accepts for a completely full page.
            if(source.FillFactor != target.FillFactor)
                rebuildable.Add($"FILLFACTOR = {(source.FillFactor == 0 ? 100 : source.FillFactor)}");
            if(source.IsPadded != target.IsPadded)
                rebuildable.Add($"PAD_INDEX = {OnOff(source.IsPadded)}");
        }

        if(!CompressionEqual(source.DataCompression, target.DataCompression))
            rebuildable.Add($"DATA_COMPRESSION = {ScriptedCompression(source.DataCompression, isColumnstore)}");

        if(settable.Count > 0)
            statements.Add($"{alterIndex} SET ({string.Join(", ", settable)});");
        if(rebuildable.Count > 0)
            statements.Add($"{alterIndex} REBUILD WITH ({string.Join(", ", rebuildable)});");

        return statements;
    }

    /// <summary>
    /// <c>ALTER TABLE ... REBUILD</c>, which is the only way to change a heap's
    /// compression: it has no index to rebuild.
    /// </summary>
    public static string BuildTableRebuild(TableModel table, string? dataCompression) =>
        $"ALTER TABLE {TableIdentifier(table)} REBUILD WITH (DATA_COMPRESSION = {ScriptedCompression(dataCompression, false)});";

    /// <summary>
    /// Spells out a compression setting for a statement that has to name one, turning
    /// "not captured" back into the explicit default for the index kind.
    /// </summary>
    private static string ScriptedCompression(string? dataCompression, bool isColumnstore) =>
        IsDefaultCompression(dataCompression)
            ? (isColumnstore ? "COLUMNSTORE" : "NONE")
            : dataCompression!.Trim().ToUpperInvariant();

    public static string BuildForeignKeyAdd(TableModel table, ForeignKeyModel foreignKey)
    {
        var fkColumnsSql = string.Join(", ", foreignKey.Columns.Select(x => Quote(x.ParentColumn)));
        var refColumnsSql = string.Join(", ", foreignKey.Columns.Select(x => Quote(x.ReferencedColumn)));
        var withCheck = foreignKey.IsNotTrusted ? "WITH NOCHECK" : "WITH CHECK";
        var nameClause = BuildConstraintNameClause(foreignKey.Name, foreignKey.IsSystemNamed);

        var sb = new StringBuilder();
        sb.Append($"ALTER TABLE {TableIdentifier(table)} {withCheck} ADD{nameClause}");
        sb.Append($" FOREIGN KEY ({fkColumnsSql})");
        sb.Append($" REFERENCES {Quote(foreignKey.ReferencedSchema, foreignKey.ReferencedTable)} ({refColumnsSql})");

        var deleteAction = ToReferentialAction(foreignKey.DeleteActionDesc);
        var updateAction = ToReferentialAction(foreignKey.UpdateActionDesc);
        if(deleteAction is not null)
            sb.Append($" ON DELETE {deleteAction}");
        if(updateAction is not null)
            sb.Append($" ON UPDATE {updateAction}");
        if(foreignKey.IsNotForReplication)
            sb.Append(" NOT FOR REPLICATION");

        sb.Append(';');
        return sb.ToString();
    }

    public static string BuildCheckConstraintAdd(TableModel table, CheckConstraintModel check)
    {
        var withCheck = check.IsNotTrusted ? "WITH NOCHECK" : "WITH CHECK";
        var nameClause = BuildConstraintNameClause(check.Name, check.IsSystemNamed);
        return $"ALTER TABLE {TableIdentifier(table)} {withCheck} ADD{nameClause} CHECK {check.Definition};";
    }

    public static string BuildIndexCreate(TableModel table, IndexModel index)
    {
        if(table.IsMemoryOptimized)
            return BuildInlineIndexAdd(table, index);

        if(IsColumnstore(index.TypeDesc))
            return BuildColumnstoreIndexCreate(table, index);

        var keyColumns = index.Columns.Where(x => !x.IsIncluded).Select(BuildIndexColumnExpression).ToList();
        var includedColumns = index.Columns.Where(x => x.IsIncluded).Select(x => Quote(x.Name)).ToList();

        var sb = new StringBuilder();
        sb.Append($"CREATE {(index.IsUnique ? "UNIQUE " : string.Empty)}{index.TypeDesc.Replace('_', ' ')} INDEX {Quote(index.Name)}");
        sb.Append($" ON {TableIdentifier(table)} ({string.Join(", ", keyColumns)})");
        if(includedColumns.Count > 0)
            sb.Append($" INCLUDE ({string.Join(", ", includedColumns)})");
        if(!string.IsNullOrWhiteSpace(index.FilterDefinition))
            sb.Append($" WHERE {index.FilterDefinition}");
        sb.Append(BuildIndexOptionsClause(index));
        sb.Append(';');
        return sb.ToString();
    }

    /// <summary>
    /// A columnstore index keeps no sort order, so its columns are listed bare
    /// (SQL Server rejects ASC and DESC on one) and there is no UNIQUE or INCLUDE.
    /// A clustered columnstore index covers every column in the table implicitly and
    /// so takes no column list at all.
    /// </summary>
    private static string BuildColumnstoreIndexCreate(TableModel table, IndexModel index)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE {index.TypeDesc.Replace('_', ' ')} INDEX {Quote(index.Name)} ON {TableIdentifier(table)}");

        if(!IsClusteredColumnstore(index.TypeDesc))
        {
            // sys.index_columns marks every columnstore column as "included" because
            // none of them is a key. They are all part of the column list all the same.
            var columns = index.Columns.Select(x => Quote(x.Name));
            sb.Append($" ({string.Join(", ", columns)})");
            if(!string.IsNullOrWhiteSpace(index.FilterDefinition))
                sb.Append($" WHERE {index.FilterDefinition}");
        }

        sb.Append(BuildIndexOptionsClause(index, isColumnstore: true));
        sb.Append(';');
        return sb.ToString();
    }

    public static string BuildIndexDrop(TableModel table, IndexModel index) =>
        table.IsMemoryOptimized
            ? BuildInlineIndexDrop(table, index)
            : $"DROP INDEX {Quote(index.Name)} ON {TableIdentifier(table)};";

    /// <summary>
    /// Drops a constraint by name. Drops always come from the target model, so the
    /// captured name is the one that actually exists there — including the random
    /// name SQL Server generated for an unnamed constraint.
    /// </summary>
    public static string BuildConstraintDrop(TableModel table, string name) =>
        $"ALTER TABLE {TableIdentifier(table)} DROP CONSTRAINT {Quote(name)};";

    /// <summary>Stops validating a constraint without dropping it.</summary>
    public static string BuildConstraintNoCheck(TableModel table, string name) =>
        $"ALTER TABLE {TableIdentifier(table)} NOCHECK CONSTRAINT {Quote(name)};";

    /// <summary>
    /// The <c>NOCHECK</c> that belongs after the <c>ADD</c> for a check constraint the
    /// source had switched off.
    /// <para>
    /// A constraint the caller named is switched off by that name. One SQL Server named
    /// for itself cannot be: the <c>CK__Widget__Price__1A2B3C4D</c> in the snapshot
    /// belongs to the server it was read from, and the <c>ADD</c> above has just been
    /// handed a fresh one of its own. Naming the old one fails, or - worse - finds
    /// something else that happens to answer to it. Until 1.7 the renderers gave up at
    /// that point and emitted nothing, which left the constraint enforcing on a target
    /// whose source had it off, and said nothing about it.
    /// </para>
    /// <para>
    /// So the name is resolved on the server, at the moment the script runs, from the
    /// two things that do carry across: the table the constraint stands on and the
    /// predicate it enforces. Exactly one match is disabled; none or several is left
    /// alone and printed, because disabling the wrong constraint is worse than
    /// disabling nothing.
    /// </para>
    /// </summary>
    public static string BuildCheckConstraintNoCheck(TableModel table, CheckConstraintModel check) =>
        IsServerNamed(check.Name, check.IsSystemNamed)
            ? BuildResolvedNoCheck(
                table,
                "CHECK constraint",
                $"the predicate {Collapse(check.Definition)}",
                BuildCheckConstraintLookup(table, check))
            : BuildConstraintNoCheck(table, check.Name);

    /// <summary>
    /// The <c>NOCHECK</c> that belongs after the <c>ADD</c> for a foreign key the source
    /// had switched off. Same problem and same answer as
    /// <see cref="BuildCheckConstraintNoCheck"/>; a foreign key is matched on the table
    /// it sits on, the table it points at, and its column pairs in order.
    /// </summary>
    public static string BuildForeignKeyNoCheck(TableModel table, ForeignKeyModel foreignKey) =>
        IsServerNamed(foreignKey.Name, foreignKey.IsSystemNamed)
            ? BuildResolvedNoCheck(
                table,
                "FOREIGN KEY",
                $"the reference to {Quote(foreignKey.ReferencedSchema, foreignKey.ReferencedTable)} " +
                $"on ({string.Join(", ", foreignKey.Columns.Select(x => Quote(x.ParentColumn)))})",
                BuildForeignKeyLookup(table, foreignKey))
            : BuildConstraintNoCheck(table, foreignKey.Name);

    /// <summary>
    /// True when the script cannot put a name in a <c>NOCHECK CONSTRAINT</c> clause and
    /// expect it to mean anything on the target. A missing name counts: a model that
    /// carries none is in the same position as one whose name the server invented.
    /// </summary>
    private static bool IsServerNamed(string? name, bool isSystemNamed) =>
        isSystemNamed || string.IsNullOrWhiteSpace(name);

    /// <summary>
    /// The batch that resolves a server-generated name and switches that one constraint
    /// off. <paramref name="matchedOn"/> is what the lookup keys on, said in English,
    /// because it is the first thing a reviewer wants to know and the last thing the run
    /// says when the lookup comes to nothing.
    /// </summary>
    private static string BuildResolvedNoCheck(TableModel table, string kind, string matchedOn, string lookup)
    {
        var tableId = TableIdentifier(table);

        var sb = new StringBuilder();
        sb.AppendLine($"-- The {kind} added above was disabled on the source and SQL Server had named");
        sb.AppendLine("-- it, so the name it carried there is not the name it has here. Find the one");
        sb.AppendLine($"-- this script just created on {tableId} by {matchedOn},");
        sb.AppendLine("-- and switch that one off. Nothing is disabled unless the lookup comes to");
        sb.AppendLine("-- exactly one constraint.");
        sb.AppendLine("DECLARE @name sysname, @matches int, @sql nvarchar(max);");
        sb.AppendLine();
        sb.AppendLine(lookup);
        sb.AppendLine();
        sb.AppendLine("IF @matches = 1");
        sb.AppendLine("BEGIN");

        // EXEC() takes variables and string literals and nothing else, so QUOTENAME
        // cannot go inside it: the resolved name is quoted into a variable first.
        sb.AppendLine($"    SET @sql = N'ALTER TABLE {Literal(tableId)} NOCHECK CONSTRAINT ' + QUOTENAME(@name);");
        sb.AppendLine("    EXEC sys.sp_executesql @sql;");
        sb.AppendLine("END");
        sb.AppendLine("ELSE");
        sb.AppendLine($"    PRINT N'-- WARNING: the {Literal(kind)} on {Literal(tableId)} matched on '");
        sb.AppendLine($"        + N'{Literal(matchedOn)} came to ' + CAST(@matches AS nvarchar(12))");
        sb.Append("        + N' constraint(s), not one, so it was left enabled.';");
        return sb.ToString();
    }

    /// <summary>
    /// Finds a server-named check constraint on the table by its predicate. The
    /// comparison ignores whitespace on both sides: SQL Server rewrites an expression
    /// from its own parse tree, and a snapshot that was hand-written or produced by an
    /// older extractor need not have spaced it the same way.
    /// </summary>
    private static string BuildCheckConstraintLookup(TableModel table, CheckConstraintModel check)
    {
        var sb = new StringBuilder();
        sb.AppendLine("SELECT @matches = COUNT(*), @name = MIN(cc.name)");
        sb.AppendLine("FROM sys.check_constraints AS cc");
        sb.AppendLine($"WHERE cc.parent_object_id = OBJECT_ID(N'{Literal(TableIdentifier(table))}')");
        sb.AppendLine("  AND cc.is_system_named = 1");
        sb.AppendLine($"  AND {Unspaced("cc.definition")} =");
        sb.Append($"      {Unspaced($"N'{Literal(check.Definition)}'")};");
        return sb.ToString();
    }

    /// <summary>
    /// Finds a server-named foreign key on the table by what it points at and the column
    /// pairs it points with. The count and the <c>EXCEPT</c> together are an equality:
    /// the key has as many columns as the source's, and not one pair the source does not
    /// have.
    /// </summary>
    private static string BuildForeignKeyLookup(TableModel table, ForeignKeyModel foreignKey)
    {
        var expected = foreignKey.Columns
            .Select((x, i) => $"({i + 1}, N'{Literal(x.ParentColumn)}', N'{Literal(x.ReferencedColumn)}')");

        var sb = new StringBuilder();
        sb.AppendLine("SELECT @matches = COUNT(*), @name = MIN(fk.name)");
        sb.AppendLine("FROM sys.foreign_keys AS fk");
        sb.AppendLine($"WHERE fk.parent_object_id = OBJECT_ID(N'{Literal(TableIdentifier(table))}')");
        sb.AppendLine("  AND fk.referenced_object_id = " +
                      $"OBJECT_ID(N'{Literal(Quote(foreignKey.ReferencedSchema, foreignKey.ReferencedTable))}')");
        sb.AppendLine("  AND fk.is_system_named = 1");
        sb.AppendLine("  AND (SELECT COUNT(*) FROM sys.foreign_key_columns AS c");
        sb.AppendLine($"       WHERE c.constraint_object_id = fk.object_id) = {foreignKey.Columns.Count}");
        sb.AppendLine("  AND NOT EXISTS");
        sb.AppendLine("      (");
        sb.AppendLine("          SELECT fkc.constraint_column_id, pc.name AS parent_column, rc.name AS referenced_column");
        sb.AppendLine("          FROM sys.foreign_key_columns AS fkc");
        sb.AppendLine("          INNER JOIN sys.columns AS pc ON pc.object_id = fkc.parent_object_id");
        sb.AppendLine("                                      AND pc.column_id = fkc.parent_column_id");
        sb.AppendLine("          INNER JOIN sys.columns AS rc ON rc.object_id = fkc.referenced_object_id");
        sb.AppendLine("                                      AND rc.column_id = fkc.referenced_column_id");
        sb.AppendLine("          WHERE fkc.constraint_object_id = fk.object_id");
        sb.AppendLine("          EXCEPT");
        sb.AppendLine("          SELECT *");
        sb.AppendLine($"          FROM (VALUES {string.Join(", ", expected)})");
        sb.AppendLine("               AS expected (constraint_column_id, parent_column, referenced_column)");
        sb.Append("      );");
        return sb.ToString();
    }

    /// <summary>
    /// A T-SQL expression with every space, tab and line break taken out of
    /// <paramref name="expression"/>, so two spellings of the same predicate compare
    /// equal.
    /// </summary>
    private static string Unspaced(string expression) =>
        $"REPLACE(REPLACE(REPLACE(REPLACE({expression}, NCHAR(13), N''), NCHAR(10), N''), NCHAR(9), N''), N' ', N'')";

    /// <summary>One line of whatever came in, for a comment or a PRINT.</summary>
    private static string Collapse(string text) =>
        string.Join(" ", text.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

    public static string BuildIndexDisable(TableModel table, IndexModel index) =>
        $"ALTER INDEX {Quote(index.Name)} ON {TableIdentifier(table)} DISABLE;";

    /// <summary>
    /// Brings a disabled index back online. SQL Server has no
    /// <c>ALTER INDEX ... ENABLE</c>: the index's rows were thrown away when it was
    /// disabled, so a rebuild is the only way back.
    /// </summary>
    public static string BuildIndexRebuild(TableModel table, IndexModel index) =>
        $"ALTER INDEX {Quote(index.Name)} ON {TableIdentifier(table)} REBUILD;";

    /// <summary>
    /// Re-enables a constraint and validates the rows already in the table, which is
    /// what clears <c>is_not_trusted</c> and lets the optimizer rely on it again.
    /// </summary>
    public static string BuildConstraintCheck(TableModel table, string name) =>
        $"ALTER TABLE {TableIdentifier(table)} WITH CHECK CHECK CONSTRAINT {Quote(name)};";

    /// <summary>
    /// Re-enables a constraint without looking at the rows already in the table, so
    /// it starts enforcing new rows while staying untrusted.
    /// </summary>
    public static string BuildConstraintCheckNoValidate(TableModel table, string name) =>
        $"ALTER TABLE {TableIdentifier(table)} WITH NOCHECK CHECK CONSTRAINT {Quote(name)};";

    /// <summary>
    /// A guarded <c>DROP CONSTRAINT</c> for a foreign key. The rebuild takes down
    /// every key pointing at the table it is about to drop, and the list it works
    /// from is a snapshot: guarding the drop is what keeps the script re-runnable
    /// when one of them has already gone.
    /// </summary>
    public static string BuildForeignKeyDropIfExists(TableModel table, string name) =>
        $"IF OBJECT_ID(N'{Literal($"{Quote(table.Schema)}.{Quote(name)}")}', 'F') IS NOT NULL{Environment.NewLine}" +
        $"    ALTER TABLE {TableIdentifier(table)} DROP CONSTRAINT {Quote(name)};";

    /// <summary>
    /// <c>sp_rename</c> for an object. The current name goes in as a quoted two-part
    /// name and the new one bare, because renaming cannot move an object between
    /// schemas and sp_rename rejects a new name that names one.
    /// </summary>
    /// <param name="objectType">
    /// sp_rename's <c>@objtype</c>. Left null for a table, where the default is what
    /// is wanted; passed as <c>OBJECT</c> for a constraint, where saying so makes the
    /// statement readable on its own.
    /// </param>
    public static string BuildObjectRename(string schema, string currentName, string newName, string? objectType = null)
    {
        var current = Literal($"{Quote(schema)}.{Quote(currentName)}");
        var suffix = string.IsNullOrWhiteSpace(objectType) ? string.Empty : $", N'{Literal(objectType)}'";
        return $"EXEC sp_rename N'{current}', N'{Literal(newName)}'{suffix};";
    }

    /// <summary>
    /// Renders only the <c>CREATE TABLE</c> statement: columns, computed columns,
    /// identity and inline defaults, plus the <c>PERIOD FOR SYSTEM_TIME</c> a temporal
    /// table declares. Everything that can be attached later (keys, checks, indexes,
    /// foreign keys) is left to the callers that place those in their own phase.
    /// <para>
    /// A memory-optimized table is the exception: <c>CREATE INDEX</c> and
    /// <c>ALTER TABLE ... ADD CONSTRAINT PRIMARY KEY</c> are both rejected on one, so
    /// its keys and indexes are written inline here and the callers must not emit them
    /// a second time.
    /// </para>
    /// </summary>
    /// <remarks>
    /// <b>This exact signature is part of the binary contract and must not change.</b>
    /// 1.8.0 replaced it with an overload taking an optional <c>deferPeriod</c>
    /// parameter. That is invisible to anyone who recompiles, and fatal to anyone who
    /// does not: an optional parameter is a different method in IL, so an assembly
    /// already built against 1.7 - <c>PeopleWorks.SyncJob.Core</c> 1.0.0, on nuget.org -
    /// failed at run time with <c>MissingMethodException</c> the first time it created
    /// a staging table. Restored in 1.8.1 as a method of its own, and package validation
    /// now fails the build on any change like it.
    /// </remarks>
    public static string BuildTableCreateOnly(TableModel table) =>
        BuildTableCreateOnly(table, deferPeriod: false);

    /// <summary>
    /// The same <c>CREATE TABLE</c>, with the choice of leaving a <c>SYSTEM_TIME</c>
    /// period out of it.
    /// </summary>
    /// <param name="table">The table to render.</param>
    /// <param name="deferPeriod">
    /// When true, a <c>SYSTEM_TIME</c> period is left out of the CREATE entirely: the
    /// two period columns come out as plain <c>datetime2 NOT NULL</c>, with no
    /// <c>GENERATED ALWAYS</c> and no <c>HIDDEN</c>, and the caller is responsible for
    /// emitting <see cref="BuildPeriodAdd"/> once the rows are loaded. Deliberately not
    /// optional - see the one-argument overload for what an optional parameter here cost.
    /// </param>
    public static string BuildTableCreateOnly(TableModel table, bool deferPeriod)
    {
        // A period cannot be deferred if there is none; asking for it on an ordinary
        // table is a no-op rather than an error, so a caller can pass the option
        // through for every table without testing each one.
        deferPeriod &= HasSystemTimePeriod(table);

        var elements = new List<string>(table.Columns.Select(x => deferPeriod ? BuildDeferredPeriodColumn(x) : BuildColumnDefinition(x)));

        if(table.IsMemoryOptimized)
        {
            elements.AddRange(table.KeyConstraints.Select(BuildInlineKeyConstraint));
            elements.AddRange(table.Indexes.Select(BuildInlineIndex));
        }

        // The period is the last element inside the parentheses, after every column
        // and after anything the table carries inline.
        if(!deferPeriod && BuildPeriodClause(table) is { } period)
            elements.Add(period);

        var sb = new StringBuilder();
        if(table.IsMemoryOptimized)
            sb.AppendLine(MemoryOptimizedFilegroupComment);

        sb.AppendLine($"CREATE TABLE {TableIdentifier(table)}");
        sb.AppendLine("(");
        for(var i = 0; i < elements.Count; i++)
        {
            sb.Append("    ");
            sb.Append(elements[i]);
            if(i != elements.Count - 1)
                sb.Append(',');
            sb.AppendLine();
        }
        sb.Append($"){BuildTableOptionsClause(table)};");
        return sb.ToString();
    }

    /// <summary>
    /// A column as it has to look while the rows are being loaded. A period column
    /// loses its <c>GENERATED ALWAYS</c> and its <c>HIDDEN</c> and becomes a plain
    /// <c>datetime2</c> of the same scale; every other column is unchanged.
    /// </summary>
    /// <remarks>
    /// The column is rendered from a copy so the snapshot keeps the shape it was
    /// extracted with — the same trick <see cref="TableRebuilder"/> uses. Clearing
    /// <c>GeneratedAlwaysType</c> takes <c>HIDDEN</c> with it, because HIDDEN is only
    /// ever rendered beside GENERATED ALWAYS. Nullability is forced off on top:
    /// <c>ADD PERIOD</c> refuses a nullable period column (error 13587), a real
    /// system-versioned table never has one, and NOT NULL here is what the source's
    /// own column will be again a phase later.
    /// </remarks>
    private static string BuildDeferredPeriodColumn(ColumnModel column)
    {
        if(BuildGeneratedAlwaysClause(column) is null)
            return BuildColumnDefinition(column);

        var plain = column.Clone();
        plain.GeneratedAlwaysType = 0;
        plain.IsNullable = false;
        return BuildColumnDefinition(plain);
    }

    /// <summary>
    /// The one thing about a memory-optimized table the engine cannot script. Adding
    /// the filegroup needs a path on the server's own disk, which no snapshot knows
    /// and no tool should guess, so the script says what the database needs and the
    /// operator provides it.
    /// </summary>
    public static string MemoryOptimizedFilegroupComment =>
        "-- Requires a filegroup CONTAINS MEMORY_OPTIMIZED_DATA on the target database;" + Environment.NewLine +
        "-- it names a path on the server's own disk, so it has to be added by hand.";

    /// <summary>
    /// A <c>PRIMARY KEY</c> or <c>UNIQUE</c> written inside <c>CREATE TABLE</c>, in
    /// the form a memory-optimized table needs: never clustered, and carrying
    /// <c>HASH ... WITH (BUCKET_COUNT = n)</c> when the index behind it is a hash.
    /// </summary>
    public static string BuildInlineKeyConstraint(KeyConstraintModel keyConstraint)
    {
        var columnsSql = string.Join(", ", keyConstraint.Columns.Select(BuildIndexColumnExpression));
        var constraintKind = keyConstraint.TypeCode == "PK" ? "PRIMARY KEY" : "UNIQUE";
        var indexKind = IsHash(keyConstraint.IndexTypeDesc) ? "NONCLUSTERED HASH" : "NONCLUSTERED";
        var name = BuildInlineConstraintName(keyConstraint.Name, keyConstraint.IsSystemNamed);

        // A hash index has no order, so its columns are listed bare.
        if(IsHash(keyConstraint.IndexTypeDesc))
            columnsSql = string.Join(", ", keyConstraint.Columns.Select(x => Quote(x.Name)));

        return $"{name}{constraintKind} {indexKind} ({columnsSql})" +
               BuildIndexOptionsClause(keyConstraint, isColumnstore: false, isMemoryOptimized: true);
    }

    /// <summary>
    /// An <c>INDEX [name] ...</c> element inside <c>CREATE TABLE</c>, the only place a
    /// memory-optimized table's secondary indexes can be declared.
    /// </summary>
    public static string BuildInlineIndex(IndexModel index)
    {
        var isHash = IsHash(index.TypeDesc);
        var columnsSql = isHash
            ? string.Join(", ", index.Columns.Select(x => Quote(x.Name)))
            : string.Join(", ", index.Columns.Where(x => !x.IsIncluded).Select(BuildIndexColumnExpression));

        return $"INDEX {Quote(index.Name)} {(isHash ? "HASH" : "NONCLUSTERED")} ({columnsSql})" +
               BuildIndexOptionsClause(index, isColumnstore: false, isMemoryOptimized: true);
    }

    /// <summary>
    /// Adds an index to a memory-optimized table. <c>CREATE INDEX</c> is rejected on
    /// one outright ("The operation 'CREATE INDEX' is not supported with memory
    /// optimized tables"); <c>ALTER TABLE ... ADD INDEX</c> is the form that works.
    /// </summary>
    public static string BuildInlineIndexAdd(TableModel table, IndexModel index) =>
        $"ALTER TABLE {TableIdentifier(table)} ADD {BuildInlineIndex(index)};";

    /// <summary>The matching drop; <c>DROP INDEX ... ON ...</c> is rejected the same way.</summary>
    public static string BuildInlineIndexDrop(TableModel table, IndexModel index) =>
        $"ALTER TABLE {TableIdentifier(table)} DROP INDEX {Quote(index.Name)};";

    /// <summary>Renders the complete CREATE TABLE script (table + keys + FKs + checks + indexes).</summary>
    public static string BuildTableCreateScript(TableModel table)
    {
        // A memory-optimized table's keys and indexes are already inside its CREATE,
        // and neither can be added afterwards, so it takes a script of its own rather
        // than the key and index loops below.
        if(table.IsMemoryOptimized)
            return BuildMemoryOptimizedTableCreateScript(table);

        var sb = new StringBuilder();
        sb.AppendLine(BuildTableCreateOnly(table));
        sb.AppendLine("GO");
        sb.AppendLine();

        foreach(var keyConstraint in table.KeyConstraints)
        {
            sb.AppendLine(BuildKeyConstraintAdd(table, keyConstraint));
            sb.AppendLine("GO");
            sb.AppendLine();
        }

        foreach(var foreignKey in table.ForeignKeys)
        {
            sb.AppendLine(BuildForeignKeyAdd(table, foreignKey));
            sb.AppendLine("GO");

            if(foreignKey.IsDisabled)
            {
                sb.AppendLine(BuildForeignKeyNoCheck(table, foreignKey));
                sb.AppendLine("GO");
            }

            sb.AppendLine();
        }

        foreach(var check in table.CheckConstraints)
        {
            sb.AppendLine(BuildCheckConstraintAdd(table, check));
            sb.AppendLine("GO");

            if(check.IsDisabled)
            {
                sb.AppendLine(BuildCheckConstraintNoCheck(table, check));
                sb.AppendLine("GO");
            }

            sb.AppendLine();
        }

        foreach(var index in table.Indexes)
        {
            sb.AppendLine(BuildIndexCreate(table, index));
            sb.AppendLine("GO");

            if(index.IsDisabled)
            {
                sb.AppendLine(BuildIndexDisable(table, index));
                sb.AppendLine("GO");
            }

            sb.AppendLine();
        }

        AppendSystemVersioning(sb, table);
        return sb.ToString().TrimEnd();
    }

    /// <summary>
    /// Turns system versioning on, last of all. It has to be last: SQL Server refuses
    /// to version a table with no primary key, and the key arrives as an ALTER of its
    /// own a few batches above.
    /// </summary>
    private static void AppendSystemVersioning(StringBuilder sb, TableModel table)
    {
        if(!IsSystemVersioned(table))
            return;

        sb.AppendLine(BuildSystemVersioningOn(table));
        sb.AppendLine("GO");
        sb.AppendLine();
    }

    /// <summary>
    /// The same script for a memory-optimized table. Its keys and indexes came out
    /// inline in the CREATE, so only the constraints that <c>ALTER TABLE</c> can still
    /// add — checks and foreign keys — follow it.
    /// </summary>
    private static string BuildMemoryOptimizedTableCreateScript(TableModel table)
    {
        var sb = new StringBuilder();
        sb.AppendLine(BuildTableCreateOnly(table));
        sb.AppendLine("GO");
        sb.AppendLine();

        foreach(var check in table.CheckConstraints)
        {
            sb.AppendLine(BuildCheckConstraintAdd(table, check));
            sb.AppendLine("GO");

            if(check.IsDisabled && !check.IsSystemNamed)
            {
                sb.AppendLine(BuildConstraintNoCheck(table, check.Name));
                sb.AppendLine("GO");
            }

            sb.AppendLine();
        }

        foreach(var foreignKey in table.ForeignKeys)
        {
            sb.AppendLine(BuildForeignKeyAdd(table, foreignKey));
            sb.AppendLine("GO");
            sb.AppendLine();
        }

        AppendSystemVersioning(sb, table);
        return sb.ToString().TrimEnd();
    }

    // ------------------------------------------------------------- table types

    /// <summary>
    /// Renders the complete <c>CREATE TYPE ... AS TABLE</c>.
    /// <para>
    /// Every constraint is inline. A table type has no ALTER: <c>ALTER TABLE</c>
    /// cannot name one, so a key or check that is not in the CREATE can never be
    /// added afterwards.
    /// </para>
    /// </summary>
    public static string BuildTableTypeCreateScript(TableTypeModel tableType)
    {
        var lines = new List<string>();
        lines.AddRange(tableType.Columns.Select(BuildColumnDefinition));
        lines.AddRange(tableType.KeyConstraints.Select(BuildTableTypeKeyConstraint));
        lines.AddRange(tableType.CheckConstraints.Select(BuildTableTypeCheckConstraint));

        var sb = new StringBuilder();
        sb.AppendLine($"CREATE TYPE {Quote(tableType.Schema, tableType.Name)} AS TABLE");
        sb.AppendLine("(");
        for(var i = 0; i < lines.Count; i++)
        {
            sb.Append("    ");
            sb.Append(lines[i]);
            if(i != lines.Count - 1)
                sb.Append(',');
            sb.AppendLine();
        }

        sb.Append(')');
        if(tableType.IsMemoryOptimized)
            sb.Append(" WITH (MEMORY_OPTIMIZED = ON)");
        sb.Append(';');
        return sb.ToString();
    }

    public static string BuildTableTypeKeyConstraint(KeyConstraintModel keyConstraint)
    {
        var columnsSql = string.Join(", ", keyConstraint.Columns.Select(BuildIndexColumnExpression));
        var constraintKind = keyConstraint.TypeCode == "PK" ? "PRIMARY KEY" : "UNIQUE";
        var indexKind = IsClustered(keyConstraint.IndexTypeDesc) ? "CLUSTERED" : "NONCLUSTERED";
        return $"{BuildInlineConstraintName(keyConstraint.Name, keyConstraint.IsSystemNamed)}{constraintKind} {indexKind} ({columnsSql})";
    }

    public static string BuildTableTypeCheckConstraint(CheckConstraintModel check) =>
        $"{BuildInlineConstraintName(check.Name, check.IsSystemNamed)}CHECK {check.Definition}";

    /// <summary>
    /// <c>CONSTRAINT [name] </c> with a trailing space, or nothing. The table-type
    /// variant of <see cref="BuildConstraintNameClause"/>: inline constraints lead
    /// the line rather than following an <c>ADD</c>.
    /// </summary>
    private static string BuildInlineConstraintName(string? name, bool isSystemNamed) =>
        isSystemNamed || string.IsNullOrWhiteSpace(name) ? string.Empty : $"CONSTRAINT {Quote(name)} ";

    // --------------------------------------------------------------- sequences

    /// <summary>The declared type of a sequence, e.g. <c>bigint</c> or <c>decimal(18,0)</c>.</summary>
    public static string BuildSequenceTypeName(SequenceModel sequence) =>
        BuildTypeName(sequence.TypeName, 0, sequence.Precision, sequence.Scale);

    public static string BuildSequenceCreate(SequenceModel sequence)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE SEQUENCE {Quote(sequence.Schema, sequence.Name)} AS {BuildSequenceTypeName(sequence)}");
        sb.Append($" START WITH {Numeric(sequence.StartValue) ?? "1"}");
        sb.Append($" INCREMENT BY {Numeric(sequence.Increment) ?? "1"}");
        sb.Append(BuildSequenceBoundsClauses(sequence));
        sb.Append(BuildSequenceCycleClause(sequence));
        sb.Append(BuildSequenceCacheClause(sequence));
        sb.Append(';');
        return sb.ToString();
    }

    /// <summary>
    /// Resumes a restored sequence where the captured one left off:
    /// <c>RESTART WITH current_value + increment</c>.
    /// <para>
    /// <c>sys.sequences.current_value</c> is the last value actually handed out, so
    /// restarting <i>at</i> it would hand the same number out twice — a duplicate key
    /// waiting to happen. Restarting one increment past it can at worst skip a single
    /// value on a sequence that was never used (where <c>current_value</c> still
    /// equals <c>start_value</c>), which costs nothing.
    /// </para>
    /// <para>
    /// Returns null when the snapshot carries no current value; there is nothing to
    /// resume from and <c>START WITH</c> already covers it.
    /// </para>
    /// </summary>
    public static string? BuildSequenceRestart(SequenceModel sequence)
    {
        var current = ParseNumeric(sequence.CurrentValue);
        if(current is null)
            return null;

        var next = current.Value + (ParseNumeric(sequence.Increment) ?? BigInteger.One);

        // A RESTART outside the sequence's own bounds is rejected by the server.
        var minimum = ParseNumeric(sequence.MinValue);
        var maximum = ParseNumeric(sequence.MaxValue);
        if(minimum is not null && next < minimum.Value)
            next = minimum.Value;
        if(maximum is not null && next > maximum.Value)
            next = maximum.Value;

        return $"ALTER SEQUENCE {Quote(sequence.Schema, sequence.Name)} RESTART WITH {next.ToString(CultureInfo.InvariantCulture)};";
    }

    /// <summary>
    /// An <c>ALTER SEQUENCE</c> carrying only the clauses that differ, or null when
    /// nothing alterable changed. The type and the start value are not alterable and
    /// are not considered here.
    /// </summary>
    public static string? BuildSequenceAlter(SequenceModel source, SequenceModel target)
    {
        var clauses = new StringBuilder();

        if(!NumericEquals(source.Increment, target.Increment))
            clauses.Append($" INCREMENT BY {Numeric(source.Increment) ?? "1"}");

        if(!NumericEquals(source.MinValue, target.MinValue))
            clauses.Append(Numeric(source.MinValue) is { } minimum ? $" MINVALUE {minimum}" : " NO MINVALUE");

        if(!NumericEquals(source.MaxValue, target.MaxValue))
            clauses.Append(Numeric(source.MaxValue) is { } maximum ? $" MAXVALUE {maximum}" : " NO MAXVALUE");

        if(source.IsCycling != target.IsCycling)
            clauses.Append(BuildSequenceCycleClause(source));

        if(source.IsCached != target.IsCached || source.CacheSize != target.CacheSize)
            clauses.Append(BuildSequenceCacheClause(source));

        return clauses.Length == 0
            ? null
            : $"ALTER SEQUENCE {Quote(source.Schema, source.Name)}{clauses};";
    }

    private static string BuildSequenceBoundsClauses(SequenceModel sequence)
    {
        var minimum = Numeric(sequence.MinValue) is { } min ? $" MINVALUE {min}" : " NO MINVALUE";
        var maximum = Numeric(sequence.MaxValue) is { } max ? $" MAXVALUE {max}" : " NO MAXVALUE";
        return minimum + maximum;
    }

    private static string BuildSequenceCycleClause(SequenceModel sequence) =>
        sequence.IsCycling ? " CYCLE" : " NO CYCLE";

    private static string BuildSequenceCacheClause(SequenceModel sequence)
    {
        if(!sequence.IsCached)
            return " NO CACHE";

        // is_cached with a null cache_size means "cache, size chosen by the server".
        return sequence.CacheSize is > 0 ? $" CACHE {sequence.CacheSize.Value.ToString(CultureInfo.InvariantCulture)}" : " CACHE";
    }

    // ---------------------------------------------------------------- triggers

    public static string BuildTriggerDisable(string schema, string name, TriggerModel trigger) =>
        $"DISABLE TRIGGER {Quote(schema, name)} ON {Quote(trigger.ParentSchema, trigger.ParentName)};";

    public static string BuildTriggerEnable(string schema, string name, TriggerModel trigger) =>
        $"ENABLE TRIGGER {Quote(schema, name)} ON {Quote(trigger.ParentSchema, trigger.ParentName)};";

    // ---------------------------------------------------- numeric-text helpers

    private static readonly Regex IntegerLiteral = new(@"^[+-]?[0-9]+$", RegexOptions.CultureInvariant);

    /// <summary>
    /// The sequence bounds are carried as text and go straight into generated DDL,
    /// so anything that is not an integer literal is treated as absent rather than
    /// pasted into a statement.
    /// </summary>
    private static string? Numeric(string? value)
    {
        if(string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        return IntegerLiteral.IsMatch(trimmed) ? trimmed : null;
    }

    private static BigInteger? ParseNumeric(string? value) =>
        Numeric(value) is { } text && BigInteger.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;

    /// <summary>Compares two numeric texts by value, so <c>+7</c> and <c>7</c> match.</summary>
    internal static bool NumericEquals(string? left, string? right)
    {
        var leftValue = ParseNumeric(left);
        var rightValue = ParseNumeric(right);
        if(leftValue is not null && rightValue is not null)
            return leftValue.Value == rightValue.Value;

        return leftValue is null && rightValue is null;
    }

    // ---------------------------------------------------------------- synonyms

    /// <summary>
    /// <c>CREATE SYNONYM</c>. The synonym's own schema and name are quoted like any
    /// other identifier; the base object name is emitted <b>exactly</b> as
    /// <c>sys.synonyms</c> reported it.
    /// <para>
    /// The catalog already stores that name bracket-quoted and one to four parts
    /// long, so quoting it again would produce <c>[[db]].[[dbo]].[[T]]</c>, and
    /// splitting it would have to guess whether the leading part is a database or a
    /// linked server. Nothing is lost by leaving it alone: <c>CREATE SYNONYM</c>
    /// never resolves its target, so a name pointing at a database this connection
    /// cannot see still creates.
    /// </para>
    /// </summary>
    public static string BuildSynonymCreate(SynonymModel synonym) =>
        $"CREATE SYNONYM {Quote(synonym.Schema, synonym.Name)} FOR {synonym.BaseObjectName};";
}
