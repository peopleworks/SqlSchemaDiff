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
        var authorization = string.IsNullOrWhiteSpace(owner) || string.Equals(owner, "dbo", StringComparison.OrdinalIgnoreCase)
            ? string.Empty
            : $" AUTHORIZATION {Quote(owner)}";

        return
            $"IF SCHEMA_ID(N'{Literal(schema)}') IS NULL{Environment.NewLine}" +
            $"    EXEC(N'{Literal($"CREATE SCHEMA {Quote(schema)}{authorization}")}');";
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

        if(column.IsIdentity)
        {
            var seed = string.IsNullOrWhiteSpace(column.IdentitySeed) ? "1" : column.IdentitySeed;
            var increment = string.IsNullOrWhiteSpace(column.IdentityIncrement) ? "1" : column.IdentityIncrement;
            sb.Append($" IDENTITY({seed},{increment})");
        }

        if(column.IsRowGuid)
            sb.Append(" ROWGUIDCOL");

        sb.Append(column.IsNullable ? " NULL" : " NOT NULL");

        if(!string.IsNullOrWhiteSpace(column.DefaultDefinition))
        {
            sb.Append(BuildConstraintNameClause(column.DefaultName, column.DefaultIsSystemNamed));
            sb.Append($" DEFAULT {column.DefaultDefinition}");
        }

        return sb.ToString();
    }

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
               $"{constraintKind} {indexKind} ({columnsSql});";
    }

    /// <summary>
    /// True only for a clustered index. Note that <c>"NONCLUSTERED".Contains("CLUSTERED")</c>
    /// is also true, which is why this compares the whole descriptor.
    /// </summary>
    public static bool IsClustered(string indexTypeDesc) =>
        string.Equals(indexTypeDesc?.Trim(), "CLUSTERED", StringComparison.OrdinalIgnoreCase);

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
        var keyColumns = index.Columns.Where(x => !x.IsIncluded).Select(BuildIndexColumnExpression).ToList();
        var includedColumns = index.Columns.Where(x => x.IsIncluded).Select(x => Quote(x.Name)).ToList();

        var sb = new StringBuilder();
        sb.Append($"CREATE {(index.IsUnique ? "UNIQUE " : string.Empty)}{index.TypeDesc.Replace('_', ' ')} INDEX {Quote(index.Name)}");
        sb.Append($" ON {TableIdentifier(table)} ({string.Join(", ", keyColumns)})");
        if(includedColumns.Count > 0)
            sb.Append($" INCLUDE ({string.Join(", ", includedColumns)})");
        if(!string.IsNullOrWhiteSpace(index.FilterDefinition))
            sb.Append($" WHERE {index.FilterDefinition}");
        sb.Append(';');
        return sb.ToString();
    }

    public static string BuildIndexDrop(TableModel table, IndexModel index) =>
        $"DROP INDEX {Quote(index.Name)} ON {TableIdentifier(table)};";

    /// <summary>
    /// Drops a constraint by name. Drops always come from the target model, so the
    /// captured name is the one that actually exists there — including the random
    /// name SQL Server generated for an unnamed constraint.
    /// </summary>
    public static string BuildConstraintDrop(TableModel table, string name) =>
        $"ALTER TABLE {TableIdentifier(table)} DROP CONSTRAINT {Quote(name)};";

    /// <summary>Renders the complete CREATE TABLE script (table + keys + FKs + checks + indexes).</summary>
    public static string BuildTableCreateScript(TableModel table)
    {
        var tableIdentifier = TableIdentifier(table);
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE TABLE {tableIdentifier}");
        sb.AppendLine("(");
        for(var i = 0; i < table.Columns.Count; i++)
        {
            var isLastColumn = i == table.Columns.Count - 1;
            sb.Append("    ");
            sb.Append(BuildColumnDefinition(table.Columns[i]));
            if(!isLastColumn)
                sb.Append(',');
            sb.AppendLine();
        }
        sb.AppendLine(");");
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

            if(foreignKey.IsDisabled && !foreignKey.IsSystemNamed)
            {
                sb.AppendLine($"ALTER TABLE {tableIdentifier} NOCHECK CONSTRAINT {Quote(foreignKey.Name)};");
                sb.AppendLine("GO");
            }

            sb.AppendLine();
        }

        foreach(var check in table.CheckConstraints)
        {
            sb.AppendLine(BuildCheckConstraintAdd(table, check));
            sb.AppendLine("GO");

            if(check.IsDisabled && !check.IsSystemNamed)
            {
                sb.AppendLine($"ALTER TABLE {tableIdentifier} NOCHECK CONSTRAINT {Quote(check.Name)};");
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
                sb.AppendLine($"ALTER INDEX {Quote(index.Name)} ON {tableIdentifier} DISABLE;");
                sb.AppendLine("GO");
            }

            sb.AppendLine();
        }

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
}
