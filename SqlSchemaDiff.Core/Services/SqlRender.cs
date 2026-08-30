using System.Text;
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
    public static string BuildSchemaCreate(string schema) =>
        $"IF SCHEMA_ID(N'{Literal(schema)}') IS NULL{Environment.NewLine}" +
        $"    EXEC(N'CREATE SCHEMA {Quote(schema).Replace("'", "''")}');";

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
}
