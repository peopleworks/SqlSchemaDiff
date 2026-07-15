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
    public static string Quote(string name) => $"[{name.Replace("]", "]]")}]";

    public static string Quote(string schema, string name) => $"{Quote(schema)}.{Quote(name)}";

    public static string TableIdentifier(TableModel table) => Quote(table.Schema, table.Name);

    public static string BuildType(ColumnModel column)
    {
        if(column.IsUserDefinedType)
            return $"{Quote(column.TypeSchema)}.{Quote(column.TypeName)}";

        var name = column.TypeName;
        return name.ToLowerInvariant() switch
        {
            "varchar" or "char" or "varbinary" or "binary" =>
                $"{name}({(column.MaxLength == -1 ? "MAX" : column.MaxLength.ToString())})",
            "nvarchar" or "nchar" =>
                $"{name}({(column.MaxLength == -1 ? "MAX" : (column.MaxLength / 2).ToString())})",
            "decimal" or "numeric" =>
                $"{name}({column.Precision},{column.Scale})",
            "datetime2" or "datetimeoffset" or "time" =>
                $"{name}({column.Scale})",
            "float" when column.Precision != 53 =>
                $"{name}({column.Precision})",
            _ => name
        };
    }

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

        if(!string.IsNullOrWhiteSpace(column.CollationName))
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
            if(!string.IsNullOrWhiteSpace(column.DefaultName))
                sb.Append($" CONSTRAINT {Quote(column.DefaultName)}");
            sb.Append($" DEFAULT {column.DefaultDefinition}");
        }

        return sb.ToString();
    }

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
        var indexKind = keyConstraint.IndexTypeDesc.Contains("CLUSTERED", StringComparison.OrdinalIgnoreCase)
            ? keyConstraint.IndexTypeDesc.Replace('_', ' ')
            : "NONCLUSTERED";

        return $"ALTER TABLE {TableIdentifier(table)} ADD CONSTRAINT {Quote(keyConstraint.Name)} " +
               $"{constraintKind} {indexKind} ({columnsSql});";
    }

    public static string BuildForeignKeyAdd(TableModel table, ForeignKeyModel foreignKey)
    {
        var fkColumnsSql = string.Join(", ", foreignKey.Columns.Select(x => Quote(x.ParentColumn)));
        var refColumnsSql = string.Join(", ", foreignKey.Columns.Select(x => Quote(x.ReferencedColumn)));
        var withCheck = foreignKey.IsNotTrusted ? "WITH NOCHECK" : "WITH CHECK";

        var sb = new StringBuilder();
        sb.Append($"ALTER TABLE {TableIdentifier(table)} {withCheck} ADD CONSTRAINT {Quote(foreignKey.Name)}");
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
        return $"ALTER TABLE {TableIdentifier(table)} {withCheck} ADD CONSTRAINT {Quote(check.Name)} CHECK {check.Definition};";
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

            if(foreignKey.IsDisabled)
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

            if(check.IsDisabled)
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
