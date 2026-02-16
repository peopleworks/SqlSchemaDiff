using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;
using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.Services;

public sealed class SqlServerSchemaExtractor
{
    public async Task<DatabaseSnapshot> ExtractAsync(string connectionString, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var databaseName = (await ExecuteScalarAsync(connection, "SELECT DB_NAME();", cancellationToken))?.ToString() ?? "UNKNOWN";

        var objects = new List<DbSchemaObject>();
        objects.AddRange(await ExtractTableObjectsAsync(connection, cancellationToken));
        objects.AddRange(await ExtractProgrammableObjectsAsync(connection, cancellationToken));

        return new DatabaseSnapshot
        {
            DatabaseName = databaseName,
            GeneratedAtUtc = DateTimeOffset.UtcNow,
            Objects = objects
        };
    }

    private static async Task<List<DbSchemaObject>> ExtractTableObjectsAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               t.object_id,
                               s.name AS schema_name,
                               t.name
                           FROM sys.tables t
                           INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                           WHERE t.is_ms_shipped = 0
                           ORDER BY s.name, t.name;
                           """;

        var tables = new List<TableInfo>();
        await using(var command = connection.CreateCommand())
        {
            command.CommandText = sql;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while(await reader.ReadAsync(cancellationToken))
            {
                tables.Add(new TableInfo(
                    reader.GetInt32(0),
                    reader.GetString(1),
                    reader.GetString(2)));
            }
        }

        var result = new List<DbSchemaObject>(tables.Count);
        foreach(var table in tables)
        {
            var tableScript = await BuildTableScriptAsync(connection, table, cancellationToken);
            result.Add(new DbSchemaObject
            {
                Type = DbObjectType.Table,
                Schema = table.Schema,
                Name = table.Name,
                Definition = tableScript.Script,
                Dependencies = tableScript.Dependencies
            });
        }

        return result;
    }

    private static async Task<List<DbSchemaObject>> ExtractProgrammableObjectsAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        var dependencyMap = await GetProgrammableDependenciesAsync(connection, cancellationToken);

        const string sql = """
                           SELECT
                               o.object_id,
                               o.type,
                               s.name AS schema_name,
                               o.name,
                               m.definition
                           FROM sys.objects o
                           INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
                           INNER JOIN sys.sql_modules m ON m.object_id = o.object_id
                           WHERE o.is_ms_shipped = 0
                             AND o.type IN ('V', 'P', 'FN', 'IF', 'TF', 'FS', 'FT')
                           ORDER BY
                               CASE o.type
                                   WHEN 'FN' THEN 1
                                   WHEN 'IF' THEN 1
                                   WHEN 'TF' THEN 1
                                   WHEN 'FS' THEN 1
                                   WHEN 'FT' THEN 1
                                   WHEN 'V' THEN 2
                                   WHEN 'P' THEN 3
                                   ELSE 99
                               END,
                               s.name,
                               o.name;
                           """;

        var result = new List<DbSchemaObject>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            var objectId = reader.GetInt32(0);
            var typeCode = reader.GetString(1);
            var schema = reader.GetString(2);
            var name = reader.GetString(3);
            var definition = reader.GetString(4).Trim();
            var dependencies = dependencyMap.TryGetValue(objectId, out var objectDependencies)
                ? objectDependencies
                : new List<string>();

            result.Add(new DbSchemaObject
            {
                Type = ToDbObjectType(typeCode),
                Schema = schema,
                Name = name,
                Definition = definition,
                Dependencies = dependencies
            });
        }

        return result;
    }

    private static async Task<Dictionary<int, List<string>>> GetProgrammableDependenciesAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               sed.referencing_id,
                               ro.type AS referenced_type,
                               rs.name AS referenced_schema,
                               ro.name AS referenced_name
                           FROM sys.sql_expression_dependencies sed
                           INNER JOIN sys.objects ro ON ro.object_id = sed.referenced_id
                           INNER JOIN sys.schemas rs ON rs.schema_id = ro.schema_id
                           WHERE sed.referenced_id IS NOT NULL
                             AND sed.referencing_id IN (
                                 SELECT object_id
                                 FROM sys.objects
                                 WHERE is_ms_shipped = 0
                                   AND type IN ('V', 'P', 'FN', 'IF', 'TF', 'FS', 'FT')
                             )
                             AND ro.is_ms_shipped = 0
                             AND ro.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF', 'FS', 'FT');
                           """;

        var map = new Dictionary<int, HashSet<string>>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            var referencingId = reader.GetInt32(0);
            var referencedTypeCode = reader.GetString(1);
            var referencedSchema = reader.GetString(2);
            var referencedName = reader.GetString(3);

            var normalizedType = ToDbObjectType(referencedTypeCode);
            var dependencyKey = BuildKey(normalizedType, referencedSchema, referencedName);

            if(!map.TryGetValue(referencingId, out var set))
            {
                set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                map[referencingId] = set;
            }

            set.Add(dependencyKey);
        }

        return map.ToDictionary(
            kvp => kvp.Key,
            kvp => kvp.Value.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToList());
    }

    private static DbObjectType ToDbObjectType(string typeCode) => typeCode switch
    {
        "U" => DbObjectType.Table,
        "V" => DbObjectType.View,
        "P" => DbObjectType.StoredProcedure,
        "FN" or "IF" or "TF" or "FS" or "FT" => DbObjectType.Function,
        _ => throw new InvalidOperationException($"Unsupported SQL object type code: {typeCode}")
    };

    private static async Task<TableScriptResult> BuildTableScriptAsync(SqlConnection connection, TableInfo table, CancellationToken cancellationToken)
    {
        var columns = await GetColumnsAsync(connection, table.ObjectId, cancellationToken);
        var keyConstraints = await GetKeyConstraintsAsync(connection, table.ObjectId, cancellationToken);
        var foreignKeys = await GetForeignKeysAsync(connection, table.ObjectId, cancellationToken);
        var checkConstraints = await GetCheckConstraintsAsync(connection, table.ObjectId, cancellationToken);
        var indexes = await GetIndexesAsync(connection, table.ObjectId, cancellationToken);
        var dependencies = foreignKeys
            .Select(x => BuildKey(DbObjectType.Table, x.ReferencedSchema, x.ReferencedTable))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var tableIdentifier = Quote(table.Schema, table.Name);
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE TABLE {tableIdentifier}");
        sb.AppendLine("(");
        for(var i = 0; i < columns.Count; i++)
        {
            var isLastColumn = i == columns.Count - 1;
            sb.Append("    ");
            sb.Append(BuildColumnDefinition(columns[i]));
            if(!isLastColumn)
                sb.Append(',');
            sb.AppendLine();
        }
        sb.AppendLine(");");
        sb.AppendLine("GO");
        sb.AppendLine();

        foreach(var keyConstraint in keyConstraints)
        {
            var columnsSql = string.Join(", ", keyConstraint.Columns.Select(BuildIndexColumnExpression));
            var constraintKind = keyConstraint.TypeCode == "PK" ? "PRIMARY KEY" : "UNIQUE";
            var indexKind = keyConstraint.IndexTypeDesc.Contains("CLUSTERED", StringComparison.OrdinalIgnoreCase)
                ? keyConstraint.IndexTypeDesc.Replace('_', ' ')
                : "NONCLUSTERED";

            sb.AppendLine(
                $"ALTER TABLE {tableIdentifier} ADD CONSTRAINT {Quote(keyConstraint.Name)} {constraintKind} {indexKind} ({columnsSql});");
            sb.AppendLine("GO");
            sb.AppendLine();
        }

        foreach(var foreignKey in foreignKeys)
        {
            var fkColumnsSql = string.Join(", ", foreignKey.Columns.Select(x => Quote(x.ParentColumn)));
            var refColumnsSql = string.Join(", ", foreignKey.Columns.Select(x => Quote(x.ReferencedColumn)));
            var withCheck = foreignKey.IsNotTrusted ? "WITH NOCHECK" : "WITH CHECK";

            sb.Append($"ALTER TABLE {tableIdentifier} {withCheck} ADD CONSTRAINT {Quote(foreignKey.Name)}");
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

            sb.AppendLine(";");
            sb.AppendLine("GO");

            if(foreignKey.IsDisabled)
            {
                sb.AppendLine($"ALTER TABLE {tableIdentifier} NOCHECK CONSTRAINT {Quote(foreignKey.Name)};");
                sb.AppendLine("GO");
            }

            sb.AppendLine();
        }

        foreach(var checkConstraint in checkConstraints)
        {
            var withCheck = checkConstraint.IsNotTrusted ? "WITH NOCHECK" : "WITH CHECK";
            sb.AppendLine(
                $"ALTER TABLE {tableIdentifier} {withCheck} ADD CONSTRAINT {Quote(checkConstraint.Name)} CHECK {checkConstraint.Definition};");
            sb.AppendLine("GO");

            if(checkConstraint.IsDisabled)
            {
                sb.AppendLine($"ALTER TABLE {tableIdentifier} NOCHECK CONSTRAINT {Quote(checkConstraint.Name)};");
                sb.AppendLine("GO");
            }

            sb.AppendLine();
        }

        foreach(var index in indexes)
        {
            var keyColumns = index.Columns.Where(x => !x.IsIncluded).Select(BuildIndexColumnExpression).ToList();
            var includedColumns = index.Columns.Where(x => x.IsIncluded).Select(x => Quote(x.Name)).ToList();

            sb.Append($"CREATE {(index.IsUnique ? "UNIQUE " : string.Empty)}{index.TypeDesc.Replace('_', ' ')} INDEX {Quote(index.Name)}");
            sb.Append($" ON {tableIdentifier} ({string.Join(", ", keyColumns)})");
            if(includedColumns.Count > 0)
                sb.Append($" INCLUDE ({string.Join(", ", includedColumns)})");
            if(!string.IsNullOrWhiteSpace(index.FilterDefinition))
                sb.Append($" WHERE {index.FilterDefinition}");
            sb.AppendLine(";");
            sb.AppendLine("GO");

            if(index.IsDisabled)
            {
                sb.AppendLine($"ALTER INDEX {Quote(index.Name)} ON {tableIdentifier} DISABLE;");
                sb.AppendLine("GO");
            }

            sb.AppendLine();
        }

        return new TableScriptResult(sb.ToString().TrimEnd(), dependencies);
    }

    private static async Task<List<ColumnInfo>> GetColumnsAsync(SqlConnection connection, int objectId, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               c.column_id,
                               c.name,
                               ts.name AS type_schema,
                               ty.name AS type_name,
                               ty.is_user_defined,
                               c.max_length,
                               c.precision,
                               c.scale,
                               c.is_nullable,
                               c.is_identity,
                               c.is_computed,
                               c.collation_name,
                               c.is_rowguidcol,
                               cc.definition AS computed_definition,
                               cc.is_persisted,
                               dc.name AS default_name,
                               dc.definition AS default_definition,
                               CONVERT(varchar(100), ic.seed_value) AS seed_value_text,
                               CONVERT(varchar(100), ic.increment_value) AS increment_value_text
                           FROM sys.columns c
                           INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                           INNER JOIN sys.schemas ts ON ts.schema_id = ty.schema_id
                           LEFT JOIN sys.computed_columns cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
                           LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
                           LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                           WHERE c.object_id = @objectId
                           ORDER BY c.column_id;
                           """;

        var result = new List<ColumnInfo>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("@objectId", objectId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            result.Add(new ColumnInfo
            {
                Name = reader.GetString(1),
                TypeSchema = reader.GetString(2),
                TypeName = reader.GetString(3),
                IsUserDefinedType = reader.GetBoolean(4),
                MaxLength = reader.GetInt16(5),
                Precision = reader.GetByte(6),
                Scale = reader.GetByte(7),
                IsNullable = reader.GetBoolean(8),
                IsIdentity = reader.GetBoolean(9),
                IsComputed = reader.GetBoolean(10),
                CollationName = reader.IsDBNull(11) ? null : reader.GetString(11),
                IsRowGuid = reader.GetBoolean(12),
                ComputedDefinition = reader.IsDBNull(13) ? null : reader.GetString(13),
                IsPersisted = !reader.IsDBNull(14) && reader.GetBoolean(14),
                DefaultName = reader.IsDBNull(15) ? null : reader.GetString(15),
                DefaultDefinition = reader.IsDBNull(16) ? null : reader.GetString(16),
                IdentitySeed = reader.IsDBNull(17) ? null : reader.GetString(17),
                IdentityIncrement = reader.IsDBNull(18) ? null : reader.GetString(18)
            });
        }

        return result;
    }

    private static async Task<List<KeyConstraintInfo>> GetKeyConstraintsAsync(SqlConnection connection, int objectId, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               kc.name,
                               kc.type,
                               i.index_id,
                               i.type_desc
                           FROM sys.key_constraints kc
                           INNER JOIN sys.indexes i
                               ON i.object_id = kc.parent_object_id
                              AND i.index_id = kc.unique_index_id
                           WHERE kc.parent_object_id = @objectId
                           ORDER BY CASE WHEN kc.type = 'PK' THEN 0 ELSE 1 END, kc.name;
                           """;

        var constraints = new List<KeyConstraintInfo>();
        await using(var command = connection.CreateCommand())
        {
            command.CommandText = sql;
            command.Parameters.AddWithValue("@objectId", objectId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while(await reader.ReadAsync(cancellationToken))
            {
                constraints.Add(new KeyConstraintInfo(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetInt32(2),
                    reader.GetString(3),
                    new List<IndexColumnInfo>()));
            }
        }

        for(var i = 0; i < constraints.Count; i++)
        {
            var columns = await GetIndexColumnsAsync(connection, objectId, constraints[i].IndexId, cancellationToken);
            constraints[i] = constraints[i] with { Columns = columns.Where(x => !x.IsIncluded).ToList() };
        }

        return constraints;
    }

    private static async Task<List<ForeignKeyInfo>> GetForeignKeysAsync(SqlConnection connection, int objectId, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               fk.object_id,
                               fk.name,
                               rs.name AS referenced_schema,
                               rt.name AS referenced_table,
                               fk.delete_referential_action_desc,
                               fk.update_referential_action_desc,
                               fk.is_not_for_replication,
                               fk.is_not_trusted,
                               fk.is_disabled
                           FROM sys.foreign_keys fk
                           INNER JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
                           INNER JOIN sys.schemas rs ON rs.schema_id = rt.schema_id
                           WHERE fk.parent_object_id = @objectId
                           ORDER BY fk.name;
                           """;

        var fks = new List<ForeignKeyInfo>();
        await using(var command = connection.CreateCommand())
        {
            command.CommandText = sql;
            command.Parameters.AddWithValue("@objectId", objectId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while(await reader.ReadAsync(cancellationToken))
            {
                fks.Add(new ForeignKeyInfo(
                    reader.GetInt32(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.GetString(4),
                    reader.GetString(5),
                    reader.GetBoolean(6),
                    reader.GetBoolean(7),
                    reader.GetBoolean(8),
                    new List<ForeignKeyColumnInfo>()));
            }
        }

        for(var i = 0; i < fks.Count; i++)
        {
            var columns = await GetForeignKeyColumnsAsync(connection, fks[i].ObjectId, cancellationToken);
            fks[i] = fks[i] with { Columns = columns };
        }

        return fks;
    }

    private static async Task<List<ForeignKeyColumnInfo>> GetForeignKeyColumnsAsync(SqlConnection connection, int fkObjectId, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               pc.name AS parent_column,
                               rc.name AS referenced_column
                           FROM sys.foreign_key_columns fkc
                           INNER JOIN sys.columns pc
                               ON pc.object_id = fkc.parent_object_id
                              AND pc.column_id = fkc.parent_column_id
                           INNER JOIN sys.columns rc
                               ON rc.object_id = fkc.referenced_object_id
                              AND rc.column_id = fkc.referenced_column_id
                           WHERE fkc.constraint_object_id = @fkObjectId
                           ORDER BY fkc.constraint_column_id;
                           """;

        var result = new List<ForeignKeyColumnInfo>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("@fkObjectId", fkObjectId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            result.Add(new ForeignKeyColumnInfo(reader.GetString(0), reader.GetString(1)));
        }

        return result;
    }

    private static async Task<List<CheckConstraintInfo>> GetCheckConstraintsAsync(SqlConnection connection, int objectId, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               cc.name,
                               cc.definition,
                               cc.is_not_trusted,
                               cc.is_disabled
                           FROM sys.check_constraints cc
                           WHERE cc.parent_object_id = @objectId
                           ORDER BY cc.name;
                           """;

        var result = new List<CheckConstraintInfo>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("@objectId", objectId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            result.Add(new CheckConstraintInfo(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetBoolean(2),
                reader.GetBoolean(3)));
        }

        return result;
    }

    private static async Task<List<IndexInfo>> GetIndexesAsync(SqlConnection connection, int objectId, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               i.index_id,
                               i.name,
                               i.is_unique,
                               i.type_desc,
                               i.filter_definition,
                               i.is_disabled
                           FROM sys.indexes i
                           WHERE i.object_id = @objectId
                             AND i.is_hypothetical = 0
                             AND i.name IS NOT NULL
                             AND i.is_primary_key = 0
                             AND i.is_unique_constraint = 0
                             AND i.type IN (1, 2)
                           ORDER BY i.name;
                           """;

        var indexes = new List<IndexInfo>();
        await using(var command = connection.CreateCommand())
        {
            command.CommandText = sql;
            command.Parameters.AddWithValue("@objectId", objectId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while(await reader.ReadAsync(cancellationToken))
            {
                indexes.Add(new IndexInfo(
                    reader.GetInt32(0),
                    reader.GetString(1),
                    reader.GetBoolean(2),
                    reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetString(4),
                    reader.GetBoolean(5),
                    new List<IndexColumnInfo>()));
            }
        }

        for(var i = 0; i < indexes.Count; i++)
        {
            var columns = await GetIndexColumnsAsync(connection, objectId, indexes[i].IndexId, cancellationToken);
            indexes[i] = indexes[i] with { Columns = columns };
        }

        return indexes;
    }

    private static async Task<List<IndexColumnInfo>> GetIndexColumnsAsync(SqlConnection connection, int objectId, int indexId, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               c.name,
                               ic.key_ordinal,
                               ic.is_descending_key,
                               ic.is_included_column,
                               ic.index_column_id
                           FROM sys.index_columns ic
                           INNER JOIN sys.columns c
                               ON c.object_id = ic.object_id
                              AND c.column_id = ic.column_id
                           WHERE ic.object_id = @objectId
                             AND ic.index_id = @indexId
                           ORDER BY ic.is_included_column, ic.key_ordinal, ic.index_column_id;
                           """;

        var result = new List<IndexColumnInfo>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("@objectId", objectId);
        command.Parameters.AddWithValue("@indexId", indexId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            result.Add(new IndexColumnInfo(
                reader.GetString(0),
                reader.GetByte(1),
                reader.GetBoolean(2),
                reader.GetBoolean(3),
                reader.GetInt32(4)));
        }

        return result;
    }

    private static string BuildColumnDefinition(ColumnInfo column)
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

    private static string BuildType(ColumnInfo column)
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

    private static string BuildIndexColumnExpression(IndexColumnInfo column)
    {
        if(column.IsIncluded)
            return Quote(column.Name);

        return $"{Quote(column.Name)} {(column.IsDescending ? "DESC" : "ASC")}";
    }

    private static string? ToReferentialAction(string action) => action.ToUpperInvariant() switch
    {
        "NO_ACTION" => null,
        "CASCADE" => "CASCADE",
        "SET_NULL" => "SET NULL",
        "SET_DEFAULT" => "SET DEFAULT",
        _ => null
    };

    private static string BuildKey(DbObjectType type, string schema, string name) => $"{type}:{schema}.{name}";

    private static string Quote(string name) => $"[{name.Replace("]", "]]")}]";

    private static string Quote(string schema, string name) => $"{Quote(schema)}.{Quote(name)}";

    private static async Task<object?> ExecuteScalarAsync(SqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync(cancellationToken);
    }

    private sealed record TableInfo(int ObjectId, string Schema, string Name);

    private sealed record TableScriptResult(string Script, List<string> Dependencies);

    private sealed class ColumnInfo
    {
        public string Name { get; init; } = string.Empty;
        public string TypeSchema { get; init; } = string.Empty;
        public string TypeName { get; init; } = string.Empty;
        public bool IsUserDefinedType { get; init; }
        public short MaxLength { get; init; }
        public byte Precision { get; init; }
        public byte Scale { get; init; }
        public bool IsNullable { get; init; }
        public bool IsIdentity { get; init; }
        public bool IsComputed { get; init; }
        public string? CollationName { get; init; }
        public bool IsRowGuid { get; init; }
        public string? ComputedDefinition { get; init; }
        public bool IsPersisted { get; init; }
        public string? DefaultName { get; init; }
        public string? DefaultDefinition { get; init; }
        public string? IdentitySeed { get; init; }
        public string? IdentityIncrement { get; init; }
    }

    private sealed record KeyConstraintInfo(
        string Name,
        string TypeCode,
        int IndexId,
        string IndexTypeDesc,
        List<IndexColumnInfo> Columns);

    private sealed record ForeignKeyInfo(
        int ObjectId,
        string Name,
        string ReferencedSchema,
        string ReferencedTable,
        string DeleteActionDesc,
        string UpdateActionDesc,
        bool IsNotForReplication,
        bool IsNotTrusted,
        bool IsDisabled,
        List<ForeignKeyColumnInfo> Columns);

    private sealed record ForeignKeyColumnInfo(string ParentColumn, string ReferencedColumn);

    private sealed record CheckConstraintInfo(string Name, string Definition, bool IsNotTrusted, bool IsDisabled);

    private sealed record IndexInfo(
        int IndexId,
        string Name,
        bool IsUnique,
        string TypeDesc,
        string? FilterDefinition,
        bool IsDisabled,
        List<IndexColumnInfo> Columns);

    private sealed record IndexColumnInfo(
        string Name,
        byte KeyOrdinal,
        bool IsDescending,
        bool IsIncluded,
        int IndexColumnId);
}
