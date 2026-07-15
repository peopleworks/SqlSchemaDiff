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
            var model = await BuildTableModelAsync(connection, table, cancellationToken);
            var dependencies = model.ForeignKeys
                .Select(x => BuildKey(DbObjectType.Table, x.ReferencedSchema, x.ReferencedTable))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                .ToList();

            result.Add(new DbSchemaObject
            {
                Type = DbObjectType.Table,
                Schema = table.Schema,
                Name = table.Name,
                Definition = SqlRender.BuildTableCreateScript(model),
                Dependencies = dependencies,
                Table = model
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

    internal static DbObjectType ToDbObjectType(string typeCode) => typeCode.Trim() switch
    {
        // sys.objects.type is char(2): single-char codes come back space-padded ("U ").
        "U" => DbObjectType.Table,
        "V" => DbObjectType.View,
        "P" => DbObjectType.StoredProcedure,
        "FN" or "IF" or "TF" or "FS" or "FT" => DbObjectType.Function,
        _ => throw new InvalidOperationException($"Unsupported SQL object type code: '{typeCode.Trim()}'")
    };

    private static async Task<TableModel> BuildTableModelAsync(SqlConnection connection, TableInfo table, CancellationToken cancellationToken)
    {
        return new TableModel
        {
            Schema = table.Schema,
            Name = table.Name,
            Columns = await GetColumnsAsync(connection, table.ObjectId, cancellationToken),
            KeyConstraints = await GetKeyConstraintsAsync(connection, table.ObjectId, cancellationToken),
            ForeignKeys = await GetForeignKeysAsync(connection, table.ObjectId, cancellationToken),
            CheckConstraints = await GetCheckConstraintsAsync(connection, table.ObjectId, cancellationToken),
            Indexes = await GetIndexesAsync(connection, table.ObjectId, cancellationToken)
        };
    }

    private static async Task<List<ColumnModel>> GetColumnsAsync(SqlConnection connection, int objectId, CancellationToken cancellationToken)
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

        var result = new List<ColumnModel>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("@objectId", objectId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            result.Add(new ColumnModel
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

    private static async Task<List<KeyConstraintModel>> GetKeyConstraintsAsync(SqlConnection connection, int objectId, CancellationToken cancellationToken)
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

        var constraints = new List<(KeyConstraintModel Model, int IndexId)>();
        await using(var command = connection.CreateCommand())
        {
            command.CommandText = sql;
            command.Parameters.AddWithValue("@objectId", objectId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while(await reader.ReadAsync(cancellationToken))
            {
                constraints.Add((
                    new KeyConstraintModel
                    {
                        Name = reader.GetString(0),
                        TypeCode = reader.GetString(1),
                        IndexTypeDesc = reader.GetString(3)
                    },
                    reader.GetInt32(2)));
            }
        }

        var result = new List<KeyConstraintModel>(constraints.Count);
        foreach(var (model, indexId) in constraints)
        {
            var columns = await GetIndexColumnsAsync(connection, objectId, indexId, cancellationToken);
            model.Columns = columns.Where(x => !x.IsIncluded).ToList();
            result.Add(model);
        }

        return result;
    }

    private static async Task<List<ForeignKeyModel>> GetForeignKeysAsync(SqlConnection connection, int objectId, CancellationToken cancellationToken)
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

        var fks = new List<(ForeignKeyModel Model, int ObjectId)>();
        await using(var command = connection.CreateCommand())
        {
            command.CommandText = sql;
            command.Parameters.AddWithValue("@objectId", objectId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while(await reader.ReadAsync(cancellationToken))
            {
                fks.Add((
                    new ForeignKeyModel
                    {
                        Name = reader.GetString(1),
                        ReferencedSchema = reader.GetString(2),
                        ReferencedTable = reader.GetString(3),
                        DeleteActionDesc = reader.GetString(4),
                        UpdateActionDesc = reader.GetString(5),
                        IsNotForReplication = reader.GetBoolean(6),
                        IsNotTrusted = reader.GetBoolean(7),
                        IsDisabled = reader.GetBoolean(8)
                    },
                    reader.GetInt32(0)));
            }
        }

        var result = new List<ForeignKeyModel>(fks.Count);
        foreach(var (model, fkObjectId) in fks)
        {
            model.Columns = await GetForeignKeyColumnsAsync(connection, fkObjectId, cancellationToken);
            result.Add(model);
        }

        return result;
    }

    private static async Task<List<ForeignKeyColumnModel>> GetForeignKeyColumnsAsync(SqlConnection connection, int fkObjectId, CancellationToken cancellationToken)
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

        var result = new List<ForeignKeyColumnModel>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("@fkObjectId", fkObjectId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            result.Add(new ForeignKeyColumnModel
            {
                ParentColumn = reader.GetString(0),
                ReferencedColumn = reader.GetString(1)
            });
        }

        return result;
    }

    private static async Task<List<CheckConstraintModel>> GetCheckConstraintsAsync(SqlConnection connection, int objectId, CancellationToken cancellationToken)
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

        var result = new List<CheckConstraintModel>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("@objectId", objectId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            result.Add(new CheckConstraintModel
            {
                Name = reader.GetString(0),
                Definition = reader.GetString(1),
                IsNotTrusted = reader.GetBoolean(2),
                IsDisabled = reader.GetBoolean(3)
            });
        }

        return result;
    }

    private static async Task<List<IndexModel>> GetIndexesAsync(SqlConnection connection, int objectId, CancellationToken cancellationToken)
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

        var indexes = new List<(IndexModel Model, int IndexId)>();
        await using(var command = connection.CreateCommand())
        {
            command.CommandText = sql;
            command.Parameters.AddWithValue("@objectId", objectId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while(await reader.ReadAsync(cancellationToken))
            {
                indexes.Add((
                    new IndexModel
                    {
                        Name = reader.GetString(1),
                        IsUnique = reader.GetBoolean(2),
                        TypeDesc = reader.GetString(3),
                        FilterDefinition = reader.IsDBNull(4) ? null : reader.GetString(4),
                        IsDisabled = reader.GetBoolean(5)
                    },
                    reader.GetInt32(0)));
            }
        }

        var result = new List<IndexModel>(indexes.Count);
        foreach(var (model, indexId) in indexes)
        {
            model.Columns = await GetIndexColumnsAsync(connection, objectId, indexId, cancellationToken);
            result.Add(model);
        }

        return result;
    }

    private static async Task<List<IndexColumnModel>> GetIndexColumnsAsync(SqlConnection connection, int objectId, int indexId, CancellationToken cancellationToken)
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

        var result = new List<IndexColumnModel>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("@objectId", objectId);
        command.Parameters.AddWithValue("@indexId", indexId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            result.Add(new IndexColumnModel
            {
                Name = reader.GetString(0),
                KeyOrdinal = reader.GetByte(1),
                IsDescending = reader.GetBoolean(2),
                IsIncluded = reader.GetBoolean(3),
                IndexColumnId = reader.GetInt32(4)
            });
        }

        return result;
    }

    private static string BuildKey(DbObjectType type, string schema, string name) => $"{type}:{schema}.{name}";

    private static async Task<object?> ExecuteScalarAsync(SqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync(cancellationToken);
    }

    private sealed record TableInfo(int ObjectId, string Schema, string Name);
}
