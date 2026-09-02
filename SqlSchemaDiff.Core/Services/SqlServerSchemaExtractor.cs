using Microsoft.Data.SqlClient;
using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.Services;

/// <summary>
/// Reads a database's structure into a <see cref="DatabaseSnapshot"/>.
/// Metadata is read with one set-based query per catalog view for the whole
/// database and grouped in memory, so extraction costs a fixed number of round
/// trips regardless of how many tables, indexes or constraints exist.
/// </summary>
public sealed class SqlServerSchemaExtractor
{
    /// <summary>Objects skipped or partially captured during extraction, with the reason.</summary>
    public List<string> Notices { get; } = new();

    public async Task<DatabaseSnapshot> ExtractAsync(string connectionString, CancellationToken cancellationToken)
    {
        Notices.Clear();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var databaseName = (await ExecuteScalarAsync(connection, "SELECT DB_NAME();", cancellationToken))?.ToString() ?? "UNKNOWN";

        var tables = await GetTablesAsync(connection, cancellationToken);
        var periods = await GetPeriodsAsync(connection, cancellationToken);
        var columns = await GetColumnsAsync(connection, cancellationToken);
        var indexColumns = await GetIndexColumnsAsync(connection, cancellationToken);
        var compression = await GetDataCompressionAsync(connection, cancellationToken);
        var keyConstraints = await GetKeyConstraintsAsync(connection, indexColumns, compression, cancellationToken);
        var foreignKeys = await GetForeignKeysAsync(connection, cancellationToken);
        var checkConstraints = await GetCheckConstraintsAsync(connection, cancellationToken);
        var indexes = await GetIndexesAsync(connection, indexColumns, compression, cancellationToken);

        var objects = new List<DbSchemaObject>();
        foreach(var table in tables)
        {
            periods.TryGetValue(table.ObjectId, out var period);
            var model = new TableModel
            {
                Schema = table.Schema,
                Name = table.Name,
                Columns = Take(columns, table.ObjectId),
                KeyConstraints = Take(keyConstraints, table.ObjectId),
                ForeignKeys = Take(foreignKeys, table.ObjectId),
                CheckConstraints = Take(checkConstraints, table.ObjectId),
                Indexes = Take(indexes, table.ObjectId),
                // index_id 0 is a heap, 1 a clustered index; a table has exactly one
                // of the two and its compression is the table's own.
                DataCompression = LookupCompression(compression, table.ObjectId, 0)
                                  ?? LookupCompression(compression, table.ObjectId, 1),
                IsMemoryOptimized = table.IsMemoryOptimized,
                Durability = table.IsMemoryOptimized ? table.DurabilityDesc : null,
                TemporalType = table.TemporalTypeDesc,
                HistoryTableSchema = table.HistorySchema,
                HistoryTableName = table.HistoryName,
                PeriodStartColumn = period.Start,
                PeriodEndColumn = period.End
            };

            var dependencies = model.ForeignKeys
                .Select(x => BuildKey(DbObjectType.Table, x.ReferencedSchema, x.ReferencedTable))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                .ToList();

            objects.Add(new DbSchemaObject
            {
                Type = DbObjectType.Table,
                Schema = table.Schema,
                Name = table.Name,
                Definition = SqlRender.BuildTableCreateScript(model),
                Dependencies = dependencies,
                Table = model
            });
        }

        objects.AddRange(await ExtractProgrammableObjectsAsync(connection, cancellationToken));

        var allTypes = await GetAliasTypesAsync(connection, cancellationToken);
        var usedTypeKeys = objects
            .Where(x => x.Table is not null)
            .SelectMany(x => x.Table!.Columns)
            .Where(c => c.IsUserDefinedType)
            .Select(c => $"{c.TypeSchema}.{c.TypeName}")
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var usedTypes = allTypes
            .Where(t => usedTypeKeys.Contains($"{t.Schema}.{t.Name}"))
            .OrderBy(t => t.Schema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(t => t.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var schemas = objects
            .Select(x => x.Schema)
            .Concat(usedTypes.Select(t => t.Schema))
            .Where(x => !string.Equals(x, "dbo", StringComparison.OrdinalIgnoreCase))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return new DatabaseSnapshot
        {
            DatabaseName = databaseName,
            GeneratedAtUtc = DateTimeOffset.UtcNow,
            Schemas = schemas,
            Types = usedTypes,
            Objects = objects
        };
    }

    // ---------------------------------------------------------------- tables

    private async Task<List<TableInfo>> GetTablesAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               t.object_id,
                               s.name AS schema_name,
                               t.name,
                               t.temporal_type,
                               t.temporal_type_desc,
                               hs.name AS history_schema,
                               ht.name AS history_name,
                               t.is_memory_optimized,
                               t.durability_desc
                           FROM sys.tables t
                           INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                           LEFT JOIN sys.tables ht ON ht.object_id = t.history_table_id
                           LEFT JOIN sys.schemas hs ON hs.schema_id = ht.schema_id
                           WHERE t.is_ms_shipped = 0
                           ORDER BY s.name, t.name;
                           """;

        var tables = new List<TableInfo>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            var schema = reader.GetString(1);
            var name = reader.GetString(2);
            var temporalType = reader.GetByte(3);

            // 1 = history table of a system-versioned table. SQL Server creates and owns
            // it, so scripting it separately would produce a CREATE that cannot be applied.
            if(temporalType == 1)
            {
                Notices.Add($"skipped [{schema}].[{name}]: temporal history table (managed by SQL Server)");
                continue;
            }

            if(temporalType == 2)
                Notices.Add($"[{schema}].[{name}] is system-versioned; the SYSTEM_VERSIONING clause is not scripted");

            // A memory-optimized table has to declare its indexes inside CREATE TABLE
            // - it cannot be created without at least a primary key, and CREATE INDEX
            // is rejected on one - so the script this renderer emits would not apply.
            // Say so rather than hand over a CREATE that fails halfway.
            var isMemoryOptimized = reader.GetBoolean(7);
            if(isMemoryOptimized)
            {
                Notices.Add($"[{schema}].[{name}] is memory-optimized; its MEMORY_OPTIMIZED and " +
                            "DURABILITY options and its inline indexes are not scripted");
            }

            // NON_TEMPORAL is stored as null so an ordinary table carries nothing new
            // in its snapshot and keeps comparing equal to one taken by an older build.
            tables.Add(new TableInfo(
                reader.GetInt32(0), schema, name,
                temporalType == 0 ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                isMemoryOptimized,
                reader.IsDBNull(8) ? null : reader.GetString(8)));
        }

        return tables;
    }

    /// <summary>
    /// The SYSTEM_TIME period columns of every temporal table. Captured so a future
    /// composer can emit <c>PERIOD FOR SYSTEM_TIME</c>; nothing renders them yet.
    /// </summary>
    private static async Task<Dictionary<int, (string? Start, string? End)>> GetPeriodsAsync(
        SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               p.object_id,
                               sc.name AS start_column,
                               ec.name AS end_column
                           FROM sys.periods p
                           INNER JOIN sys.tables t ON t.object_id = p.object_id AND t.is_ms_shipped = 0
                           INNER JOIN sys.columns sc ON sc.object_id = p.object_id AND sc.column_id = p.start_column_id
                           INNER JOIN sys.columns ec ON ec.object_id = p.object_id AND ec.column_id = p.end_column_id;
                           """;

        var result = new Dictionary<int, (string? Start, string? End)>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
            result[reader.GetInt32(0)] = (reader.GetString(1), reader.GetString(2));

        return result;
    }

    private static async Task<ILookup<int, ColumnModel>> GetColumnsAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               c.object_id,
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
                               dc.is_system_named AS default_is_system_named,
                               CONVERT(varchar(100), ic.seed_value) AS seed_value_text,
                               CONVERT(varchar(100), ic.increment_value) AS increment_value_text,
                               c.is_sparse
                           FROM sys.columns c
                           INNER JOIN sys.tables t ON t.object_id = c.object_id AND t.is_ms_shipped = 0
                           INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                           INNER JOIN sys.schemas ts ON ts.schema_id = ty.schema_id
                           LEFT JOIN sys.computed_columns cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
                           LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
                           LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                           ORDER BY c.object_id, c.column_id;
                           """;

        var rows = new List<(int ObjectId, ColumnModel Model)>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            rows.Add((reader.GetInt32(0), new ColumnModel
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
                DefaultIsSystemNamed = !reader.IsDBNull(17) && reader.GetBoolean(17),
                IdentitySeed = reader.IsDBNull(18) ? null : reader.GetString(18),
                IdentityIncrement = reader.IsDBNull(19) ? null : reader.GetString(19),
                IsSparse = reader.GetBoolean(20)
            }));
        }

        return rows.ToLookup(x => x.ObjectId, x => x.Model);
    }

    // ----------------------------------------------------------- constraints

    private static async Task<ILookup<int, KeyConstraintModel>> GetKeyConstraintsAsync(
        SqlConnection connection,
        Dictionary<(int ObjectId, int IndexId), List<IndexColumnModel>> indexColumns,
        Dictionary<(int ObjectId, int IndexId), string> compression,
        CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               kc.parent_object_id,
                               kc.name,
                               kc.type,
                               kc.unique_index_id,
                               i.type_desc,
                               kc.is_system_named,
                               i.fill_factor,
                               i.is_padded,
                               i.ignore_dup_key,
                               i.allow_row_locks,
                               i.allow_page_locks
                           FROM sys.key_constraints kc
                           INNER JOIN sys.tables t ON t.object_id = kc.parent_object_id AND t.is_ms_shipped = 0
                           INNER JOIN sys.indexes i
                               ON i.object_id = kc.parent_object_id
                              AND i.index_id = kc.unique_index_id
                           ORDER BY kc.parent_object_id, CASE WHEN kc.type = 'PK' THEN 0 ELSE 1 END, kc.name;
                           """;

        var rows = new List<(int ObjectId, KeyConstraintModel Model)>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            var parentObjectId = reader.GetInt32(0);
            var indexId = reader.GetInt32(3);
            rows.Add((parentObjectId, new KeyConstraintModel
            {
                Name = reader.GetString(1),
                TypeCode = reader.GetString(2).Trim(),
                IndexTypeDesc = reader.GetString(4),
                IsSystemNamed = reader.GetBoolean(5),
                Columns = Lookup(indexColumns, parentObjectId, indexId).Where(x => !x.IsIncluded).ToList(),
                FillFactor = reader.GetByte(6),
                IsPadded = reader.GetBoolean(7),
                IgnoreDupKey = reader.GetBoolean(8),
                AllowRowLocks = reader.GetBoolean(9),
                AllowPageLocks = reader.GetBoolean(10),
                DataCompression = LookupCompression(compression, parentObjectId, indexId)
            }));
        }

        return rows.ToLookup(x => x.ObjectId, x => x.Model);
    }

    private static async Task<ILookup<int, ForeignKeyModel>> GetForeignKeysAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               fk.parent_object_id,
                               fk.object_id,
                               fk.name,
                               rs.name AS referenced_schema,
                               rt.name AS referenced_table,
                               fk.delete_referential_action_desc,
                               fk.update_referential_action_desc,
                               fk.is_not_for_replication,
                               fk.is_not_trusted,
                               fk.is_disabled,
                               fk.is_system_named
                           FROM sys.foreign_keys fk
                           INNER JOIN sys.tables t ON t.object_id = fk.parent_object_id AND t.is_ms_shipped = 0
                           INNER JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
                           INNER JOIN sys.schemas rs ON rs.schema_id = rt.schema_id
                           ORDER BY fk.parent_object_id, fk.name;
                           """;

        var rows = new List<(int ObjectId, int FkObjectId, ForeignKeyModel Model)>();
        await using(var command = connection.CreateCommand())
        {
            command.CommandText = sql;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while(await reader.ReadAsync(cancellationToken))
            {
                rows.Add((reader.GetInt32(0), reader.GetInt32(1), new ForeignKeyModel
                {
                    Name = reader.GetString(2),
                    ReferencedSchema = reader.GetString(3),
                    ReferencedTable = reader.GetString(4),
                    DeleteActionDesc = reader.GetString(5),
                    UpdateActionDesc = reader.GetString(6),
                    IsNotForReplication = reader.GetBoolean(7),
                    IsNotTrusted = reader.GetBoolean(8),
                    IsDisabled = reader.GetBoolean(9),
                    IsSystemNamed = reader.GetBoolean(10)
                }));
            }
        }

        var columnsByFk = await GetForeignKeyColumnsAsync(connection, cancellationToken);
        foreach(var row in rows)
            row.Model.Columns = columnsByFk[row.FkObjectId].ToList();

        return rows.ToLookup(x => x.ObjectId, x => x.Model);
    }

    private static async Task<ILookup<int, ForeignKeyColumnModel>> GetForeignKeyColumnsAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               fkc.constraint_object_id,
                               pc.name AS parent_column,
                               rc.name AS referenced_column
                           FROM sys.foreign_key_columns fkc
                           INNER JOIN sys.columns pc
                               ON pc.object_id = fkc.parent_object_id
                              AND pc.column_id = fkc.parent_column_id
                           INNER JOIN sys.columns rc
                               ON rc.object_id = fkc.referenced_object_id
                              AND rc.column_id = fkc.referenced_column_id
                           ORDER BY fkc.constraint_object_id, fkc.constraint_column_id;
                           """;

        var rows = new List<(int FkObjectId, ForeignKeyColumnModel Model)>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            rows.Add((reader.GetInt32(0), new ForeignKeyColumnModel
            {
                ParentColumn = reader.GetString(1),
                ReferencedColumn = reader.GetString(2)
            }));
        }

        return rows.ToLookup(x => x.FkObjectId, x => x.Model);
    }

    private static async Task<ILookup<int, CheckConstraintModel>> GetCheckConstraintsAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               cc.parent_object_id,
                               cc.name,
                               cc.definition,
                               cc.is_not_trusted,
                               cc.is_disabled,
                               cc.is_system_named
                           FROM sys.check_constraints cc
                           INNER JOIN sys.tables t ON t.object_id = cc.parent_object_id AND t.is_ms_shipped = 0
                           ORDER BY cc.parent_object_id, cc.name;
                           """;

        var rows = new List<(int ObjectId, CheckConstraintModel Model)>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            rows.Add((reader.GetInt32(0), new CheckConstraintModel
            {
                Name = reader.GetString(1),
                Definition = reader.GetString(2),
                IsNotTrusted = reader.GetBoolean(3),
                IsDisabled = reader.GetBoolean(4),
                IsSystemNamed = reader.GetBoolean(5)
            }));
        }

        return rows.ToLookup(x => x.ObjectId, x => x.Model);
    }

    // --------------------------------------------------------------- indexes

    private async Task<ILookup<int, IndexModel>> GetIndexesAsync(
        SqlConnection connection,
        Dictionary<(int ObjectId, int IndexId), List<IndexColumnModel>> indexColumns,
        Dictionary<(int ObjectId, int IndexId), string> compression,
        CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               i.object_id,
                               i.index_id,
                               i.name,
                               i.is_unique,
                               i.type_desc,
                               i.filter_definition,
                               i.is_disabled,
                               i.type,
                               s.name AS schema_name,
                               t.name AS table_name,
                               i.fill_factor,
                               i.is_padded,
                               i.ignore_dup_key,
                               i.allow_row_locks,
                               i.allow_page_locks
                           FROM sys.indexes i
                           INNER JOIN sys.tables t ON t.object_id = i.object_id AND t.is_ms_shipped = 0
                           INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                           WHERE i.is_hypothetical = 0
                             AND i.name IS NOT NULL
                             AND i.is_primary_key = 0
                             AND i.is_unique_constraint = 0
                           ORDER BY i.object_id, i.name;
                           """;

        var rows = new List<(int ObjectId, IndexModel Model)>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            var objectId = reader.GetInt32(0);
            var indexId = reader.GetInt32(1);
            var name = reader.GetString(2);
            var indexType = reader.GetByte(7);

            // 1 = clustered rowstore, 2 = nonclustered rowstore, 5 = clustered
            // columnstore, 6 = nonclustered columnstore. XML, spatial and hash
            // indexes need syntax this renderer does not emit, so they are reported
            // rather than silently dropped from the snapshot.
            if(indexType is not (1 or 2 or 5 or 6))
            {
                Notices.Add($"skipped index [{name}] on [{reader.GetString(8)}].[{reader.GetString(9)}]: " +
                            $"{DescribeIndexKind(indexType, reader.GetString(4))} indexes are not scripted");
                continue;
            }

            rows.Add((objectId, new IndexModel
            {
                Name = name,
                IsUnique = reader.GetBoolean(3),
                TypeDesc = reader.GetString(4),
                FilterDefinition = reader.IsDBNull(5) ? null : reader.GetString(5),
                IsDisabled = reader.GetBoolean(6),
                // A clustered columnstore index covers the whole table implicitly and
                // is scripted with no column list. Capturing the list anyway would
                // make every added column read as a change to the index.
                Columns = indexType == 5 ? new List<IndexColumnModel>() : Lookup(indexColumns, objectId, indexId),
                FillFactor = reader.GetByte(10),
                IsPadded = reader.GetBoolean(11),
                IgnoreDupKey = reader.GetBoolean(12),
                AllowRowLocks = reader.GetBoolean(13),
                AllowPageLocks = reader.GetBoolean(14),
                DataCompression = LookupCompression(compression, objectId, indexId)
            }));
        }

        return rows.ToLookup(x => x.ObjectId, x => x.Model);
    }

    private static async Task<Dictionary<(int, int), List<IndexColumnModel>>> GetIndexColumnsAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               ic.object_id,
                               ic.index_id,
                               c.name,
                               ic.key_ordinal,
                               ic.is_descending_key,
                               ic.is_included_column,
                               ic.index_column_id
                           FROM sys.index_columns ic
                           INNER JOIN sys.tables t ON t.object_id = ic.object_id AND t.is_ms_shipped = 0
                           INNER JOIN sys.columns c
                               ON c.object_id = ic.object_id
                              AND c.column_id = ic.column_id
                           ORDER BY ic.object_id, ic.index_id, ic.is_included_column, ic.key_ordinal, ic.index_column_id;
                           """;

        var result = new Dictionary<(int, int), List<IndexColumnModel>>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            var key = (reader.GetInt32(0), reader.GetInt32(1));
            if(!result.TryGetValue(key, out var list))
            {
                list = new List<IndexColumnModel>();
                result[key] = list;
            }

            list.Add(new IndexColumnModel
            {
                Name = reader.GetString(2),
                KeyOrdinal = reader.GetByte(3),
                IsDescending = reader.GetBoolean(4),
                IsIncluded = reader.GetBoolean(5),
                IndexColumnId = reader.GetInt32(6)
            });
        }

        return result;
    }

    /// <summary>
    /// Compression per index, from partition 1. Partitioned tables can compress each
    /// partition differently; only the first is captured, because the renderer emits
    /// a single unqualified DATA_COMPRESSION and a partition scheme is out of scope.
    /// </summary>
    private static async Task<Dictionary<(int ObjectId, int IndexId), string>> GetDataCompressionAsync(
        SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               p.object_id,
                               p.index_id,
                               p.data_compression_desc
                           FROM sys.partitions p
                           INNER JOIN sys.tables t ON t.object_id = p.object_id AND t.is_ms_shipped = 0
                           WHERE p.partition_number = 1;
                           """;

        var result = new Dictionary<(int ObjectId, int IndexId), string>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            if(reader.IsDBNull(2))
                continue;

            result[(reader.GetInt32(0), reader.GetInt32(1))] = reader.GetString(2);
        }

        return result;
    }

    /// <summary>Names an index kind for a notice about one this renderer skips.</summary>
    private static string DescribeIndexKind(byte indexType, string typeDesc) => indexType switch
    {
        3 => "XML",
        4 => "spatial",
        7 => "memory-optimized hash",
        _ => typeDesc
    };

    // ----------------------------------------------------------- alias types

    private static async Task<List<AliasTypeModel>> GetAliasTypesAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               s.name AS schema_name,
                               ty.name,
                               bt.name AS base_type_name,
                               ty.max_length,
                               ty.precision,
                               ty.scale,
                               ty.is_nullable,
                               ty.collation_name
                           FROM sys.types ty
                           INNER JOIN sys.schemas s ON s.schema_id = ty.schema_id
                           INNER JOIN sys.types bt
                               ON bt.user_type_id = ty.system_type_id
                              AND bt.is_user_defined = 0
                           WHERE ty.is_user_defined = 1
                             AND ty.is_table_type = 0
                             AND ty.is_assembly_type = 0
                           ORDER BY s.name, ty.name;
                           """;

        var result = new List<AliasTypeModel>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            result.Add(new AliasTypeModel
            {
                Schema = reader.GetString(0),
                Name = reader.GetString(1),
                BaseTypeName = reader.GetString(2),
                MaxLength = reader.GetInt16(3),
                Precision = reader.GetByte(4),
                Scale = reader.GetByte(5),
                IsNullable = reader.GetBoolean(6),
                CollationName = reader.IsDBNull(7) ? null : reader.GetString(7)
            });
        }

        return result;
    }

    // -------------------------------------------------- programmable objects

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

    // --------------------------------------------------------------- helpers

    private static List<T> Take<T>(ILookup<int, T> lookup, int objectId) => lookup[objectId].ToList();

    private static List<IndexColumnModel> Lookup(
        Dictionary<(int ObjectId, int IndexId), List<IndexColumnModel>> source, int objectId, int indexId) =>
        source.TryGetValue((objectId, indexId), out var columns) ? columns : new List<IndexColumnModel>();

    /// <summary>
    /// Null when nothing was captured for that index, which the renderer and the
    /// differ both read as "uncompressed".
    /// </summary>
    private static string? LookupCompression(
        Dictionary<(int ObjectId, int IndexId), string> source, int objectId, int indexId) =>
        source.TryGetValue((objectId, indexId), out var value) ? value : null;

    private static string BuildKey(DbObjectType type, string schema, string name) => $"{type}:{schema}.{name}";

    private static async Task<object?> ExecuteScalarAsync(SqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync(cancellationToken);
    }

    private sealed record TableInfo(
        int ObjectId, string Schema, string Name,
        string? TemporalTypeDesc, string? HistorySchema, string? HistoryName,
        bool IsMemoryOptimized, string? DurabilityDesc);
}
