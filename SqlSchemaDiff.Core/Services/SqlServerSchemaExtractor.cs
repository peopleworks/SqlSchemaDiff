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
    /// <summary>
    /// Tables and table types have the same catalog shape: a table type's columns,
    /// keys and checks hang off <c>sys.table_types.type_table_object_id</c> exactly
    /// as a table's hang off its <c>object_id</c>. Feeding both through one derived
    /// table keeps every reader set-based and single-round-trip, and lets a table
    /// type reuse the table models rather than duplicating them.
    /// </summary>
    private const string ColumnOwners = """
                                        (
                                            SELECT object_id FROM sys.tables WHERE is_ms_shipped = 0
                                            UNION ALL
                                            SELECT type_table_object_id FROM sys.table_types
                                            WHERE is_user_defined = 1 AND is_assembly_type = 0
                                        )
                                        """;

    /// <summary>Objects skipped or partially captured during extraction, with the reason.</summary>
    public List<string> Notices { get; } = new();

    public async Task<DatabaseSnapshot> ExtractAsync(string connectionString, CancellationToken cancellationToken)
    {
        Notices.Clear();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var databaseName = (await ExecuteScalarAsync(connection, "SELECT DB_NAME();", cancellationToken))?.ToString() ?? "UNKNOWN";

        var tables = await GetTablesAsync(connection, cancellationToken);
        var columns = await GetColumnsAsync(connection, cancellationToken);
        var indexColumns = await GetIndexColumnsAsync(connection, cancellationToken);
        var keyConstraints = await GetKeyConstraintsAsync(connection, indexColumns, cancellationToken);
        var foreignKeys = await GetForeignKeysAsync(connection, cancellationToken);
        var checkConstraints = await GetCheckConstraintsAsync(connection, cancellationToken);
        var indexes = await GetIndexesAsync(connection, indexColumns, cancellationToken);
        var sequences = await GetSequencesAsync(connection, cancellationToken);
        var moduleDependencies = await GetModuleDependenciesAsync(connection, cancellationToken);

        var objects = new List<DbSchemaObject>();
        objects.AddRange(sequences.Select(BuildSequenceObject));
        objects.AddRange(await ExtractTableTypesAsync(connection, columns, keyConstraints, checkConstraints, cancellationToken));

        foreach(var table in tables)
        {
            var model = new TableModel
            {
                Schema = table.Schema,
                Name = table.Name,
                Columns = Take(columns, table.ObjectId),
                KeyConstraints = Take(keyConstraints, table.ObjectId),
                ForeignKeys = Take(foreignKeys, table.ObjectId),
                CheckConstraints = Take(checkConstraints, table.ObjectId),
                Indexes = Take(indexes, table.ObjectId)
            };

            // A column default of NEXT VALUE FOR ties the table to a sequence that
            // must already exist. That edge is nowhere in the catalog's relational
            // views — only inside the default's expression text — so it is read from
            // there, which also makes it survive a JSON round trip.
            var sequenceEdges = model.Columns
                .SelectMany(column => SequenceReferenceFinder.FindDependencyKeys(column.DefaultDefinition, sequences, table.Schema));

            var dependencies = model.ForeignKeys
                .Select(x => BuildKey(DbObjectType.Table, x.ReferencedSchema, x.ReferencedTable))
                .Concat(sequenceEdges)
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

        objects.AddRange(await ExtractProgrammableObjectsAsync(connection, moduleDependencies, cancellationToken));
        objects.AddRange(await ExtractTriggersAsync(connection, tables, moduleDependencies, cancellationToken));

        var allTypes = await GetAliasTypesAsync(connection, cancellationToken);
        // Both tables and table types can have alias-typed columns, and either one
        // makes the alias type a prerequisite of the script.
        var usedTypeKeys = objects
            .SelectMany(AliasTypedColumns)
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
            SchemaOwners = await GetSchemaOwnersAsync(connection, schemas, cancellationToken),
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
                               t.temporal_type
                           FROM sys.tables t
                           INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
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

            tables.Add(new TableInfo(reader.GetInt32(0), schema, name));
        }

        return tables;
    }

    private static async Task<ILookup<int, ColumnModel>> GetColumnsAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = $"""
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
                               CONVERT(varchar(100), ic.increment_value) AS increment_value_text
                           FROM sys.columns c
                           INNER JOIN {ColumnOwners} owner ON owner.object_id = c.object_id
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
                IdentityIncrement = reader.IsDBNull(19) ? null : reader.GetString(19)
            }));
        }

        return rows.ToLookup(x => x.ObjectId, x => x.Model);
    }

    // ----------------------------------------------------------- constraints

    private static async Task<ILookup<int, KeyConstraintModel>> GetKeyConstraintsAsync(
        SqlConnection connection,
        Dictionary<(int ObjectId, int IndexId), List<IndexColumnModel>> indexColumns,
        CancellationToken cancellationToken)
    {
        const string sql = $"""
                           SELECT
                               kc.parent_object_id,
                               kc.name,
                               kc.type,
                               kc.unique_index_id,
                               i.type_desc,
                               kc.is_system_named
                           FROM sys.key_constraints kc
                           INNER JOIN {ColumnOwners} owner ON owner.object_id = kc.parent_object_id
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
                Columns = Lookup(indexColumns, parentObjectId, indexId).Where(x => !x.IsIncluded).ToList()
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
        const string sql = $"""
                           SELECT
                               cc.parent_object_id,
                               cc.name,
                               cc.definition,
                               cc.is_not_trusted,
                               cc.is_disabled,
                               cc.is_system_named
                           FROM sys.check_constraints cc
                           INNER JOIN {ColumnOwners} owner ON owner.object_id = cc.parent_object_id
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
                               t.name AS table_name
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

            // 1 = clustered rowstore, 2 = nonclustered rowstore. Columnstore, XML,
            // spatial and hash indexes need syntax this renderer does not emit, so
            // they are reported rather than silently dropped from the snapshot.
            if(indexType is not (1 or 2))
            {
                Notices.Add($"skipped index [{name}] on [{reader.GetString(8)}].[{reader.GetString(9)}]: " +
                            $"unsupported index type {reader.GetString(4)}");
                continue;
            }

            rows.Add((objectId, new IndexModel
            {
                Name = name,
                IsUnique = reader.GetBoolean(3),
                TypeDesc = reader.GetString(4),
                FilterDefinition = reader.IsDBNull(5) ? null : reader.GetString(5),
                IsDisabled = reader.GetBoolean(6),
                Columns = Lookup(indexColumns, objectId, indexId)
            }));
        }

        return rows.ToLookup(x => x.ObjectId, x => x.Model);
    }

    private static async Task<Dictionary<(int, int), List<IndexColumnModel>>> GetIndexColumnsAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = $"""
                           SELECT
                               ic.object_id,
                               ic.index_id,
                               c.name,
                               ic.key_ordinal,
                               ic.is_descending_key,
                               ic.is_included_column,
                               ic.index_column_id
                           FROM sys.index_columns ic
                           INNER JOIN {ColumnOwners} owner ON owner.object_id = ic.object_id
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

    private static async Task<List<DbSchemaObject>> ExtractProgrammableObjectsAsync(
        SqlConnection connection,
        Dictionary<int, List<string>> dependencyMap,
        CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               o.object_id,
                               o.type,
                               s.name AS schema_name,
                               o.name,
                               m.definition,
                               m.uses_ansi_nulls,
                               m.uses_quoted_identifier
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
                Dependencies = dependencies,

                // SQL Server re-applies these when the module runs, so a module
                // created with one of them OFF has to be recreated the same way.
                UsesAnsiNulls = reader.IsDBNull(5) ? null : reader.GetBoolean(5),
                UsesQuotedIdentifier = reader.IsDBNull(6) ? null : reader.GetBoolean(6)
            });
        }

        return result;
    }

    /// <summary>
    /// What each module (view, procedure, function or trigger) references, as
    /// <see cref="DbSchemaObject.Key"/> values.
    /// </summary>
    private static async Task<Dictionary<int, List<string>>> GetModuleDependenciesAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string referencingModules = """
                                          SELECT object_id
                                          FROM sys.objects
                                          WHERE is_ms_shipped = 0
                                            AND type IN ('V', 'P', 'FN', 'IF', 'TF', 'FS', 'FT', 'TR')
                                          """;

        // referenced_class 1 (OBJECT_OR_COLUMN) is the only class whose referenced_id
        // is an object_id. 'SO' is a sequence: NEXT VALUE FOR inside a module body.
        const string objectSql = $"""
                                  SELECT
                                      sed.referencing_id,
                                      ro.type AS referenced_type,
                                      rs.name AS referenced_schema,
                                      ro.name AS referenced_name
                                  FROM sys.sql_expression_dependencies sed
                                  INNER JOIN sys.objects ro ON ro.object_id = sed.referenced_id
                                  INNER JOIN sys.schemas rs ON rs.schema_id = ro.schema_id
                                  WHERE sed.referenced_class = 1
                                    AND sed.referencing_class = 1
                                    AND sed.referenced_id IS NOT NULL
                                    AND sed.referencing_id IN (
                                  {referencingModules}
                                    )
                                    AND ro.is_ms_shipped = 0
                                    AND ro.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF', 'FS', 'FT', 'SO');
                                  """;

        // referenced_class 6 (TYPE) carries a user_type_id in referenced_id, not an
        // object_id — joining that to sys.objects would silently match an unrelated
        // row. It covers both a table-valued parameter and a DECLARE in the body.
        // Only table types are objects here; alias types travel as prerequisites.
        const string typeSql = $"""
                                SELECT
                                    sed.referencing_id,
                                    rs.name AS referenced_schema,
                                    tt.name AS referenced_name
                                FROM sys.sql_expression_dependencies sed
                                INNER JOIN sys.table_types tt ON tt.user_type_id = sed.referenced_id
                                INNER JOIN sys.schemas rs ON rs.schema_id = tt.schema_id
                                WHERE sed.referenced_class = 6
                                  AND sed.referencing_class = 1
                                  AND tt.is_user_defined = 1
                                  AND tt.is_assembly_type = 0
                                  AND sed.referencing_id IN (
                                {referencingModules}
                                  );
                                """;

        var map = new Dictionary<int, HashSet<string>>();

        await using(var command = connection.CreateCommand())
        {
            command.CommandText = objectSql;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while(await reader.ReadAsync(cancellationToken))
            {
                Add(map, reader.GetInt32(0),
                    BuildKey(ToDbObjectType(reader.GetString(1)), reader.GetString(2), reader.GetString(3)));
            }
        }

        await using(var command = connection.CreateCommand())
        {
            command.CommandText = typeSql;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while(await reader.ReadAsync(cancellationToken))
            {
                Add(map, reader.GetInt32(0),
                    BuildKey(DbObjectType.TableType, reader.GetString(1), reader.GetString(2)));
            }
        }

        return map.ToDictionary(
            kvp => kvp.Key,
            kvp => kvp.Value.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToList());

        static void Add(Dictionary<int, HashSet<string>> map, int referencingId, string dependencyKey)
        {
            if(!map.TryGetValue(referencingId, out var set))
            {
                set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                map[referencingId] = set;
            }

            set.Add(dependencyKey);
        }
    }

    internal static DbObjectType ToDbObjectType(string typeCode) => typeCode.Trim() switch
    {
        // sys.objects.type is char(2): single-char codes come back space-padded ("U ").
        "U" => DbObjectType.Table,
        "V" => DbObjectType.View,
        "P" => DbObjectType.StoredProcedure,
        "FN" or "IF" or "TF" or "FS" or "FT" => DbObjectType.Function,
        "TR" => DbObjectType.Trigger,
        "SO" => DbObjectType.Sequence,
        // The table behind a table type. Its sys.objects row carries a generated name
        // in the sys schema, so the extractor always names one from sys.table_types
        // instead; the mapping is here for completeness.
        "TT" => DbObjectType.TableType,
        _ => throw new InvalidOperationException($"Unsupported SQL object type code: '{typeCode.Trim()}'")
    };

    // ---------------------------------------------------------------- triggers

    /// <summary>
    /// DML triggers on user tables. Database-level DDL triggers and server-level or
    /// logon triggers are out of scope: they are not schema-bound objects and
    /// carrying them into another database is a policy decision, not a schema one.
    /// </summary>
    private async Task<List<DbSchemaObject>> ExtractTriggersAsync(
        SqlConnection connection,
        IReadOnlyCollection<TableInfo> tables,
        Dictionary<int, List<string>> dependencyMap,
        CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               tr.object_id,
                               tr.parent_id,
                               s.name AS schema_name,
                               tr.name,
                               ps.name AS parent_schema,
                               pt.name AS parent_name,
                               tr.is_disabled,
                               tr.is_instead_of_trigger,
                               m.definition
                           FROM sys.triggers tr
                           INNER JOIN sys.objects o ON o.object_id = tr.object_id
                           INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
                           INNER JOIN sys.tables pt ON pt.object_id = tr.parent_id
                           INNER JOIN sys.schemas ps ON ps.schema_id = pt.schema_id
                           LEFT JOIN sys.sql_modules m ON m.object_id = tr.object_id
                           WHERE tr.parent_class = 1
                             AND tr.is_ms_shipped = 0
                           ORDER BY ps.name, pt.name, tr.name;
                           """;

        var capturedTables = tables.Select(x => x.ObjectId).ToHashSet();

        var result = new List<DbSchemaObject>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            var objectId = reader.GetInt32(0);
            var parentId = reader.GetInt32(1);
            var schema = reader.GetString(2);
            var name = reader.GetString(3);
            var parentSchema = reader.GetString(4);
            var parentName = reader.GetString(5);

            // The parent table was skipped (a temporal history table, say), so the
            // trigger has nothing to hang off in this snapshot.
            if(!capturedTables.Contains(parentId))
                continue;

            // A CLR trigger has no sys.sql_modules row, and one created WITH
            // ENCRYPTION has a row with no text. Neither can be scripted.
            if(reader.IsDBNull(8))
            {
                Notices.Add($"skipped trigger [{schema}].[{name}] on [{parentSchema}].[{parentName}]: " +
                            "no readable definition (CLR or encrypted)");
                continue;
            }

            var model = new TriggerModel
            {
                ParentSchema = parentSchema,
                ParentName = parentName,
                IsDisabled = reader.GetBoolean(6),
                IsInsteadOf = reader.GetBoolean(7)
            };

            var dependencies = new[] { BuildKey(DbObjectType.Table, parentSchema, parentName) }
                .Concat(dependencyMap.TryGetValue(objectId, out var referenced) ? referenced : Enumerable.Empty<string>())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                .ToList();

            result.Add(new DbSchemaObject
            {
                Type = DbObjectType.Trigger,
                Schema = schema,
                Name = name,
                Definition = reader.GetString(8).Trim(),
                Dependencies = dependencies,
                Trigger = model
            });
        }

        return result;
    }

    // --------------------------------------------------------------- sequences

    private static async Task<List<SequenceModel>> GetSequencesAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        // start_value, increment, minimum_value, maximum_value and current_value are
        // sql_variant. They are converted to text in the server rather than boxed and
        // reinterpreted here: a decimal(38,0) sequence runs past both long and decimal.
        const string sql = """
                           SELECT
                               s.name AS schema_name,
                               q.name,
                               TYPE_NAME(q.user_type_id) AS type_name,
                               q.precision,
                               q.scale,
                               CONVERT(varchar(64), q.start_value) AS start_value,
                               CONVERT(varchar(64), q.increment) AS increment,
                               CONVERT(varchar(64), q.minimum_value) AS minimum_value,
                               CONVERT(varchar(64), q.maximum_value) AS maximum_value,
                               q.is_cycling,
                               q.is_cached,
                               q.cache_size,
                               CONVERT(varchar(64), q.current_value) AS current_value
                           FROM sys.sequences q
                           INNER JOIN sys.schemas s ON s.schema_id = q.schema_id
                           WHERE q.is_ms_shipped = 0
                           ORDER BY s.name, q.name;
                           """;

        var result = new List<SequenceModel>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            result.Add(new SequenceModel
            {
                Schema = reader.GetString(0),
                Name = reader.GetString(1),
                TypeName = reader.IsDBNull(2) ? "bigint" : reader.GetString(2),
                Precision = reader.GetByte(3),
                Scale = reader.GetByte(4),
                StartValue = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                Increment = reader.IsDBNull(6) ? string.Empty : reader.GetString(6),
                MinValue = reader.IsDBNull(7) ? null : reader.GetString(7),
                MaxValue = reader.IsDBNull(8) ? null : reader.GetString(8),
                IsCycling = reader.GetBoolean(9),
                IsCached = reader.GetBoolean(10),
                CacheSize = reader.IsDBNull(11) ? null : reader.GetInt32(11),
                CurrentValue = reader.IsDBNull(12) ? null : reader.GetString(12)
            });
        }

        return result;
    }

    private static DbSchemaObject BuildSequenceObject(SequenceModel sequence) => new()
    {
        Type = DbObjectType.Sequence,
        Schema = sequence.Schema,
        Name = sequence.Name,
        Definition = SqlRender.BuildSequenceCreate(sequence),
        Sequence = sequence
    };

    // ------------------------------------------------------------- table types

    private async Task<List<DbSchemaObject>> ExtractTableTypesAsync(
        SqlConnection connection,
        ILookup<int, ColumnModel> columns,
        ILookup<int, KeyConstraintModel> keyConstraints,
        ILookup<int, CheckConstraintModel> checkConstraints,
        CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT
                               tt.type_table_object_id,
                               s.name AS schema_name,
                               tt.name,
                               tt.is_memory_optimized
                           FROM sys.table_types tt
                           INNER JOIN sys.schemas s ON s.schema_id = tt.schema_id
                           WHERE tt.is_user_defined = 1
                             AND tt.is_assembly_type = 0
                           ORDER BY s.name, tt.name;
                           """;

        var withUnscriptedIndexes = await GetTableTypesWithInlineIndexesAsync(connection, cancellationToken);

        var result = new List<DbSchemaObject>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            var typeTableObjectId = reader.GetInt32(0);
            var schema = reader.GetString(1);
            var name = reader.GetString(2);

            if(withUnscriptedIndexes.Contains(typeTableObjectId))
            {
                Notices.Add($"[{schema}].[{name}]: inline indexes on a table type are not scripted; " +
                            "only PRIMARY KEY, UNIQUE and CHECK constraints are");
            }

            var model = new TableTypeModel
            {
                Schema = schema,
                Name = name,
                Columns = Take(columns, typeTableObjectId),
                KeyConstraints = Take(keyConstraints, typeTableObjectId),
                CheckConstraints = Take(checkConstraints, typeTableObjectId),
                IsMemoryOptimized = reader.GetBoolean(3)
            };

            result.Add(new DbSchemaObject
            {
                Type = DbObjectType.TableType,
                Schema = schema,
                Name = name,
                Definition = SqlRender.BuildTableTypeCreateScript(model),
                TableType = model
            });
        }

        return result;
    }

    /// <summary>
    /// Table types whose definition carries an <c>INDEX</c> clause that is not backing
    /// a key constraint. Those are not rendered, so they are reported instead.
    /// </summary>
    private static async Task<HashSet<int>> GetTableTypesWithInlineIndexesAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
                           SELECT DISTINCT i.object_id
                           FROM sys.indexes i
                           INNER JOIN sys.table_types tt ON tt.type_table_object_id = i.object_id
                           WHERE tt.is_user_defined = 1
                             AND tt.is_assembly_type = 0
                             AND i.index_id > 0
                             AND i.is_primary_key = 0
                             AND i.is_unique_constraint = 0;
                           """;

        var result = new HashSet<int>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
            result.Add(reader.GetInt32(0));

        return result;
    }

    // ----------------------------------------------------------- schema owners

    private static async Task<Dictionary<string, string>?> GetSchemaOwnersAsync(
        SqlConnection connection,
        IReadOnlyCollection<string> schemas,
        CancellationToken cancellationToken)
    {
        if(schemas.Count == 0)
            return null;

        // schema_id >= 16384 is the block reserved for the schemas that shadow the
        // fixed database roles; none of them is a user schema.
        const string sql = """
                           SELECT s.name, p.name AS owner_name
                           FROM sys.schemas s
                           INNER JOIN sys.database_principals p ON p.principal_id = s.principal_id
                           WHERE s.schema_id < 16384
                           ORDER BY s.name;
                           """;

        var wanted = new HashSet<string>(schemas, StringComparer.OrdinalIgnoreCase);
        var owners = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while(await reader.ReadAsync(cancellationToken))
        {
            var schema = reader.GetString(0);
            if(wanted.Contains(schema))
                owners[schema] = reader.GetString(1);
        }

        return owners.Count == 0 ? null : owners;
    }

    // --------------------------------------------------------------- helpers

    private static List<T> Take<T>(ILookup<int, T> lookup, int objectId) => lookup[objectId].ToList();

    /// <summary>The alias-typed columns of an object, whether it is a table or a table type.</summary>
    internal static IEnumerable<ColumnModel> AliasTypedColumns(DbSchemaObject schemaObject) =>
        (schemaObject.Table?.Columns ?? Enumerable.Empty<ColumnModel>())
        .Concat(schemaObject.TableType?.Columns ?? Enumerable.Empty<ColumnModel>())
        .Where(column => column.IsUserDefinedType);

    private static List<IndexColumnModel> Lookup(
        Dictionary<(int ObjectId, int IndexId), List<IndexColumnModel>> source, int objectId, int indexId) =>
        source.TryGetValue((objectId, indexId), out var columns) ? columns : new List<IndexColumnModel>();

    private static string BuildKey(DbObjectType type, string schema, string name) =>
        DbSchemaObject.BuildKey(type, schema, name);

    private static async Task<object?> ExecuteScalarAsync(SqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync(cancellationToken);
    }

    private sealed record TableInfo(int ObjectId, string Schema, string Name);
}
