using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;

namespace SqlSchemaDiff.IntegrationTests;

/// <summary>
/// WP 1.8b: one table, read on its own.
/// <para>
/// Two things decide this. The first is that the per-table read and the
/// whole-database read must produce the <b>same</b> model for the same table, field
/// by field — otherwise the cheap path is a second implementation that will drift.
/// The second is the history table: <c>ExtractAsync</c> refuses to model one, and it
/// is right to, but its rows are data no other object carries, so a restore needs
/// its exact shape and something has to hand it over.
/// </para>
/// <para>
/// What a history table carries is measured against <c>sys.columns</c> here, never
/// derived from the parent's shape, because the two differ in more ways than are
/// obvious.
/// </para>
/// </summary>
[Collection(SqlServerCollection.Name)]
public sealed class TableExtractLiveTests
{
    private static readonly JsonSerializerOptions Comparable = new() { WriteIndented = true };

    private readonly SqlServerFixture _sqlServer;

    public TableExtractLiveTests(SqlServerFixture sqlServer) => _sqlServer = sqlServer;

    // ------------------------------------------------- the two paths agree

    /// <summary>
    /// The purpose-built schema that exercises every extractor branch, table by
    /// table. Comparing serialized models rather than named properties is deliberate:
    /// a property added to <see cref="TableModel"/> tomorrow is covered by this test
    /// the day it is added, and a hand-written list of assertions would not be.
    /// </summary>
    [LiveFact]
    public async Task EveryTable_ReadsTheSameOnItsOwnAsInAFullExtract()
    {
        var connectionString = await _sqlServer.CreateDatabaseWithFullSchemaAsync();
        await AssertEveryTableAgreesAsync(connectionString);
    }

    /// <summary>The same, over the temporal, columnstore and compressed shapes of this work package.</summary>
    [LiveFact]
    public async Task EveryTable_ReadsTheSameOnItsOwn_ForTemporalAndCompressedShapes()
    {
        var connectionString = await CreateTableExtractDatabaseAsync();
        await AssertEveryTableAgreesAsync(connectionString);
    }

    /// <summary>
    /// And over a memory-optimized table, which is the one shape where the per-table
    /// read runs a different number of queries — it asks for bucket counts only when
    /// the table can have a hash index.
    /// </summary>
    [MemoryOptimizedFact]
    public async Task EveryTable_ReadsTheSameOnItsOwn_ForMemoryOptimizedTables()
    {
        var connectionString = _sqlServer.CreateDatabaseWithMemoryOptimizedFilegroup();
        await SqlServerFixture.ApplyAsync(connectionString, MemorySection(), useTransaction: false);

        var models = await AssertEveryTableAgreesAsync(connectionString);
        Assert.Contains(models, x => x.IsMemoryOptimized && x.KeyConstraints.Any(k => k.BucketCount > 0));
    }

    // --------------------------------------------------------- history tables

    /// <summary>
    /// The whole-database read still refuses the history table, with the same notice,
    /// and the per-table read still hands it over. Both halves matter: the first is
    /// the behaviour every existing test depends on, the second is why this work
    /// package exists.
    /// </summary>
    [LiveFact]
    public async Task HistoryTable_IsSkippedByTheFullExtractAndReturnedByName()
    {
        var connectionString = await CreateTableExtractDatabaseAsync();
        var (snapshot, notices) = await SqlServerFixture.ExtractWithNoticesAsync(connectionString);

        Assert.DoesNotContain(snapshot.Objects, x => string.Equals(x.Name, "PedidoHistoria", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(notices, x => x.Contains("[hist].[PedidoHistoria]") && x.Contains("history table"));

        var history = await ExtractTableAsync(connectionString, "hist", "PedidoHistoria");
        Assert.NotNull(history);
        Assert.Equal("hist", history!.Schema);
        Assert.Equal("PedidoHistoria", history.Name);
        Assert.Equal("HISTORY_TABLE", history.TemporalType);

        // It is a history table, so it has no history table of its own and no period:
        // the period lives on the parent, and nothing here would render one.
        Assert.Null(history.HistoryTableName);
        Assert.Null(history.PeriodStartColumn);
        Assert.Null(history.PeriodEndColumn);
    }

    /// <summary>
    /// Column for column, against the catalog itself. If the extractor and
    /// <c>sys.columns</c> disagree about a history table, the extractor is wrong.
    /// </summary>
    [LiveFact]
    public async Task HistoryTable_MatchesSysColumnsColumnForColumn()
    {
        var connectionString = await CreateTableExtractDatabaseAsync();
        var history = await ExtractTableAsync(connectionString, "hist", "PedidoHistoria");
        Assert.NotNull(history);

        var catalog = await ReadCatalogColumnsAsync(connectionString, "hist.PedidoHistoria");
        Assert.NotEmpty(catalog);
        Assert.Equal(catalog.Count, history!.Columns.Count);

        foreach(var (expected, actual) in catalog.Zip(history.Columns))
        {
            Assert.Equal(expected.Name, actual.Name);
            Assert.Equal(expected.TypeName, actual.TypeName);
            Assert.Equal(expected.IsUserDefinedType, actual.IsUserDefinedType);
            Assert.Equal(expected.MaxLength, actual.MaxLength);
            Assert.Equal(expected.Precision, actual.Precision);
            Assert.Equal(expected.Scale, actual.Scale);
            Assert.Equal(expected.IsNullable, actual.IsNullable);
            Assert.Equal(expected.IsIdentity, actual.IsIdentity);
            Assert.Equal(expected.IsComputed, actual.IsComputed);
            Assert.Equal(expected.IsRowGuid, actual.IsRowGuid);
            Assert.Equal(expected.IsSparse, actual.IsSparse);
            Assert.Equal(expected.GeneratedAlwaysType, actual.GeneratedAlwaysType);
            Assert.Equal(expected.IsHidden, actual.IsHidden);
            Assert.Equal(expected.CollationName, actual.CollationName);
            Assert.Equal(expected.HasDefault, actual.DefaultDefinition is not null);
        }
    }

    /// <summary>
    /// The five things SQL Server takes off a column on its way into the history
    /// table, and the five it leaves on. Every one of them is asserted on both sides
    /// — parent and history — so the test says what the difference <i>is</i> rather
    /// than only that the history side has some value.
    /// </summary>
    [LiveFact]
    public async Task HistoryTable_DropsWhatTheParentGeneratesAndKeepsTheRest()
    {
        var connectionString = await CreateTableExtractDatabaseAsync();

        var parent = await ExtractTableAsync(connectionString, "dbo", "Pedido");
        var history = await ExtractTableAsync(connectionString, "hist", "PedidoHistoria");
        Assert.NotNull(parent);
        Assert.NotNull(history);

        // Dropped: the identity is a plain column in history.
        Assert.True(parent!.Column("PedidoId").IsIdentity);
        Assert.False(history!.Column("PedidoId").IsIdentity);

        // Dropped: a computed column is materialised, and becomes nullable with it.
        // Its rows hold data that exists nowhere else, which is the whole argument.
        Assert.True(parent.Column("Grito").IsComputed);
        Assert.NotNull(parent.Column("Grito").ComputedDefinition);
        Assert.False(history.Column("Grito").IsComputed);
        Assert.Null(history.Column("Grito").ComputedDefinition);
        Assert.True(history.Column("Grito").IsNullable);

        // The same for a PERSISTED one: persistence is not what decides it.
        Assert.True(parent.Column("GritoFijo").IsComputed);
        Assert.True(parent.Column("GritoFijo").IsPersisted);
        Assert.False(history.Column("GritoFijo").IsComputed);
        Assert.False(history.Column("GritoFijo").IsPersisted);

        // Dropped: the GENERATED ALWAYS kind. In history they are plain datetime2.
        Assert.Equal(1, parent.Column("Desde").GeneratedAlwaysType);
        Assert.Equal(2, parent.Column("Hasta").GeneratedAlwaysType);
        Assert.Equal(0, history.Column("Desde").GeneratedAlwaysType);
        Assert.Equal(0, history.Column("Hasta").GeneratedAlwaysType);

        // Dropped, and not in the specification this was written from: HIDDEN goes
        // too. The history copies of the period columns are ordinary, visible ones.
        Assert.True(parent.Column("Desde").IsHidden);
        Assert.True(parent.Column("Hasta").IsHidden);
        Assert.False(history.Column("Desde").IsHidden);
        Assert.False(history.Column("Hasta").IsHidden);

        // Dropped: ROWGUIDCOL, and every default constraint.
        Assert.True(parent.Column("Rastro").IsRowGuid);
        Assert.False(history.Column("Rastro").IsRowGuid);
        Assert.NotNull(parent.Column("Total").DefaultDefinition);
        Assert.All(history.Columns, column => Assert.Null(column.DefaultDefinition));

        // Kept: SPARSE, the alias type, nvarchar(max), a non-default collation.
        Assert.True(parent.Column("Flojo").IsSparse);
        Assert.True(history.Column("Flojo").IsSparse);
        Assert.True(history.Column("Sku").IsUserDefinedType);
        Assert.Equal("Codigo", history.Column("Sku").TypeName);
        Assert.Equal(-1, history.Column("Nota").MaxLength);
        Assert.Equal("Latin1_General_CS_AS", history.Column("Etiqueta").CollationName);

        // Kept, and a trap for whoever restores one: rowversion stays rowversion, and
        // no INSERT can write a rowversion column. The bytes in history are real data
        // that only a binary(8) column could take back.
        Assert.Equal("timestamp", parent.Column("Version").TypeName);
        Assert.Equal("timestamp", history.Column("Version").TypeName);
    }

    /// <summary>
    /// A history table has no key, check or foreign key — SQL Server does not put any
    /// there — but it does have an index, and which one depends on who created the
    /// table. Both cases are in the fixture: the one SQL Server built for
    /// <c>dbo.Pedido</c>, and the columnstore one written by hand for <c>dbo.Evento</c>.
    /// </summary>
    [LiveFact]
    public async Task HistoryTable_CarriesItsOwnIndexesAndNoConstraints()
    {
        var connectionString = await CreateTableExtractDatabaseAsync();

        var generated = await ExtractTableAsync(connectionString, "hist", "PedidoHistoria");
        Assert.NotNull(generated);
        Assert.Empty(generated!.KeyConstraints);
        Assert.Empty(generated.CheckConstraints);
        Assert.Empty(generated.ForeignKeys);

        // Auto-named ix_<history table>, clustered, keyed on the period's *end* column
        // first. Nothing about that is guessable from the parent.
        var index = Assert.Single(generated.Indexes);
        Assert.Equal("ix_PedidoHistoria", index.Name);
        Assert.Equal("CLUSTERED", index.TypeDesc);
        Assert.False(index.IsUnique);
        Assert.Equal(new[] { "Hasta", "Desde" }, index.Columns.OrderBy(x => x.KeyOrdinal).Select(x => x.Name));

        var handBuilt = await ExtractTableAsync(connectionString, "hist", "EventoHistoria");
        Assert.NotNull(handBuilt);
        var columnstore = Assert.Single(handBuilt!.Indexes);
        Assert.Equal("CCI_EventoHistoria", columnstore.Name);
        Assert.Equal("CLUSTERED COLUMNSTORE", columnstore.TypeDesc);

        // A clustered columnstore covers every column implicitly and is scripted with
        // no column list, so the extractor deliberately captures none.
        Assert.Empty(columnstore.Columns);
    }

    // ------------------------------------------------------- names and misses

    [LiveFact]
    public async Task NamesThatAreNotUserTables_ComeBackAsNull()
    {
        var connectionString = await _sqlServer.CreateDatabaseWithFullSchemaAsync();

        // Nothing of that name at all.
        Assert.Null(await ExtractTableAsync(connectionString, "dbo", "NoSuchTable"));

        // The right name in the wrong schema.
        Assert.Null(await ExtractTableAsync(connectionString, "nosuchschema", "Customer"));

        // A view, and a table type: both are objects with columns, neither is a table.
        Assert.Null(await ExtractTableAsync(connectionString, "sales", "vInvoiceTotals"));
        Assert.Null(await ExtractTableAsync(connectionString, "sales", "InvoiceLineList"));

        // Identifiers are taken raw. Brackets are part of the name being looked for,
        // not quoting, and a two-part string is one name with a dot in it.
        Assert.Null(await ExtractTableAsync(connectionString, "[sales]", "Customer"));
        Assert.Null(await ExtractTableAsync(connectionString, "sales", "sales.Customer"));

        // A table in another database is out of reach: the current database is what
        // sys.tables means, and there is no cross-database form.
        Assert.Null(await ExtractTableAsync(connectionString, "dbo", "spt_values"));
    }

    // --------------------------------------------------- connection ownership

    /// <summary>
    /// The gap this closes: a caller reading rows inside its own transaction can now
    /// read the schema inside the same one. The proof is that it sees a change the
    /// transaction has made and not committed — no other connection can — and that
    /// leaving the transaction out is an error rather than a quiet read from outside.
    /// </summary>
    [LiveFact]
    public async Task PerTableExtract_ReadsInsideTheCallersTransaction()
    {
        var connectionString = await CreateTableExtractDatabaseAsync();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();

        await using(var command = connection.CreateCommand())
        {
            command.CommandText = "ALTER TABLE dbo.Plano ADD Anexo nvarchar(10) NULL;";
            command.Transaction = transaction;
            await command.ExecuteNonQueryAsync();
        }

        var inside = await new SqlServerSchemaExtractor()
            .ExtractTableAsync(connection, transaction, "dbo", "Plano", CancellationToken.None);

        Assert.NotNull(inside);
        Assert.Contains(inside!.Columns, x => x.Name == "Anexo");

        // Forgetting the transaction is not a silent read of the wrong instant.
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new SqlServerSchemaExtractor()
                .ExtractTableAsync(connection, null, "dbo", "Plano", CancellationToken.None));

        await transaction.RollbackAsync();
    }

    /// <summary>
    /// The whole-database read takes a caller's connection on the same terms, and
    /// gives the same answer as the connection-string form.
    /// </summary>
    [LiveFact]
    public async Task FullExtract_OverAnOpenConnection_MatchesTheConnectionStringForm()
    {
        var connectionString = await CreateTableExtractDatabaseAsync();
        var fromString = await SqlServerFixture.ExtractAsync(connectionString);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        var fromConnection = await new SqlServerSchemaExtractor()
            .ExtractAsync(connection, null, CancellationToken.None);

        Assert.Equal(fromString.DatabaseName, fromConnection.DatabaseName);
        Assert.Equal(Serialize(fromString.Objects), Serialize(fromConnection.Objects));
        Assert.Equal(Serialize(fromString.Types), Serialize(fromConnection.Types));
        Assert.Equal(fromString.Schemas, fromConnection.Schemas);

        // The connection belongs to the caller: it is still open, and still usable,
        // which is the whole point of handing one in.
        Assert.Equal(ConnectionState.Open, connection.State);
        var table = await new SqlServerSchemaExtractor()
            .ExtractTableAsync(connection, null, "dbo", "Pedido", CancellationToken.None);
        Assert.NotNull(table);
    }

    // ------------------------------------------------------------------ cost

    /// <summary>
    /// The reason <c>SwapPublisher</c> needs this at all: it runs a full extract per
    /// table it publishes, so a fifty-table restore reads the whole catalog fifty
    /// times. The claim being made is that reading one table costs one table's worth,
    /// and the server's own statistics are what settle it — rows selected, not
    /// wall-clock, so the number is the same on a slow machine.
    /// </summary>
    [LiveFact]
    public async Task PerTableExtract_CostsOneTablesWorthOfReads()
    {
        var connectionString = await CreateTableExtractDatabaseAsync();
        await FillWithTablesAsync(connectionString, 150);

        await using var connection = new SqlConnection(connectionString);
        connection.StatisticsEnabled = true;
        await connection.OpenAsync();

        connection.ResetStatistics();
        await new SqlServerSchemaExtractor().ExtractAsync(connection, null, CancellationToken.None);
        var full = Statistics(connection);

        connection.ResetStatistics();
        var one = await new SqlServerSchemaExtractor()
            .ExtractTableAsync(connection, null, "dbo", "Pedido", CancellationToken.None);
        var single = Statistics(connection);

        Assert.NotNull(one);

        var report = $"full: {full.Roundtrips} round trips, {full.Rows} rows; " +
                     $"one table: {single.Roundtrips} round trips, {single.Rows} rows";

        // Twenty times fewer rows on a database of this size, and the gap widens with
        // every table added — which is exactly the shape of the problem being fixed.
        Assert.True(single.Rows * 20 < full.Rows, $"the per-table read is not cheap enough. {report}");

        // It also must not have turned into a query per column or per index: the
        // number of round trips is fixed and small.
        Assert.True(single.Roundtrips <= full.Roundtrips, $"the per-table read makes more round trips than a full one. {report}");
        Assert.True(single.Roundtrips <= 16, $"the per-table read makes too many round trips. {report}");
    }

    // --------------------------------------------------------------- helpers

    private async Task<List<TableModel>> AssertEveryTableAgreesAsync(string connectionString)
    {
        var snapshot = await SqlServerFixture.ExtractAsync(connectionString);
        var tables = snapshot.Objects
            .Where(x => x.Type == DbObjectType.Table && x.Table is not null)
            .Select(x => x.Table!)
            .ToList();

        Assert.NotEmpty(tables);

        foreach(var expected in tables)
        {
            var actual = await ExtractTableAsync(connectionString, expected.Schema, expected.Name);
            Assert.True(actual is not null, $"[{expected.Schema}].[{expected.Name}] came back null from the per-table read.");
            Assert.Equal(Serialize(expected), Serialize(actual));
        }

        return tables;
    }

    private static async Task<TableModel?> ExtractTableAsync(string connectionString, string schema, string name) =>
        await new SqlServerSchemaExtractor().ExtractTableAsync(connectionString, schema, name, CancellationToken.None);

    private static string Serialize<T>(T value) => JsonSerializer.Serialize(value, Comparable);

    private async Task<string> CreateTableExtractDatabaseAsync()
    {
        var connectionString = _sqlServer.CreateDatabase();
        await SqlServerFixture.ApplyAsync(connectionString, Script("table-extract.sql"), useTransaction: false);
        return connectionString;
    }

    private static string Script(string fileName) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Schemas", fileName));

    /// <summary>The memory-optimized half of the shared special-tables script.</summary>
    private static string MemorySection()
    {
        var script = SqlServerFixture.SpecialTablesScript;
        var start = script.IndexOf("-- @@MEMORY@@", StringComparison.Ordinal);
        Assert.True(start >= 0, "the special-tables script no longer has a @@MEMORY@@ section.");
        return script[start..];
    }

    /// <summary>
    /// Enough plain tables to make "the whole catalog" cost something. They are
    /// created in one batch: a hundred and fifty round trips would take longer than
    /// the measurement they are for.
    /// </summary>
    private static async Task FillWithTablesAsync(string connectionString, int count)
    {
        var sql = string.Join(
            Environment.NewLine,
            Enumerable.Range(1, count).Select(i =>
                $"CREATE TABLE dbo.Relleno{i} (Id int NOT NULL CONSTRAINT PK_Relleno{i} PRIMARY KEY, " +
                $"Texto nvarchar(50) NULL CONSTRAINT DF_Relleno{i} DEFAULT (N''), Cuando datetime2(7) NULL);"));

        await SqlServerFixture.ApplyAsync(connectionString, sql, useTransaction: false);
    }

    private static (long Roundtrips, long Rows) Statistics(SqlConnection connection)
    {
        var statistics = connection.RetrieveStatistics();
        return ((long)statistics["ServerRoundtrips"]!, (long)statistics["SelectRows"]!);
    }

    private static async Task<List<CatalogColumn>> ReadCatalogColumnsAsync(string connectionString, string table)
    {
        const string sql = """
                           SELECT
                               c.name,
                               ty.name AS type_name,
                               ty.is_user_defined,
                               c.max_length,
                               c.precision,
                               c.scale,
                               c.is_nullable,
                               c.is_identity,
                               c.is_computed,
                               c.is_rowguidcol,
                               c.is_sparse,
                               c.generated_always_type,
                               c.is_hidden,
                               c.collation_name,
                               CASE WHEN c.default_object_id = 0 THEN CAST(0 AS bit) ELSE CAST(1 AS bit) END AS has_default
                           FROM sys.columns c
                           INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                           WHERE c.object_id = OBJECT_ID(@table)
                           ORDER BY c.column_id;
                           """;

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add("@table", SqlDbType.NVarChar, 260).Value = table;

        var result = new List<CatalogColumn>();
        await using var reader = await command.ExecuteReaderAsync();
        while(await reader.ReadAsync())
        {
            result.Add(new CatalogColumn(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetBoolean(2),
                reader.GetInt16(3),
                reader.GetByte(4),
                reader.GetByte(5),
                reader.GetBoolean(6),
                reader.GetBoolean(7),
                reader.GetBoolean(8),
                reader.GetBoolean(9),
                reader.GetBoolean(10),
                reader.GetByte(11),
                reader.GetBoolean(12),
                reader.IsDBNull(13) ? null : reader.GetString(13),
                reader.GetBoolean(14)));
        }

        return result;
    }

    private sealed record CatalogColumn(
        string Name, string TypeName, bool IsUserDefinedType, short MaxLength, byte Precision, byte Scale,
        bool IsNullable, bool IsIdentity, bool IsComputed, bool IsRowGuid, bool IsSparse,
        byte GeneratedAlwaysType, bool IsHidden, string? CollationName, bool HasDefault);
}
