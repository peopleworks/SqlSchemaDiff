using Microsoft.Data.SqlClient;
using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;

namespace SqlSchemaDiff.IntegrationTests;

/// <summary>
/// WP 1.8a, against a real server. The property under test is not a string in a
/// script: it is that after a restore, <c>FOR SYSTEM_TIME AS OF</c> answers for
/// every instant the source could answer for — including the stretch between the
/// end of the archived history and the moment of the restore, which is exactly
/// where a restored temporal table used to have a hole.
/// <para>
/// Every assertion about time is put to the server. The test never computes what a
/// row's <c>ValidFrom</c> ought to be; it asks, and compares source with target.
/// </para>
/// </summary>
[Collection(SqlServerCollection.Name)]
public sealed class PeriodAfterDataLiveTests
{
    /// <summary>An instant after the archived history ends and before any restore can run.</summary>
    private const string InTheGap = "2023-06-01T00:00:00";

    /// <summary>An instant inside the archived history.</summary>
    private const string InTheHistory = "2021-06-01T00:00:00";

    private static readonly Lazy<string> Fixture = new(() =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Schemas", "period-after-data.sql")));

    private readonly SqlServerFixture _sqlServer;
    private readonly SchemaDiffer _differ = new();

    public PeriodAfterDataLiveTests(SqlServerFixture sqlServer) => _sqlServer = sqlServer;

    /// <summary>
    /// The acceptance test. Compose the restore shape with the period deferred, load
    /// the rows with the periods they had, run finalize, and ask the server the
    /// question that used to return nothing.
    /// </summary>
    [LiveFact]
    public async Task PeriodAfterData_RestoresTheRowsOwnValidFromAndAnswersAsOfAcrossTheGap()
    {
        var sourceConnection = await CreateSourceAsync();
        var targetConnection = _sqlServer.CreateDatabase();

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var phases = ScriptComposer.ComposePhases(
            source, new ComposeOptions { ConstraintsAfterData = true, PeriodAfterData = true });

        // The tables phase has to be loadable: no period, so no GENERATED ALWAYS, so
        // no refused INSERT — and the scale of each period column preserved, because
        // the maximum value ADD PERIOD demands is the maximum for that scale.
        var tables = PhaseSql(phases, "tables");
        Assert.DoesNotContain("PERIOD FOR SYSTEM_TIME", tables);
        Assert.DoesNotContain("GENERATED ALWAYS", tables);
        Assert.DoesNotContain("HIDDEN", tables);
        Assert.Contains("[Desde] datetime2(7) NOT NULL", tables);
        Assert.Contains("[Desde] datetime2(3) NOT NULL", tables);

        AssertOrder(PhaseSql(phases, "finalize"),
            "ALTER TABLE [dbo].[Contrato] ADD PERIOD FOR SYSTEM_TIME ([Desde], [Hasta]);",
            "ALTER TABLE [dbo].[Contrato] ALTER COLUMN [Desde] ADD HIDDEN;",
            "ALTER TABLE [dbo].[Contrato] SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [archivo].[ContratoHistoria]));");

        await RunPhasesAsync(phases, targetConnection, LoadRowsAsync);

        // The row kept the ValidFrom it had on the source, not the instant of the
        // restore — and the server, not this test, says so.
        Assert.Equal(
            await ScalarStringAsync(sourceConnection, "SELECT CONVERT(varchar(30), Desde, 121) FROM dbo.Contrato;"),
            await ScalarStringAsync(targetConnection, "SELECT CONVERT(varchar(30), Desde, 121) FROM dbo.Contrato;"));

        // The hole. Before 1.8 this returned nothing at all.
        Assert.Equal("tercero", await ScalarStringAsync(targetConnection,
            $"SELECT Titular FROM dbo.Contrato FOR SYSTEM_TIME AS OF '{InTheGap}';"));
        Assert.Equal("primero", await ScalarStringAsync(targetConnection,
            $"SELECT Titular FROM dbo.Contrato FOR SYSTEM_TIME AS OF '{InTheHistory}';"));

        // The same for the scale-3 table, whose maximum end value is .999.
        Assert.Equal("vida", await ScalarStringAsync(targetConnection,
            $"SELECT Ramo FROM dbo.Poliza FOR SYSTEM_TIME AS OF '{InTheGap}';"));
        Assert.Equal("salud", await ScalarStringAsync(targetConnection,
            $"SELECT Ramo FROM dbo.Poliza FOR SYSTEM_TIME AS OF '{InTheHistory}';"));

        // HIDDEN is a property of the column: the table that had it gets it back, the
        // table that did not must not acquire it.
        Assert.Equal(1, await HiddenAsync(targetConnection, "dbo.Contrato", "Desde"));
        Assert.Equal(1, await HiddenAsync(targetConnection, "dbo.Contrato", "Hasta"));
        Assert.Equal(0, await HiddenAsync(targetConnection, "dbo.Poliza", "Desde"));

        var target = await SqlServerFixture.ExtractAsync(targetConnection);
        Snapshots.AssertNoChanges("period-after-data source -> target", Compare(source, target));
        Snapshots.AssertNoChanges("period-after-data target -> source", Compare(target, source));
    }

    /// <summary>
    /// The defect, measured. With the period inline — every release up to 1.7 — the
    /// restore cannot name the period columns at all, and the rows it does load are
    /// stamped with the instant of the restore, so the gap answers nothing.
    /// </summary>
    [LiveFact]
    public async Task WithoutPeriodAfterData_TheGapAnswersNothing()
    {
        var sourceConnection = await CreateSourceAsync();
        var targetConnection = _sqlServer.CreateDatabase();

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var phases = ScriptComposer.ComposePhases(source, new ComposeOptions { ConstraintsAfterData = true });

        // Unchanged from 1.7: the period is still inside CREATE TABLE.
        Assert.Contains("PERIOD FOR SYSTEM_TIME ([Desde], [Hasta])", PhaseSql(phases, "tables"));
        Assert.DoesNotContain("ADD PERIOD FOR SYSTEM_TIME", PhaseSql(phases, "finalize"));

        await RunPhasesAsync(phases, targetConnection, async connection =>
        {
            await SqlServerFixture.ApplyAsync(connection, Section("HISTORY", "MEMSOURCE"), useTransaction: false);

            // Naming the period columns is refused outright.
            var refused = await Assert.ThrowsAsync<SqlException>(() => SqlServerFixture.ApplyAsync(connection,
                "INSERT INTO dbo.Contrato (ContratoId, Titular, Importe, Desde, Hasta) " +
                "VALUES (1, N'tercero', 300, '2023-01-01T00:00:00.0000000', '9999-12-31T23:59:59.9999999');",
                useTransaction: false));
            Assert.Equal(13536, refused.Number);

            // So the restore has to leave them out, and SQL Server stamps them itself.
            await SqlServerFixture.ApplyAsync(connection,
                "INSERT INTO dbo.Contrato (ContratoId, Titular, Importe) VALUES (1, N'tercero', 300);" +
                "INSERT INTO dbo.Poliza (PolizaId, Ramo) VALUES (1, N'vida');" +
                "INSERT INTO archivo.ContratoHistoria (ContratoId, Titular, Importe, Desde, Hasta) " +
                "VALUES (1, N'primero', 100, '2021-01-01T00:00:00.0000000', '2023-01-01T00:00:00.0000000');",
                useTransaction: false);
        });

        Assert.Equal(0, await SqlServerFixture.ScalarAsync<int>(targetConnection,
            $"SELECT COUNT(*) FROM dbo.Contrato FOR SYSTEM_TIME AS OF '{InTheGap}';"));

        // Not because the row is missing — because its ValidFrom is the restore.
        Assert.Equal(1, await SqlServerFixture.ScalarAsync<int>(targetConnection,
            "SELECT COUNT(*) FROM dbo.Contrato;"));
        Assert.NotEqual(
            await ScalarStringAsync(sourceConnection, "SELECT CONVERT(varchar(30), Desde, 121) FROM dbo.Contrato;"),
            await ScalarStringAsync(targetConnection, "SELECT CONVERT(varchar(30), Desde, 121) FROM dbo.Contrato;"));
    }

    /// <summary>
    /// The combination the specification expected to fail. Measured on SQL Server
    /// 2025 it does not: a memory-optimized temporal table takes the deferred period
    /// exactly like a disk-based one, so the composer keeps no special case for it.
    /// </summary>
    [MemoryOptimizedFact]
    public async Task AMemoryOptimizedTemporalTable_TakesTheDeferredPeriodToo()
    {
        var sourceConnection = _sqlServer.CreateDatabaseWithMemoryOptimizedFilegroup();
        await SqlServerFixture.ApplyAsync(sourceConnection, Section("MEMSOURCE", "MEMHISTORY"), useTransaction: false);

        var targetConnection = _sqlServer.CreateDatabaseWithMemoryOptimizedFilegroup();

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var phases = ScriptComposer.ComposePhases(
            source, new ComposeOptions { ConstraintsAfterData = true, PeriodAfterData = true });

        Assert.DoesNotContain("PERIOD FOR SYSTEM_TIME", PhaseSql(phases, "tables"));
        Assert.Contains("MEMORY_OPTIMIZED = ON", PhaseSql(phases, "tables"));

        await RunPhasesAsync(phases, targetConnection, async connection =>
        {
            await SqlServerFixture.ApplyAsync(connection, Section("MEMHISTORY", null), useTransaction: false);
            await SqlServerFixture.ApplyAsync(connection,
                "INSERT INTO dbo.MemContrato (ContratoId, Titular, Desde, Hasta) " +
                "VALUES (1, N'tercero', '2023-01-01T00:00:00.0000000', '9999-12-31T23:59:59.9999999');" +
                "INSERT INTO dbo.MemContratoHistoria (ContratoId, Titular, Desde, Hasta) " +
                "VALUES (1, N'primero', '2021-01-01T00:00:00.0000000', '2023-01-01T00:00:00.0000000');",
                useTransaction: false);
        });

        Assert.Equal("tercero", await ScalarStringAsync(targetConnection,
            $"SELECT Titular FROM dbo.MemContrato FOR SYSTEM_TIME AS OF '{InTheGap}';"));
        Assert.Equal("primero", await ScalarStringAsync(targetConnection,
            $"SELECT Titular FROM dbo.MemContrato FOR SYSTEM_TIME AS OF '{InTheHistory}';"));
        Assert.Equal(
            await ScalarStringAsync(sourceConnection, "SELECT CONVERT(varchar(30), Desde, 121) FROM dbo.MemContrato;"),
            await ScalarStringAsync(targetConnection, "SELECT CONVERT(varchar(30), Desde, 121) FROM dbo.MemContrato;"));
    }

    // ------------------------------------------------------------------ helpers

    /// <summary>
    /// The rows a restore loads, and the history tables it has to create first. A
    /// history table is never in the snapshot — SQL Server owns it, so the extractor
    /// skips it and only the schema it lives in survives — which means the driver,
    /// not the composed script, is what puts the archived rows back.
    /// </summary>
    private static async Task LoadRowsAsync(string connectionString)
    {
        await SqlServerFixture.ApplyAsync(connectionString, Section("HISTORY", "MEMSOURCE"), useTransaction: false);

        await SqlServerFixture.ApplyAsync(connectionString,
            "INSERT INTO dbo.Contrato (ContratoId, Titular, Importe, Desde, Hasta) " +
            "VALUES (1, N'tercero', 300, '2023-01-01T00:00:00.0000000', '9999-12-31T23:59:59.9999999');" +
            "INSERT INTO archivo.ContratoHistoria (ContratoId, Titular, Importe, Desde, Hasta) VALUES " +
            "(1, N'primero', 100, '2021-01-01T00:00:00.0000000', '2022-01-01T00:00:00.0000000')," +
            "(1, N'segundo', 200, '2022-01-01T00:00:00.0000000', '2023-01-01T00:00:00.0000000');" +
            "INSERT INTO dbo.Poliza (PolizaId, Ramo, Desde, Hasta) " +
            "VALUES (1, N'vida', '2023-01-01T00:00:00.000', '9999-12-31T23:59:59.999');" +
            "INSERT INTO archivo.PolizaHistoria (PolizaId, Ramo, Desde, Hasta) " +
            "VALUES (1, N'salud', '2021-01-01T00:00:00.000', '2023-01-01T00:00:00.000');",
            useTransaction: false);
    }

    /// <summary>
    /// Runs the phases in order and loads the rows where a restore would: after the
    /// tables phase, before anything that has to see them.
    /// </summary>
    private static async Task RunPhasesAsync(
        IReadOnlyList<ScriptPhase> phases, string connectionString, Func<string, Task> loadRows)
    {
        foreach(var phase in phases)
        {
            var sql = PhaseSql(phases, phase.Name);
            if(!string.IsNullOrWhiteSpace(sql))
                await SqlServerFixture.ApplyAsync(connectionString, sql, useTransaction: false);

            if(phase.Name == "tables")
                await loadRows(connectionString);
        }
    }

    private async Task<string> CreateSourceAsync()
    {
        var connectionString = _sqlServer.CreateDatabase();
        await SqlServerFixture.ApplyAsync(connectionString, Section("SOURCE", "HISTORY"), useTransaction: false);
        return connectionString;
    }

    private static async Task<string?> ScalarStringAsync(string connectionString, string sql) =>
        await SqlServerFixture.ScalarAsync<string>(connectionString, sql);

    private static async Task<int> HiddenAsync(string connectionString, string table, string column) =>
        await SqlServerFixture.ScalarAsync<int>(connectionString,
            $"SELECT CONVERT(int, is_hidden) FROM sys.columns " +
            $"WHERE object_id = OBJECT_ID('{table}') AND name = '{column}';");

    /// <summary>Cuts one <c>@@MARKER@@</c> block out of this work package's own fixture script.</summary>
    private static string Section(string from, string? to)
    {
        var script = Fixture.Value;
        var start = script.IndexOf($"-- @@{from}@@", StringComparison.Ordinal);
        Assert.True(start >= 0, $"marker @@{from}@@ is missing from period-after-data.sql");

        if(to is null)
            return script[start..];

        var end = script.IndexOf($"-- @@{to}@@", start, StringComparison.Ordinal);
        Assert.True(end > start, $"marker @@{to}@@ is missing from period-after-data.sql");
        return script[start..end];
    }

    private static string PhaseSql(IReadOnlyList<ScriptPhase> phases, string name) =>
        string.Join(
            Environment.NewLine + "GO" + Environment.NewLine,
            phases.Single(x => x.Name == name).Batches.Select(x => x.Sql));

    private static void AssertOrder(string script, params string[] fragments)
    {
        var previous = -1;
        foreach(var fragment in fragments)
        {
            var index = script.IndexOf(fragment, StringComparison.Ordinal);
            Assert.True(index > previous,
                $"'{fragment}' is missing or out of order in:{Environment.NewLine}{script}");
            previous = index;
        }
    }

    private DiffResult Compare(DatabaseSnapshot source, DatabaseSnapshot target) =>
        _differ.Diff(source, target, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: false, addOnly: false);
}
