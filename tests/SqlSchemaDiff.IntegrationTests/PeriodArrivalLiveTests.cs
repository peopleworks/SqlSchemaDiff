using SqlSchemaDiff.Services;

namespace SqlSchemaDiff.IntegrationTests;

/// <summary>
/// A table that gains a <c>SYSTEM_TIME</c> period, diffed and then actually run.
/// <para>
/// Until 1.8 the differ emitted the two period columns as separate
/// <c>ALTER TABLE ... ADD</c> statements followed by <c>ADD PERIOD</c>, and that
/// script could not run at all: SQL Server refuses a <c>GENERATED ALWAYS</c> column
/// while no period is defined (13509), so the period then names columns that do not
/// exist (4924) and the versioning switch fails behind it (13510). It survived 1.7
/// because every test of this path read the script and none of them ran it.
/// </para>
/// <para>
/// So the assertion here is not on the text. The script is applied to a database with
/// rows in it, and the questions asked afterwards are asked of the server.
/// </para>
/// </summary>
[Collection(SqlServerCollection.Name)]
public sealed class PeriodArrivalLiveTests
{
    private const string Before = """
        CREATE TABLE dbo.Contrato (
            Id     int           NOT NULL CONSTRAINT PK_Contrato PRIMARY KEY,
            Nombre nvarchar(40)  NOT NULL
        );
        INSERT INTO dbo.Contrato (Id, Nombre) VALUES (1, N'uno'), (2, N'dos'), (3, N'tres');
        """;

    /// <summary>
    /// The same table, versioned, with its period columns at a scale that is not the
    /// default: the maximum value <c>ADD PERIOD</c> demands is the maximum for the
    /// column's own scale, and a literal hard-coded at seven digits is refused on a
    /// <c>datetime2(3)</c> column with 13575.
    /// </summary>
    private const string After = """
        CREATE TABLE dbo.Contrato (
            Id     int           NOT NULL CONSTRAINT PK_Contrato PRIMARY KEY,
            Nombre nvarchar(40)  NOT NULL,
            Desde  datetime2(3)  GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
            Hasta  datetime2(3)  GENERATED ALWAYS AS ROW END   HIDDEN NOT NULL,
            PERIOD FOR SYSTEM_TIME (Desde, Hasta)
        ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.ContratoHist));
        """;

    private readonly SqlServerFixture _sqlServer;
    private readonly SchemaDiffer _differ = new();

    public PeriodArrivalLiveTests(SqlServerFixture sqlServer) => _sqlServer = sqlServer;

    /// <summary>
    /// The script the differ writes for a table gaining a period runs, keeps the rows
    /// that were already there, leaves the table versioned, and leaves nothing behind
    /// that the source does not have.
    /// </summary>
    [LiveFact]
    public async Task ATableGainingAPeriodGetsAScriptThatRuns()
    {
        var target = _sqlServer.CreateDatabase();
        var source = _sqlServer.CreateDatabase();

        await SqlServerFixture.ApplyAsync(target, Before);
        await SqlServerFixture.ApplyAsync(source, After);

        var script = _differ.Diff(
            await SqlServerFixture.ExtractAsync(source),
            await SqlServerFixture.ExtractAsync(target),
            includeDrops: false, includeTableDrops: false,
            allowTableRebuild: false, addOnly: false).Script;

        // Not in a transaction: ADD PERIOD and SET SYSTEM_VERSIONING are their own
        // batches, and wrapping them tells us less about what an operator will hit.
        var applied = await SqlServerFixture.ApplyAsync(target, script, useTransaction: false);

        // Every batch, not merely "it did not throw": a script that stopped half way
        // through is the failure this test exists to catch.
        Assert.Equal(applied.BatchCount, applied.Executed);
        Assert.False(applied.RolledBack);

        // The rows are the point. A script that reached the end by dropping the table
        // and building it again would pass every other assertion here.
        Assert.Equal(3, await SqlServerFixture.ScalarAsync<int>(target, "SELECT COUNT(*) FROM dbo.Contrato;"));

        Assert.Equal(
            "SYSTEM_VERSIONED_TEMPORAL_TABLE",
            await SqlServerFixture.ScalarAsync<string>(
                target, "SELECT temporal_type_desc FROM sys.tables WHERE object_id = OBJECT_ID('dbo.Contrato');"));

        // The two defaults exist only so NOT NULL columns can be added to a table that
        // already has rows. The source has none, so leaving them would be drift.
        Assert.Equal(0, await SqlServerFixture.ScalarAsync<int>(
            target, "SELECT COUNT(*) FROM sys.default_constraints WHERE parent_object_id = OBJECT_ID('dbo.Contrato');"));

        // HIDDEN survives, and so does the scale.
        Assert.Equal(2, await SqlServerFixture.ScalarAsync<int>(
            target,
            "SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('dbo.Contrato') AND is_hidden = 1 AND scale = 3;"));
    }

    /// <summary>
    /// And the run leaves nothing for a second run to do. An idempotence check is what
    /// catches scaffolding a script forgot to clear up: the temporary defaults showed
    /// up here as a second diff proposing to drop them.
    /// </summary>
    [LiveFact]
    public async Task AndThenTheTwoDatabasesAgree()
    {
        var target = _sqlServer.CreateDatabase();
        var source = _sqlServer.CreateDatabase();

        await SqlServerFixture.ApplyAsync(target, Before);
        await SqlServerFixture.ApplyAsync(source, After);

        var first = _differ.Diff(
            await SqlServerFixture.ExtractAsync(source),
            await SqlServerFixture.ExtractAsync(target),
            includeDrops: false, includeTableDrops: false,
            allowTableRebuild: false, addOnly: false);

        Assert.True(first.HasChanges);
        await SqlServerFixture.ApplyAsync(target, first.Script, useTransaction: false);

        var second = _differ.Diff(
            await SqlServerFixture.ExtractAsync(source),
            await SqlServerFixture.ExtractAsync(target),
            includeDrops: false, includeTableDrops: false,
            allowTableRebuild: false, addOnly: false);

        Assert.False(second.HasChanges, second.Script);
    }
}
