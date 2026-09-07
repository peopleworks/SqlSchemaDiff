using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;

namespace SqlSchemaDiff.IntegrationTests;

/// <summary>
/// A fact that needs a server willing to host a memory-optimized table — In-Memory
/// OLTP on the edition, and a data path the service can create a container in. The
/// answer is probed once per run against the real server, so a laptop or a CI image
/// without it skips with the reason rather than failing.
/// </summary>
public sealed class MemoryOptimizedFactAttribute : FactAttribute
{
    public MemoryOptimizedFactAttribute()
    {
        if(SqlServerFixture.MemoryOptimizedSkipReason is { } reason)
            Skip = reason;
    }
}

/// <summary>
/// The property WP 1.7b exists for: a database with a system-versioned or a
/// memory-optimized table has to round-trip to an empty diff. Before 1.7 both came
/// back as plain tables — the history was gone and the guarantee with it — and the
/// only trace was a notice.
/// </summary>
[Collection(SqlServerCollection.Name)]
public sealed class SpecialTableLiveTests
{
    private readonly SqlServerFixture _sqlServer;
    private readonly SchemaDiffer _differ = new();

    public SpecialTableLiveTests(SqlServerFixture sqlServer) => _sqlServer = sqlServer;

    // ------------------------------------------------------------------ temporal

    [LiveFact]
    public async Task SystemVersionedTable_IsCapturedWithEverythingItNeeds()
    {
        var source = await CreateTemporalDatabaseAsync();
        var (snapshot, notices) = await SqlServerFixture.ExtractWithNoticesAsync(source);

        var employee = snapshot.Table("dbo", "Employee");
        Assert.Equal("SYSTEM_VERSIONED_TEMPORAL_TABLE", employee.TemporalType);
        Assert.Equal("history", employee.HistoryTableSchema);
        Assert.Equal("EmployeeEntryHistory", employee.HistoryTableName);
        Assert.Equal("ValidFrom", employee.PeriodStartColumn);
        Assert.Equal("ValidTo", employee.PeriodEndColumn);
        Assert.Equal(1, employee.Column("ValidFrom").GeneratedAlwaysType);
        Assert.Equal(2, employee.Column("ValidTo").GeneratedAlwaysType);
        Assert.True(employee.Column("ValidFrom").IsHidden);
        Assert.True(employee.Column("ValidTo").IsHidden);
        Assert.False(employee.Column("FullName").IsHidden);

        // SQL Server owns the history table, so it is not in the snapshot — but the
        // schema it lives in is, or the SYSTEM_VERSIONING clause has nowhere to put it.
        Assert.DoesNotContain(snapshot.Objects, x => x.Name == "EmployeeEntryHistory");
        Assert.Contains("history", snapshot.Schemas);

        // A period without system versioning is a different shape and has to survive too.
        var assignment = snapshot.Table("dbo", "Assignment");
        Assert.Null(assignment.TemporalType);
        Assert.Equal("StartedAt", assignment.PeriodStartColumn);
        Assert.False(assignment.Column("StartedAt").IsHidden);

        // The notice list stops crying wolf: the only thing left to say about a
        // system-versioned table is that its history table was not scripted.
        var joined = string.Join(Environment.NewLine, notices);
        Assert.DoesNotContain("the SYSTEM_VERSIONING clause is not scripted", joined);
        Assert.Contains("EmployeeEntryHistory", joined);
    }

    /// <summary>
    /// The schema-only shape: versioning is stated inline on the CREATE, and there is
    /// nothing to defer because there are no rows to load.
    /// </summary>
    [LiveFact]
    public async Task SystemVersionedTable_RoundTripsThroughComposeFullScript()
    {
        var sourceConnection = await CreateTemporalDatabaseAsync();
        var targetConnection = _sqlServer.CreateDatabase();

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var script = ScriptComposer.ComposeFullScript(source);
        Assert.Contains(
            "ALTER TABLE [dbo].[Employee] SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [history].[EmployeeEntryHistory]));",
            script);

        // The schema the history table goes in is a prerequisite of that statement and
        // is named nowhere else in the snapshot.
        Assert.Contains("CREATE SCHEMA [history]", script);

        await SqlServerFixture.ApplyAsync(targetConnection, script, useTransaction: false);

        var target = await SqlServerFixture.ExtractAsync(targetConnection);
        Snapshots.AssertNoChanges("composed source -> target", Compare(source, target));
        Snapshots.AssertNoChanges("composed target -> source", Compare(target, source));

        // And the target really is versioned, not merely a table with the same columns.
        Assert.Equal("SYSTEM_VERSIONED_TEMPORAL_TABLE", target.Table("dbo", "Employee").TemporalType);
    }

    /// <summary>
    /// The restore shape, which is the whole reason this work package exists.
    /// <c>ConstraintsAfterData</c> loads rows into a bare table, and a system-versioned
    /// table refuses an INSERT that names its period columns — so the table has to
    /// arrive with versioning off and be switched on in the finalize phase, after the
    /// rows are in.
    /// </summary>
    [LiveFact]
    public async Task SystemVersionedTable_RestoreShapeTurnsVersioningOnAfterTheRowsAreLoaded()
    {
        var sourceConnection = await CreateTemporalDatabaseAsync();
        var targetConnection = _sqlServer.CreateDatabase();

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var phases = ScriptComposer.ComposePhases(source, new ComposeOptions { ConstraintsAfterData = true });

        var tables = PhaseSql(phases, "tables");
        Assert.Contains("PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo])", tables);
        Assert.DoesNotContain("SYSTEM_VERSIONING", tables);
        Assert.Contains(
            "ALTER TABLE [dbo].[Employee] SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [history].[EmployeeEntryHistory]));",
            PhaseSql(phases, "finalize"));

        // Run the phases in order, and load rows between the tables phase and the rest
        // — exactly where a restore would, and exactly where a versioned table would
        // have refused them.
        foreach(var phase in phases)
        {
            var sql = PhaseSql(phases, phase.Name);
            if(string.IsNullOrWhiteSpace(sql))
                continue;

            await SqlServerFixture.ApplyAsync(targetConnection, sql, useTransaction: false);

            if(phase.Name == "tables")
            {
                await SqlServerFixture.ApplyAsync(targetConnection, """
                    INSERT INTO dbo.Employee (EmployeeId, FullName, Department, Salary)
                    VALUES (1, N'Ada', N'Engineering', 100), (2, N'Grace', N'Engineering', 120);
                    """, useTransaction: false);
            }
        }

        var target = await SqlServerFixture.ExtractAsync(targetConnection);
        Snapshots.AssertNoChanges("restore-shaped source -> target", Compare(source, target));
        Snapshots.AssertNoChanges("restore-shaped target -> source", Compare(target, source));

        Assert.Equal("SYSTEM_VERSIONED_TEMPORAL_TABLE", target.Table("dbo", "Employee").TemporalType);
        Assert.Equal(2, await SqlServerFixture.ScalarAsync<int>(targetConnection,
            "SELECT COUNT(*) FROM dbo.Employee;"));
    }

    [LiveFact]
    public async Task SystemVersionedTable_RoundTripsThroughTheDiffer()
    {
        var sourceConnection = await CreateTemporalDatabaseAsync();
        var targetConnection = _sqlServer.CreateDatabase();

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var empty = await SqlServerFixture.ExtractAsync(targetConnection);

        var deployment = _differ.Diff(source, empty, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: false, addOnly: false);
        Assert.Equal(0, deployment.Skipped);

        await SqlServerFixture.ApplyAsync(targetConnection, deployment.Script, useTransaction: false);

        var target = await SqlServerFixture.ExtractAsync(targetConnection);
        Snapshots.AssertNoChanges("source -> target", Compare(source, target));
        Snapshots.AssertNoChanges("target -> source", Compare(target, source));
    }

    /// <summary>
    /// Drift on a live system-versioned table. An added and a widened column both go
    /// through with versioning left on — SQL Server carries the change into the
    /// history table itself, which is why the differ must not turn versioning off
    /// around them.
    /// </summary>
    [LiveFact]
    public async Task ColumnDriftOnAVersionedTable_ConvergesWithoutCyclingVersioning()
    {
        var sourceConnection = await CreateTemporalDatabaseAsync();
        var targetConnection = await CreateTemporalDatabaseAsync();

        await SqlServerFixture.ApplyAsync(sourceConnection, """
            ALTER TABLE dbo.Employee ADD Nickname nvarchar(30) NULL;
            GO
            ALTER TABLE dbo.Employee ALTER COLUMN Department nvarchar(80) NULL;
            GO
            """, useTransaction: false);

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var target = await SqlServerFixture.ExtractAsync(targetConnection);

        var drift = _differ.Diff(source, target, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: false, addOnly: false);
        Assert.True(drift.HasChanges);
        Assert.DoesNotContain("SYSTEM_VERSIONING", drift.Script);

        await SqlServerFixture.ApplyAsync(targetConnection, drift.Script, useTransaction: false);

        var converged = await SqlServerFixture.ExtractAsync(targetConnection);
        Snapshots.AssertNoChanges("after the drift", Compare(source, converged));
        Assert.Equal("SYSTEM_VERSIONED_TEMPORAL_TABLE", converged.Table("dbo", "Employee").TemporalType);
    }

    /// <summary>
    /// A change no ALTER can make on a versioned table. The differ has to refuse the
    /// rebuild rather than plan a DROP TABLE that SQL Server will not run, and say why.
    /// </summary>
    [LiveFact]
    public async Task AVersionedTableTheDifferCannotExpress_IsRefusedRatherThanDropped()
    {
        var sourceConnection = await CreateTemporalDatabaseAsync();
        var targetConnection = await CreateTemporalDatabaseAsync();

        // Adding a computed column is refused with versioning on, and doing it with
        // versioning off leaves the history table a column short.
        await SqlServerFixture.ApplyAsync(sourceConnection, """
            ALTER TABLE dbo.Employee SET (SYSTEM_VERSIONING = OFF);
            GO
            ALTER TABLE dbo.Employee ADD DoubleSalary AS (Salary * 2);
            GO
            ALTER TABLE history.EmployeeEntryHistory ADD DoubleSalary decimal(21,4) NULL;
            GO
            ALTER TABLE dbo.Employee SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = history.EmployeeEntryHistory));
            GO
            """, useTransaction: false);

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var target = await SqlServerFixture.ExtractAsync(targetConnection);

        var diff = _differ.Diff(source, target, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: true, addOnly: false);

        Assert.DoesNotContain("DROP TABLE [dbo].[Employee]", diff.Script);
        Assert.Contains("system-versioned", diff.Script);
    }

    // ---------------------------------------------------------- memory-optimized

    [MemoryOptimizedFact]
    public async Task MemoryOptimizedTable_IsCapturedWithItsIndexesAndBucketCounts()
    {
        var connectionString = await CreateMemoryOptimizedDatabaseAsync();
        var (snapshot, notices) = await SqlServerFixture.ExtractWithNoticesAsync(connectionString);

        var order = snapshot.Table("dbo", "MemOrder");
        Assert.True(order.IsMemoryOptimized);
        Assert.Equal("SCHEMA_AND_DATA", order.Durability);
        Assert.Equal(1024, order.Key("PK_MemOrder").BucketCount);
        Assert.Equal(512, order.Index("IX_MemOrder_Code").BucketCount);
        Assert.Equal(0, order.Index("IX_MemOrder_Total").BucketCount);
        Assert.Contains("HASH", order.Index("IX_MemOrder_Code").TypeDesc, StringComparison.OrdinalIgnoreCase);

        Assert.Equal("SCHEMA_ONLY", snapshot.Table("dbo", "MemScratch").Durability);
        Assert.Equal(0, snapshot.Table("dbo", "MemScratch").Key("PK_MemScratch").BucketCount);

        // The hash index used to be dropped from the snapshot with a notice. Now the
        // only thing said about a memory-optimized table is the one thing no snapshot
        // can carry: its filegroup.
        var joined = string.Join(Environment.NewLine, notices);
        Assert.DoesNotContain("IX_MemOrder_Code", joined);
        Assert.Contains("MEMORY_OPTIMIZED_DATA", joined);
    }

    [MemoryOptimizedFact]
    public async Task MemoryOptimizedTable_RoundTripsToAnEmptyDiff()
    {
        var sourceConnection = await CreateMemoryOptimizedDatabaseAsync();
        var targetConnection = _sqlServer.CreateDatabaseWithMemoryOptimizedFilegroup();

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var script = ScriptComposer.ComposeFullScript(source);

        Assert.Contains("MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA", script);
        Assert.Contains("PRIMARY KEY NONCLUSTERED HASH ([OrderId]) WITH (BUCKET_COUNT = 1024)", script);
        Assert.DoesNotContain("CREATE NONCLUSTERED INDEX [IX_MemOrder_Total]", script);

        await SqlServerFixture.ApplyAsync(targetConnection, script, useTransaction: false);

        var target = await SqlServerFixture.ExtractAsync(targetConnection);
        Snapshots.AssertNoChanges("composed source -> target", Compare(source, target));
        Snapshots.AssertNoChanges("composed target -> source", Compare(target, source));
        Assert.True(target.Table("dbo", "MemOrder").IsMemoryOptimized);
    }

    /// <summary>
    /// The restore shape has nothing extra to do for a memory-optimized table, but it
    /// must not route its keys and indexes to the later phases either: they only exist
    /// inside CREATE TABLE.
    /// </summary>
    [MemoryOptimizedFact]
    public async Task MemoryOptimizedTable_RoundTripsUnderConstraintsAfterData()
    {
        var sourceConnection = await CreateMemoryOptimizedDatabaseAsync();
        var targetConnection = _sqlServer.CreateDatabaseWithMemoryOptimizedFilegroup();

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var phases = ScriptComposer.ComposePhases(source, new ComposeOptions { ConstraintsAfterData = true });

        Assert.DoesNotContain("MemOrder", PhaseSql(phases, "indexes"));

        foreach(var phase in phases)
        {
            var sql = PhaseSql(phases, phase.Name);
            if(!string.IsNullOrWhiteSpace(sql))
                await SqlServerFixture.ApplyAsync(targetConnection, sql, useTransaction: false);
        }

        var target = await SqlServerFixture.ExtractAsync(targetConnection);
        Snapshots.AssertNoChanges("restore-shaped source -> target", Compare(source, target));
        Snapshots.AssertNoChanges("restore-shaped target -> source", Compare(target, source));
    }

    [MemoryOptimizedFact]
    public async Task AddingAnIndexToAMemoryOptimizedTable_GoesThroughAlterTable()
    {
        var sourceConnection = await CreateMemoryOptimizedDatabaseAsync();
        var targetConnection = await CreateMemoryOptimizedDatabaseAsync();

        await SqlServerFixture.ApplyAsync(sourceConnection,
            "ALTER TABLE dbo.MemOrder ADD INDEX IX_MemOrder_Note NONCLUSTERED (Note);",
            useTransaction: false);

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var target = await SqlServerFixture.ExtractAsync(targetConnection);

        var drift = _differ.Diff(source, target, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: false, addOnly: false);
        Assert.Contains("ALTER TABLE [dbo].[MemOrder] ADD INDEX [IX_MemOrder_Note]", drift.Script);
        Assert.DoesNotContain("CREATE NONCLUSTERED INDEX", drift.Script);

        await SqlServerFixture.ApplyAsync(targetConnection, drift.Script, useTransaction: false);

        Snapshots.AssertNoChanges("after adding the index",
            Compare(source, await SqlServerFixture.ExtractAsync(targetConnection)));
    }

    // -------------------------------------------------------------------- helpers

    private async Task<string> CreateTemporalDatabaseAsync()
    {
        var connectionString = _sqlServer.CreateDatabase();
        await SqlServerFixture.ApplyAsync(connectionString, Section("TEMPORAL", "MEMORY"), useTransaction: false);
        return connectionString;
    }

    private async Task<string> CreateMemoryOptimizedDatabaseAsync()
    {
        var connectionString = _sqlServer.CreateDatabaseWithMemoryOptimizedFilegroup();
        await SqlServerFixture.ApplyAsync(connectionString, Section("MEMORY", null), useTransaction: false);
        return connectionString;
    }

    /// <summary>Cuts one <c>@@MARKER@@</c> block out of the fixture script.</summary>
    private static string Section(string from, string? to)
    {
        var script = SqlServerFixture.SpecialTablesScript;
        var start = script.IndexOf($"-- @@{from}@@", StringComparison.Ordinal);
        Assert.True(start >= 0, $"marker @@{from}@@ is missing from special-tables.sql");

        if(to is null)
            return script[start..];

        var end = script.IndexOf($"-- @@{to}@@", start, StringComparison.Ordinal);
        Assert.True(end > start, $"marker @@{to}@@ is missing from special-tables.sql");
        return script[start..end];
    }

    private static string PhaseSql(IReadOnlyList<ScriptPhase> phases, string name) =>
        string.Join(
            Environment.NewLine + "GO" + Environment.NewLine,
            phases.Single(x => x.Name == name).Batches.Select(x => x.Sql));

    private DiffResult Compare(DatabaseSnapshot source, DatabaseSnapshot target) =>
        _differ.Diff(source, target, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: false, addOnly: false);
}
