using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

/// <summary>
/// WP 1.8a. A restore that loads rows into a table SQL Server is already writing
/// the period columns of cannot keep the row's original <c>ValidFrom</c>: the
/// INSERT is refused outright (error 13536), so the row arrives stamped with the
/// instant of the restore and every <c>AS OF</c> between the end of the archived
/// history and that instant answers nothing. <see cref="ComposeOptions.PeriodAfterData"/>
/// moves the period out of CREATE TABLE and into the finalize phase so the rows
/// can be loaded with the periods they had.
/// <para>
/// Half of these tests exist to prove the other half changes nothing: with the
/// option off — which is every caller that has not asked for it — the script is
/// the one 1.7 emitted, character for character.
/// </para>
/// </summary>
public class PeriodAfterDataTests
{
    private static readonly string NL = Environment.NewLine;

    /// <summary>The CREATE 1.7 emits for <see cref="Employee"/>, character for character.</summary>
    private const string InlinePeriodCreate = """
                                              CREATE TABLE [dbo].[Employee]
                                              (
                                                  [EmployeeId] int NOT NULL,
                                                  [FullName] nvarchar(80) NOT NULL,
                                                  [ValidFrom] datetime2(7) GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
                                                  [ValidTo] datetime2(7) GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,
                                                  PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo])
                                              );
                                              """;

    private const string VersioningOn =
        "ALTER TABLE [dbo].[Employee] SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[EmployeeHistory]));";

    // ------------------------------------------------ the default is untouched

    /// <summary>
    /// The guarantee the option is worth nothing without: every caller that does not
    /// ask for it gets the 1.7 script. The expected text here is what <c>main</c>
    /// produces, so a change to the inline rendering fails this test rather than
    /// silently reshaping every existing consumer's script.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void WithTheOptionOff_TheTemporalTableKeepsItsInlinePeriod(bool constraintsAfterData)
    {
        var phases = Compose(Employee(hidden: true), new ComposeOptions { ConstraintsAfterData = constraintsAfterData });

        Assert.Equal(Normalize(InlinePeriodCreate), Sql(phases, "tables"));
        Assert.Equal(VersioningOn, Sql(phases, "finalize"));
    }

    [Fact]
    public void TheDefaultOptions_AreTheOptionOff()
    {
        var phases = Compose(Employee(hidden: true), ComposeOptions.Default);

        Assert.Equal(Normalize(InlinePeriodCreate), Sql(phases, "tables"));
        Assert.Equal(VersioningOn, Sql(phases, "finalize"));
    }

    /// <summary>
    /// The renderer the diff path calls — one argument, no options — has to keep
    /// emitting the inline period whatever this work package added beside it.
    /// </summary>
    [Fact]
    public void TheOneArgumentRenderer_StillEmitsTheInlinePeriod() =>
        Assert.Equal(Normalize(InlinePeriodCreate), SqlRender.BuildTableCreateOnly(Employee(hidden: true)));

    // ------------------------------------------------------- the option is on

    /// <summary>
    /// The two columns come out plain. Not <c>GENERATED ALWAYS</c>, because that is
    /// what refuses the INSERT; not <c>HIDDEN</c>, because SQL Server will only set
    /// HIDDEN on a column that is already GENERATED ALWAYS (error 13735); and not
    /// <c>datetime2</c> with no scale, because the scale has to be the source's — the
    /// maximum value <c>ADD PERIOD</c> demands in the end column is the maximum for
    /// that scale and nothing else.
    /// </summary>
    [Fact]
    public void WithTheOptionOn_ThePeriodColumnsComeOutPlainAndThePeriodIsGone()
    {
        var expected = """
                       CREATE TABLE [dbo].[Employee]
                       (
                           [EmployeeId] int NOT NULL,
                           [FullName] nvarchar(80) NOT NULL,
                           [ValidFrom] datetime2(7) NOT NULL,
                           [ValidTo] datetime2(7) NOT NULL
                       );
                       """;

        var phases = Compose(Employee(hidden: true), new ComposeOptions { PeriodAfterData = true });

        Assert.Equal(Normalize(expected), Sql(phases, "tables"));
    }

    /// <summary>
    /// Both statements land in finalize, and the period comes first:
    /// <c>SYSTEM_VERSIONING = ON</c> has nothing to turn on until <c>ADD PERIOD</c>
    /// has converted the loaded columns.
    /// </summary>
    [Fact]
    public void WithTheOptionOn_ThePeriodIsAddedInFinalizeBeforeVersioning()
    {
        var expected =
            "ALTER TABLE [dbo].[Employee] ADD PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]);" + NL +
            "ALTER TABLE [dbo].[Employee] ALTER COLUMN [ValidFrom] ADD HIDDEN;" + NL +
            "ALTER TABLE [dbo].[Employee] ALTER COLUMN [ValidTo] ADD HIDDEN;" + NL +
            VersioningOn;

        var phases = Compose(Employee(hidden: true), new ComposeOptions { PeriodAfterData = true });
        var finalize = phases.Single(x => x.Name == "finalize");

        Assert.Equal(expected, Sql(phases, "finalize"));
        Assert.Equal(
            new[] { "Period for system time on [dbo].[Employee]", "System versioning on [dbo].[Employee]" },
            finalize.Batches.Select(x => x.Describe));
    }

    /// <summary>
    /// A period column that was not hidden on the source must not become hidden on
    /// the target: HIDDEN is a property of the column, and a restore that invents one
    /// changes what <c>SELECT *</c> returns.
    /// </summary>
    [Fact]
    public void WithTheOptionOn_APeriodColumnThatWasNotHidden_GetsNoAddHidden()
    {
        var phases = Compose(Employee(hidden: false), new ComposeOptions { PeriodAfterData = true });

        Assert.DoesNotContain("HIDDEN", Sql(phases, "finalize"), StringComparison.Ordinal);
        Assert.DoesNotContain("HIDDEN", Sql(phases, "tables"), StringComparison.Ordinal);
    }

    /// <summary>
    /// A memory-optimized temporal table takes the same route. Measured on SQL Server
    /// 2025: <c>ALTER TABLE ... ADD PERIOD</c>, <c>ALTER COLUMN ... ADD HIDDEN</c> and
    /// <c>SET (SYSTEM_VERSIONING = ON)</c> against a populated disk-based history
    /// table are all accepted on one, so there is nothing to special-case. Its keys
    /// and indexes stay inline, where they have to be.
    /// </summary>
    [Fact]
    public void WithTheOptionOn_AMemoryOptimizedTemporalTable_DefersItsPeriodToo()
    {
        var table = Employee(hidden: false);
        table.IsMemoryOptimized = true;
        table.Durability = "SCHEMA_AND_DATA";
        table.KeyConstraints.Add(Key("PK_Employee", "PK", "NONCLUSTERED", "EmployeeId"));

        var phases = Compose(table, new ComposeOptions { PeriodAfterData = true });
        var tables = Sql(phases, "tables");

        Assert.DoesNotContain("PERIOD FOR SYSTEM_TIME", tables, StringComparison.Ordinal);
        Assert.DoesNotContain("GENERATED ALWAYS", tables, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT [PK_Employee] PRIMARY KEY NONCLUSTERED ([EmployeeId] ASC)", tables, StringComparison.Ordinal);
        Assert.Contains("WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);", tables, StringComparison.Ordinal);
        Assert.Contains("ADD PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]);", Sql(phases, "finalize"), StringComparison.Ordinal);
    }

    /// <summary>
    /// A period column keeps the scale it had. <c>ADD PERIOD</c> wants the end column
    /// at the maximum <i>for its own scale</i> — <c>…59.999</c> at <c>datetime2(3)</c>,
    /// not <c>…59.9999999</c> — so a renderer that dropped the scale would produce a
    /// script that runs where the source happened to be scale 7 and fails with 13575
    /// everywhere else.
    /// </summary>
    [Fact]
    public void WithTheOptionOn_APeriodColumnKeepsItsScale()
    {
        var table = Employee(hidden: false);
        foreach(var column in table.Columns.Where(x => x.GeneratedAlwaysType is 1 or 2))
            column.Scale = 3;

        var tables = Sql(Compose(table, new ComposeOptions { PeriodAfterData = true }), "tables");

        Assert.Contains("[ValidFrom] datetime2(3) NOT NULL", tables, StringComparison.Ordinal);
        Assert.Contains("[ValidTo] datetime2(3) NOT NULL", tables, StringComparison.Ordinal);
    }

    /// <summary>
    /// A period without system versioning is a real state — and the one this option
    /// is keyed on, because the period alone is what refuses the INSERT. Such a table
    /// gets its ADD PERIOD and no versioning statement at all.
    /// </summary>
    [Fact]
    public void APeriodWithoutVersioning_IsStillDeferred()
    {
        var table = Employee(hidden: false);
        table.TemporalType = null;
        table.HistoryTableName = null;

        var phases = Compose(table, new ComposeOptions { PeriodAfterData = true });

        Assert.Equal(
            "ALTER TABLE [dbo].[Employee] ADD PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]);",
            Sql(phases, "finalize"));
    }

    /// <summary>
    /// A nullable period column is a shape SQL Server will not take back:
    /// <c>ADD PERIOD</c> refuses it with error 13587. It cannot exist on a real
    /// source, so a model carrying one is already wrong — but emitting NOT NULL is
    /// both the shape the column ends up in anyway and the only one that runs.
    /// </summary>
    [Fact]
    public void WithTheOptionOn_APeriodColumnIsEmittedNotNullWhateverTheModelSays()
    {
        var table = Employee(hidden: false);
        foreach(var column in table.Columns.Where(x => x.GeneratedAlwaysType is 1 or 2))
            column.IsNullable = true;

        Assert.Contains(
            "[ValidFrom] datetime2(7) NOT NULL",
            Sql(Compose(table, new ComposeOptions { PeriodAfterData = true }), "tables"),
            StringComparison.Ordinal);
    }

    /// <summary>An ordinary table cannot tell the option was on.</summary>
    [Fact]
    public void ATableWithNoPeriod_IsUnaffected()
    {
        var table = Table("Plain", Col("Id", nullable: false), NVarchar("Name", 20));

        Assert.Equal(
            SqlRender.BuildTableCreateOnly(table),
            Sql(Compose(table, new ComposeOptions { PeriodAfterData = true }), "tables"));

        Assert.Null(SqlRender.BuildPeriodAdd(table));
    }

    // ------------------------------------------------------------------ helpers

    private static string Normalize(string text) => text.Replace("\r\n", "\n").Replace("\n", NL);

    private static string Sql(IReadOnlyList<ScriptPhase> phases, string phaseName) =>
        string.Join(NL, phases.Single(x => x.Name == phaseName).Batches.Select(x => x.Sql));

    private static ColumnModel PeriodColumn(string name, byte generatedAlwaysType, bool hidden)
    {
        var column = Col(name, "datetime2", maxLength: 8, precision: 27, scale: 7, nullable: false);
        column.GeneratedAlwaysType = generatedAlwaysType;
        column.IsHidden = hidden;
        return column;
    }

    private static TableModel Employee(bool hidden)
    {
        var table = Table("Employee",
            Col("EmployeeId", nullable: false),
            NVarchar("FullName", 80, nullable: false, collation: null!),
            PeriodColumn("ValidFrom", 1, hidden),
            PeriodColumn("ValidTo", 2, hidden));

        table.TemporalType = "SYSTEM_VERSIONED_TEMPORAL_TABLE";
        table.HistoryTableSchema = "dbo";
        table.HistoryTableName = "EmployeeHistory";
        table.PeriodStartColumn = "ValidFrom";
        table.PeriodEndColumn = "ValidTo";
        return table;
    }

    private static IReadOnlyList<ScriptPhase> Compose(TableModel table, ComposeOptions options) =>
        ScriptComposer.ComposePhases(Snapshot("Db", TableObject(table, SqlRender.BuildTableCreateScript(table))), options);
}
