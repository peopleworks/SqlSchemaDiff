using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

/// <summary>
/// The two table shapes 1.6 captured and refused to script. Both need syntax that
/// only exists inside <c>CREATE TABLE</c> — a <c>PERIOD FOR SYSTEM_TIME</c>, a
/// memory-optimized table's whole index list — and a system-versioned table also
/// needs an order the restore shape has to respect: rows cannot be loaded while
/// SQL Server is writing the period columns.
/// </summary>
public class SpecialTableTests
{
    private readonly TableDiffer _differ = new();
    private readonly SchemaDiffer _schemaDiffer = new();

    private static readonly string NL = Environment.NewLine;

    // ------------------------------------------------------------- rendering

    [Fact]
    public void PeriodColumn_RendersGeneratedAlwaysAndHidden()
    {
        Assert.Equal(
            "[ValidFrom] datetime2(7) GENERATED ALWAYS AS ROW START HIDDEN NOT NULL",
            SqlRender.BuildColumnDefinition(PeriodColumn("ValidFrom", 1, hidden: true)));

        Assert.Equal(
            "[ValidTo] datetime2(7) GENERATED ALWAYS AS ROW END NOT NULL",
            SqlRender.BuildColumnDefinition(PeriodColumn("ValidTo", 2, hidden: false)));
    }

    // sys.columns.generated_always_type also numbers the ledger columns. Scripting
    // one of those as a period column would produce a CREATE TABLE that fails, so
    // anything past ROW END is left alone.
    [Fact]
    public void ALedgerGeneratedColumn_IsNotScriptedAsAPeriodColumn()
    {
        var column = Col("TransactionId", "bigint", nullable: false);
        column.GeneratedAlwaysType = 5;

        Assert.Equal("[TransactionId] bigint NOT NULL", SqlRender.BuildColumnDefinition(column));
        Assert.Null(SqlRender.BuildGeneratedAlwaysClause(column));
    }

    /// <summary>
    /// The CREATE carries the period columns and the period itself — they are part of
    /// the row — but never the SYSTEM_VERSIONING clause. SQL Server refuses to version
    /// a table with no primary key, and this renderer always attaches the key with an
    /// ALTER of its own, so versioning is a statement of its own too.
    /// </summary>
    [Fact]
    public void SystemVersionedTable_ScriptsThePeriodOnTheCreateAndVersioningSeparately()
    {
        var expected =
            "CREATE TABLE [dbo].[Employee]" + NL +
            "(" + NL +
            "    [EmployeeId] int NOT NULL," + NL +
            "    [FullName] nvarchar(80) NOT NULL," + NL +
            "    [ValidFrom] datetime2(7) GENERATED ALWAYS AS ROW START HIDDEN NOT NULL," + NL +
            "    [ValidTo] datetime2(7) GENERATED ALWAYS AS ROW END HIDDEN NOT NULL," + NL +
            "    PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo])" + NL +
            ");";

        Assert.Equal(expected, SqlRender.BuildTableCreateOnly(Employee()));
    }

    [Fact]
    public void TheFullCreateScript_TurnsVersioningOnAfterThePrimaryKey()
    {
        var table = Employee();
        table.KeyConstraints.Add(Key("PK_Employee", "PK", "CLUSTERED", "EmployeeId"));

        var script = SqlRender.BuildTableCreateScript(table);

        AssertOrder(script,
            "CREATE TABLE [dbo].[Employee]",
            "ADD CONSTRAINT [PK_Employee] PRIMARY KEY CLUSTERED ([EmployeeId] ASC);",
            "ALTER TABLE [dbo].[Employee] SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[EmployeeHistory]));");
    }

    [Fact]
    public void SystemVersioningStatements_NameTheHistoryTable()
    {
        Assert.Equal(
            "ALTER TABLE [dbo].[Employee] SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[EmployeeHistory]));",
            SqlRender.BuildSystemVersioningOn(Employee()));

        Assert.Equal(
            "ALTER TABLE [dbo].[Employee] SET (SYSTEM_VERSIONING = OFF);",
            SqlRender.BuildSystemVersioningOff(Employee()));
    }

    [Fact]
    public void HistoryTableInAnotherSchema_KeepsThatSchema()
    {
        var table = Employee();
        table.HistoryTableSchema = "history";

        Assert.Contains("HISTORY_TABLE = [history].[EmployeeHistory]", SqlRender.BuildSystemVersioningOn(table));
    }

    [Fact]
    public void MemoryOptimizedTable_TakesItsKeysAndIndexesInline()
    {
        var expected =
            SqlRender.MemoryOptimizedFilegroupComment + NL +
            "CREATE TABLE [dbo].[MemOrder]" + NL +
            "(" + NL +
            "    [OrderId] int NOT NULL," + NL +
            "    [Code] nvarchar(40) NOT NULL," + NL +
            "    [Total] int NULL," + NL +
            "    CONSTRAINT [PK_MemOrder] PRIMARY KEY NONCLUSTERED HASH ([OrderId]) WITH (BUCKET_COUNT = 1024)," + NL +
            "    INDEX [IX_MemOrder_Code] HASH ([Code]) WITH (BUCKET_COUNT = 512)," + NL +
            "    INDEX [IX_MemOrder_Total] NONCLUSTERED ([Total] ASC)" + NL +
            ") WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);";

        Assert.Equal(expected, SqlRender.BuildTableCreateOnly(MemOrder()));
    }

    [Fact]
    public void MemoryOptimizedTable_ScriptsItsDurability()
    {
        var table = MemOrder();
        table.Durability = "SCHEMA_ONLY";

        Assert.Contains("WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY);", SqlRender.BuildTableCreateOnly(table));
    }

    // An index on a memory-optimized table lives in memory: SQL Server reports
    // allow_row_locks and allow_page_locks as 0 and rejects both options outright,
    // exactly as it does for a columnstore index.
    [Fact]
    public void MemoryOptimizedIndex_DoesNotScriptLockOrPageOptions()
    {
        var table = MemOrder();
        foreach(var index in table.Indexes)
        {
            index.AllowRowLocks = false;
            index.AllowPageLocks = false;
            index.DataCompression = "NONE";
        }

        var sql = SqlRender.BuildTableCreateOnly(table);

        Assert.DoesNotContain("ALLOW_ROW_LOCKS", sql);
        Assert.DoesNotContain("ALLOW_PAGE_LOCKS", sql);
        Assert.DoesNotContain("DATA_COMPRESSION", sql);
    }

    [Fact]
    public void MemoryOptimizedTable_ScriptsNoCreateIndexAndNoAlterTableAddConstraint()
    {
        var script = SqlRender.BuildTableCreateScript(MemOrder());

        Assert.DoesNotContain("CREATE NONCLUSTERED INDEX", script);
        Assert.DoesNotContain("CREATE INDEX", script);
        Assert.DoesNotContain("ADD CONSTRAINT [PK_MemOrder]", script);
        Assert.Contains("PRIMARY KEY NONCLUSTERED HASH ([OrderId])", script);
    }

    [Fact]
    public void MemoryOptimizedTable_StillScriptsItsChecksAndForeignKeys()
    {
        var table = MemOrder();
        table.CheckConstraints.Add(Check("CK_MemOrder_Total", "([Total]>=(0))"));
        table.ForeignKeys.Add(ForeignKey("FK_MemOrder_Customer", "MemCustomer", "OrderId"));

        var script = SqlRender.BuildTableCreateScript(table);

        Assert.Contains("ADD CONSTRAINT [CK_MemOrder_Total] CHECK ([Total]>=(0));", script);
        Assert.Contains("ADD CONSTRAINT [FK_MemOrder_Customer] FOREIGN KEY ([OrderId])", script);
    }

    // CREATE INDEX and DROP INDEX are both rejected on a memory-optimized table;
    // ALTER TABLE ADD/DROP INDEX is the form that works.
    [Fact]
    public void IndexChangesOnAMemoryOptimizedTable_GoThroughAlterTable()
    {
        var source = MemOrder();
        var target = MemOrder();
        target.Indexes.RemoveAll(x => x.Name == "IX_MemOrder_Total");
        source.Indexes.Add(HashIndex("IX_MemOrder_Extra", 256, "Code"));

        var result = _differ.Diff(source, target, includeDrops: true);

        Assert.Contains("ALTER TABLE [dbo].[MemOrder] ADD INDEX [IX_MemOrder_Total] NONCLUSTERED ([Total] ASC);", result.Script);
        Assert.Contains("ALTER TABLE [dbo].[MemOrder] ADD INDEX [IX_MemOrder_Extra] HASH ([Code]) WITH (BUCKET_COUNT = 256);", result.Script);
        Assert.DoesNotContain("CREATE NONCLUSTERED INDEX", result.Script);
        Assert.DoesNotContain("DROP INDEX [IX_MemOrder_Total] ON", result.Script);
    }

    [Fact]
    public void ADroppedMemoryOptimizedIndex_UsesAlterTableDropIndex()
    {
        var source = MemOrder();
        source.Indexes.RemoveAll(x => x.Name == "IX_MemOrder_Total");
        var target = MemOrder();

        var result = _differ.Diff(source, target, includeDrops: true);

        Assert.Contains("ALTER TABLE [dbo].[MemOrder] DROP INDEX [IX_MemOrder_Total];", result.Script);
    }

    // --------------------------------------------------------------- composer

    /// <summary>
    /// Both shapes put the switch in the finalize phase. Under
    /// <c>ConstraintsAfterData</c> that is what lets the rows load first; without it,
    /// it is still the only place the primary key is guaranteed to exist.
    /// </summary>
    [Fact]
    public void TheVersioningSwitchLandsInFinalize_InEitherShape()
    {
        foreach(var options in new[] { ComposeOptions.Default, new ComposeOptions { ConstraintsAfterData = true } })
        {
            var phases = Compose(Employee(), options);

            Assert.DoesNotContain("SYSTEM_VERSIONING", PhaseSql(phases, "tables"));
            Assert.Contains("PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo])", PhaseSql(phases, "tables"));
            Assert.Contains(
                "ALTER TABLE [dbo].[Employee] SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[EmployeeHistory]));",
                PhaseSql(phases, "finalize"));
        }
    }

    [Fact]
    public void AnOrdinaryTable_PutsNothingInFinalize()
    {
        var phases = Compose(Table("Plain", Col("Id", nullable: false)), ComposeOptions.Default);

        Assert.Equal(string.Empty, PhaseSql(phases, "finalize"));
    }

    [Fact]
    public void TheComposerNeverEmitsACreateIndexForAMemoryOptimizedTable()
    {
        foreach(var options in new[] { ComposeOptions.Default, new ComposeOptions { ConstraintsAfterData = true } })
        {
            var phases = Compose(MemOrder(), options);
            var all = string.Join(Environment.NewLine, phases.SelectMany(x => x.Batches).Select(x => x.Sql));

            Assert.DoesNotContain("CREATE INDEX", all);
            Assert.DoesNotContain("CREATE NONCLUSTERED INDEX", all);
            Assert.DoesNotContain("ADD CONSTRAINT [PK_MemOrder]", all);
            Assert.Empty(PhaseSql(phases, "indexes"));

            // Everything the table needs is in the one CREATE, filegroup note included.
            var tables = PhaseSql(phases, "tables");
            Assert.Contains("PRIMARY KEY NONCLUSTERED HASH ([OrderId]) WITH (BUCKET_COUNT = 1024)", tables);
            Assert.Contains("INDEX [IX_MemOrder_Code] HASH ([Code]) WITH (BUCKET_COUNT = 512)", tables);
            Assert.Contains("MEMORY_OPTIMIZED_DATA", tables);
        }
    }

    // ------------------------------------------------------------------- diff

    [Fact]
    public void TurningSystemVersioningOn_EmitsTheSetStatement()
    {
        var target = Employee();
        target.TemporalType = null;
        target.HistoryTableSchema = null;
        target.HistoryTableName = null;

        var result = _differ.Diff(Employee(), target, includeDrops: true);

        Assert.True(result.HasChanges);
        Assert.Contains(
            "ALTER TABLE [dbo].[Employee] SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[EmployeeHistory]));",
            result.Script);
        Assert.DoesNotContain("SYSTEM_VERSIONING = OFF", result.Script);
    }

    [Fact]
    public void TurningSystemVersioningOff_EmitsTheSetStatementAndSaysWhatHappensToTheHistory()
    {
        var source = Employee();
        source.TemporalType = null;

        var result = _differ.Diff(source, Employee(), includeDrops: true);

        Assert.True(result.HasChanges);
        Assert.Contains("ALTER TABLE [dbo].[Employee] SET (SYSTEM_VERSIONING = OFF);", result.Script);
        Assert.DoesNotContain("SYSTEM_VERSIONING = ON", result.Script);
        Assert.Contains("stays behind as an ordinary table", result.Script);
    }

    [Fact]
    public void MovingTheHistoryTable_TurnsVersioningOffAndBackOn()
    {
        var target = Employee();
        target.HistoryTableName = "EmployeeArchive";

        var result = _differ.Diff(Employee(), target, includeDrops: true);

        var off = result.Script.IndexOf("SYSTEM_VERSIONING = OFF", StringComparison.Ordinal);
        var on = result.Script.IndexOf("SYSTEM_VERSIONING = ON", StringComparison.Ordinal);
        Assert.True(off >= 0 && on > off, $"expected OFF then ON:{NL}{result.Script}");
        Assert.Contains("the rows already recorded stay", result.Script);
    }

    /// <summary>
    /// The spec for this work package asked for off-change-on around an added column.
    /// SQL Server says otherwise, and the server is the authority: ADD, DROP and
    /// ALTER COLUMN all work with versioning on and are propagated into the history
    /// table, while doing them with versioning off leaves the two out of step and the
    /// SET ... ON afterwards fails with a column-count mismatch. So an ordinary column
    /// change on a versioned table emits the column change and nothing else.
    /// </summary>
    [Fact]
    public void AddingAColumnToAVersionedTable_DoesNotCycleVersioning()
    {
        var source = Employee();
        source.Columns.Insert(2, NVarchar("Nickname", 40, collation: null!));

        var result = _differ.Diff(source, Employee(), includeDrops: true);

        Assert.Contains("ALTER TABLE [dbo].[Employee] ADD [Nickname] nvarchar(40) NULL;", result.Script);
        Assert.DoesNotContain("SYSTEM_VERSIONING", result.Script);
    }

    // The one column change SQL Server refuses outright, with versioning on or off.
    [Fact]
    public void AddingAComputedColumnToAVersionedTable_IsReportedRatherThanScripted()
    {
        var source = Employee();
        source.Columns.Add(Col("Label", "nvarchar", computedDefinition: "([FullName])"));

        var result = _differ.Diff(source, Employee(), includeDrops: true);

        Assert.True(result.RequiresRebuild);
        Assert.Contains(result.RebuildReasons, x => x.Contains("computed column [Label]"));
        Assert.Contains("cannot be added to a system-versioned table", result.Script);
        Assert.Contains("is system-versioned and cannot be rebuilt", result.Script);
    }

    /// <summary>
    /// A table gaining a period gets its two columns and the period in <b>one</b>
    /// statement, then loses the defaults that only existed so the columns could be
    /// added to a table with rows in it.
    /// </summary>
    /// <remarks>
    /// Until 1.8 this test asserted the opposite — three separate statements — and
    /// passed, because it read the script and never ran it. That script cannot run:
    /// SQL Server refuses a <c>GENERATED ALWAYS</c> column while no period is defined
    /// (13509), so the <c>ADD PERIOD</c> behind it names columns that do not exist
    /// (4924) and the versioning switch fails after that (13510). Measured on SQL
    /// Server 2025. <c>PeriodArrivalLiveTests</c> now runs the script; this one only
    /// pins its shape.
    /// </remarks>
    [Fact]
    public void AddingAPeriodToATableThatHasNone_AddsBothColumnsAndThePeriodInOneStatement()
    {
        var target = Table("Employee", Col("EmployeeId", nullable: false), NVarchar("FullName", 80, nullable: false));

        var result = _differ.Diff(Employee(), target, includeDrops: true);

        AssertOrder(result.Script,
            "ALTER TABLE [dbo].[Employee] ADD",
            "[ValidFrom] datetime2(7) GENERATED ALWAYS AS ROW START HIDDEN NOT NULL CONSTRAINT [DF_sqldiff_period_Employee_ValidFrom] DEFAULT SYSUTCDATETIME(),",
            "[ValidTo] datetime2(7) GENERATED ALWAYS AS ROW END HIDDEN NOT NULL CONSTRAINT [DF_sqldiff_period_Employee_ValidTo] DEFAULT '9999-12-31 23:59:59.9999999',",
            "PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]);",
            "ALTER TABLE [dbo].[Employee] DROP CONSTRAINT [DF_sqldiff_period_Employee_ValidFrom], [DF_sqldiff_period_Employee_ValidTo];",
            "SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[EmployeeHistory]));");

        // The period columns must not also be emitted one at a time - that is the
        // statement the server refuses with 13509.
        Assert.DoesNotContain("ADD [ValidFrom] datetime2(7) GENERATED ALWAYS", result.Script, StringComparison.Ordinal);
    }

    [Fact]
    public void DroppingAPeriod_TurnsVersioningOffFirst()
    {
        var source = Table("Employee", Col("EmployeeId", nullable: false), NVarchar("FullName", 80, nullable: false));

        var result = _differ.Diff(source, Employee(), includeDrops: true);

        AssertOrder(result.Script,
            "ALTER TABLE [dbo].[Employee] SET (SYSTEM_VERSIONING = OFF);",
            "ALTER TABLE [dbo].[Employee] DROP PERIOD FOR SYSTEM_TIME;",
            "DROP COLUMN [ValidFrom];");
    }

    [Fact]
    public void TwoIdenticalSystemVersionedTables_AreNotADifference()
    {
        var result = _differ.Diff(Employee(), Employee(), includeDrops: true);

        Assert.False(result.HasChanges);
        Assert.Equal(string.Empty, result.Script);
    }

    [Fact]
    public void TwoIdenticalMemoryOptimizedTables_AreNotADifference()
    {
        var result = _differ.Diff(MemOrder(), MemOrder(), includeDrops: true);

        Assert.False(result.HasChanges);
        Assert.Equal(string.Empty, result.Script);
    }

    [Fact]
    public void AChangedBucketCount_ReadsAsAnIndexChange()
    {
        var source = MemOrder();
        source.Indexes.Single(x => x.Name == "IX_MemOrder_Code").BucketCount = 4096;

        var result = _differ.Diff(source, MemOrder(), includeDrops: true);

        Assert.True(result.HasChanges);
        Assert.Contains("WITH (BUCKET_COUNT = 4096)", result.Script);
    }

    [Fact]
    public void TurningMemoryOptimizedOn_IsReportedAsSomethingAlterCannotDo()
    {
        var target = MemOrder();
        target.IsMemoryOptimized = false;
        target.Durability = null;

        var result = _differ.Diff(MemOrder(), target, includeDrops: true);

        Assert.True(result.RequiresRebuild);
        Assert.Contains(result.RebuildReasons, x => x.Contains("memory-optimized"));
        Assert.Contains("is memory-optimized and cannot be rebuilt", result.Script);
    }

    [Fact]
    public void AChangedDurability_IsReportedAsSomethingAlterCannotDo()
    {
        var target = MemOrder();
        target.Durability = "SCHEMA_ONLY";

        var result = _differ.Diff(MemOrder(), target, includeDrops: true);

        Assert.True(result.RequiresRebuild);
        Assert.Contains(result.RebuildReasons, x => x.Contains("DURABILITY"));
    }

    // ----------------------------------------------------- rebuild refusal

    /// <summary>
    /// A rebuild is a <c>DROP TABLE</c> with the rows carried across, and SQL Server
    /// refuses to drop a table while SYSTEM_VERSIONING is ON. The differ has to say so
    /// instead of planning a DROP that fails half way through a restore.
    /// </summary>
    [Fact]
    public void ASystemVersionedTable_IsNeverRebuilt()
    {
        var source = Employee();
        source.Columns[0] = Col("EmployeeId", nullable: false, identity: true);
        var target = Employee();

        var diff = _schemaDiffer.Diff(
            Snapshot("Src", TableObject(source, SqlRender.BuildTableCreateScript(source))),
            Snapshot("Tgt", TableObject(target, SqlRender.BuildTableCreateScript(target))),
            includeDrops: true, includeTableDrops: true, allowTableRebuild: true, addOnly: false);

        Assert.DoesNotContain("DROP TABLE", diff.Script);
        Assert.Contains("is system-versioned and cannot be rebuilt", diff.Script);
    }

    [Fact]
    public void AMemoryOptimizedTable_IsNeverRebuilt()
    {
        var source = MemOrder();
        source.Columns[0] = Col("OrderId", nullable: false, identity: true);
        var target = MemOrder();

        var diff = _schemaDiffer.Diff(
            Snapshot("Src", TableObject(source, SqlRender.BuildTableCreateScript(source))),
            Snapshot("Tgt", TableObject(target, SqlRender.BuildTableCreateScript(target))),
            includeDrops: true, includeTableDrops: true, allowTableRebuild: true, addOnly: false);

        Assert.DoesNotContain("DROP TABLE", diff.Script);
        Assert.Contains("is memory-optimized and cannot be rebuilt", diff.Script);
    }

    // ------------------------------------------------------ backward compatibility

    /// <summary>
    /// The properties this package added all default to "not a special table", so a
    /// snapshot written before them still scripts as a plain table and still compares
    /// equal to one freshly extracted from an unchanged database.
    /// </summary>
    [Fact]
    public void ATableWithoutAnyOfTheNewProperties_ScriptsExactlyAsBefore()
    {
        var table = Table("Orders", Col("Id", nullable: false), NVarchar("Code", 20));
        table.KeyConstraints.Add(Key("PK_Orders", "PK", "CLUSTERED", "Id"));
        table.Indexes.Add(Index("IX_Orders_Code", unique: false, "Code"));

        var script = SqlRender.BuildTableCreateScript(table);

        Assert.DoesNotContain("GENERATED ALWAYS", script);
        Assert.DoesNotContain("PERIOD FOR", script);
        Assert.DoesNotContain("MEMORY_OPTIMIZED", script);
        Assert.DoesNotContain("SYSTEM_VERSIONING", script);
        Assert.DoesNotContain("BUCKET_COUNT", script);
        Assert.Contains("CREATE INDEX", script.Replace("CREATE NONCLUSTERED INDEX", "CREATE INDEX"));
    }

    /// <summary>
    /// The same thing proved through the JSON, which is the actual compatibility
    /// surface: a snapshot written by 1.5 knows nothing about
    /// <c>GeneratedAlwaysType</c>, <c>IsHidden</c> or <c>BucketCount</c>, and has to
    /// load with all three at the value an ordinary table would report.
    /// </summary>
    [Fact]
    public void A15EraSnapshot_StillDeserializesAndReadsAsAnOrdinaryTable()
    {
        const string json = """
            {
              "DatabaseName": "Legacy",
              "GeneratedAtUtc": "2025-01-01T00:00:00+00:00",
              "Schemas": [],
              "Types": [],
              "Objects": [
                {
                  "Type": "Table",
                  "Schema": "dbo",
                  "Name": "Orders",
                  "Definition": "CREATE TABLE [dbo].[Orders] (...)",
                  "Dependencies": [],
                  "Table": {
                    "Schema": "dbo",
                    "Name": "Orders",
                    "Columns": [
                      {
                        "Name": "Id", "TypeSchema": "sys", "TypeName": "int", "IsUserDefinedType": false,
                        "MaxLength": 4, "Precision": 10, "Scale": 0, "IsNullable": false,
                        "IsIdentity": false, "IsComputed": false, "CollationName": null, "IsRowGuid": false
                      }
                    ],
                    "KeyConstraints": [
                      {
                        "TypeCode": "PK", "Name": "PK_Orders", "IsSystemNamed": false, "IndexTypeDesc": "CLUSTERED",
                        "Columns": [ { "Name": "Id", "KeyOrdinal": 1, "IsDescending": false, "IsIncluded": false, "IndexColumnId": 1 } ]
                      }
                    ],
                    "ForeignKeys": [],
                    "CheckConstraints": [],
                    "Indexes": [
                      {
                        "Name": "IX_Orders_Id", "IsUnique": false, "TypeDesc": "NONCLUSTERED",
                        "FilterDefinition": null, "IsDisabled": false,
                        "Columns": [ { "Name": "Id", "KeyOrdinal": 1, "IsDescending": false, "IsIncluded": false, "IndexColumnId": 1 } ]
                      }
                    ]
                  }
                }
              ]
            }
            """;

        var options = new System.Text.Json.JsonSerializerOptions
        {
            Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() }
        };
        var table = System.Text.Json.JsonSerializer
            .Deserialize<DatabaseSnapshot>(json, options)!.Objects[0].Table!;

        Assert.All(table.Columns, column =>
        {
            Assert.Equal(0, column.GeneratedAlwaysType);
            Assert.False(column.IsHidden);
        });
        Assert.Equal(0, table.KeyConstraints[0].BucketCount);
        Assert.Equal(0, table.Indexes[0].BucketCount);
        Assert.False(SqlRender.IsSystemVersioned(table));
        Assert.False(SqlRender.HasSystemTimePeriod(table));

        var script = SqlRender.BuildTableCreateScript(table);
        Assert.DoesNotContain("GENERATED ALWAYS", script);
        Assert.DoesNotContain("PERIOD FOR", script);
        Assert.DoesNotContain("BUCKET_COUNT", script);
        Assert.DoesNotContain("WITH (", script);

        // And it still reads as unchanged against itself, which is the regression
        // that matters: a new property must never turn an untouched table into drift.
        Assert.False(_differ.Diff(table, table, includeDrops: true).HasChanges);
    }

    // ---------------------------------------------------------------- fixtures

    private static ColumnModel PeriodColumn(string name, byte generatedAlwaysType, bool hidden)
    {
        var column = Col(name, "datetime2", maxLength: 8, precision: 27, scale: 7, nullable: false);
        column.GeneratedAlwaysType = generatedAlwaysType;
        column.IsHidden = hidden;
        return column;
    }

    private static TableModel Employee()
    {
        var table = Table("Employee",
            Col("EmployeeId", nullable: false),
            NVarchar("FullName", 80, nullable: false, collation: null!),
            PeriodColumn("ValidFrom", 1, hidden: true),
            PeriodColumn("ValidTo", 2, hidden: true));

        table.TemporalType = "SYSTEM_VERSIONED_TEMPORAL_TABLE";
        table.HistoryTableSchema = "dbo";
        table.HistoryTableName = "EmployeeHistory";
        table.PeriodStartColumn = "ValidFrom";
        table.PeriodEndColumn = "ValidTo";
        return table;
    }

    private static TableModel MemOrder()
    {
        var table = Table("MemOrder",
            Col("OrderId", nullable: false),
            NVarchar("Code", 40, nullable: false, collation: null!),
            Col("Total", "int"));

        table.IsMemoryOptimized = true;
        table.Durability = "SCHEMA_AND_DATA";

        var pk = Key("PK_MemOrder", "PK", "NONCLUSTERED HASH", "OrderId");
        pk.BucketCount = 1024;
        table.KeyConstraints.Add(pk);

        table.Indexes.Add(HashIndex("IX_MemOrder_Code", 512, "Code"));
        table.Indexes.Add(Index("IX_MemOrder_Total", unique: false, "Total"));
        return table;
    }

    private static IndexModel HashIndex(string name, int bucketCount, params string[] columns)
    {
        var index = Index(name, unique: false, columns);
        index.TypeDesc = "NONCLUSTERED HASH";
        index.BucketCount = bucketCount;
        return index;
    }

    private static IReadOnlyList<ScriptPhase> Compose(TableModel table, ComposeOptions options) =>
        ScriptComposer.ComposePhases(
            Snapshot("Db", TableObject(table, SqlRender.BuildTableCreateScript(table))), options);

    private static string PhaseSql(IReadOnlyList<ScriptPhase> phases, string name) =>
        string.Join(Environment.NewLine, phases.Single(x => x.Name == name).Batches.Select(x => x.Sql));

    private static void AssertOrder(string script, params string[] fragments)
    {
        var previous = -1;
        foreach(var fragment in fragments)
        {
            var index = script.IndexOf(fragment, StringComparison.Ordinal);
            Assert.True(index >= 0, $"the script never says \"{fragment}\":{NL}{script}");
            Assert.True(index > previous, $"\"{fragment}\" is out of order:{NL}{script}");
            previous = index;
        }
    }
}
