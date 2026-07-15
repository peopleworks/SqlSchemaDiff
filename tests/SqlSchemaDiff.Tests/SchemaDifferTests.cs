using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

public class SchemaDifferTests
{
    private readonly SchemaDiffer _differ = new();

    [Fact]
    public void NewTable_IsCreated()
    {
        var source = Snapshot("Src",
            TableObject(Table("Employee", Col("Id", nullable: false))),
            TableObject(Table("Department", Col("Id", nullable: false))));
        var target = Snapshot("Tgt",
            TableObject(Table("Employee", Col("Id", nullable: false))));

        var result = _differ.Diff(source, target, includeDrops: false, includeTableDrops: false, allowTableRebuild: false, addOnly: false);

        Assert.Equal(1, result.Added);
        Assert.Contains("CREATE TABLE", result.Script);
    }

    [Fact]
    public void ChangedTable_ProducesColumnLevelAlter_NotSkip()
    {
        var source = Snapshot("Src",
            TableObject(Table("Employee", Col("Id", nullable: false), NVarchar("Email", 256)), "def-source"));
        var target = Snapshot("Tgt",
            TableObject(Table("Employee", Col("Id", nullable: false)), "def-target"));

        var result = _differ.Diff(source, target, includeDrops: false, includeTableDrops: false, allowTableRebuild: false, addOnly: false);

        Assert.Equal(1, result.Changed);
        Assert.Equal(0, result.Skipped);
        Assert.Contains("ALTER TABLE [dbo].[Employee] ADD [Email]", result.Script);
    }

    [Fact]
    public void AddOnly_SkipsChangedTable()
    {
        var source = Snapshot("Src",
            TableObject(Table("Employee", Col("Id", nullable: false), NVarchar("Email", 256)), "def-source"));
        var target = Snapshot("Tgt",
            TableObject(Table("Employee", Col("Id", nullable: false)), "def-target"));

        var result = _differ.Diff(source, target, includeDrops: false, includeTableDrops: false, allowTableRebuild: false, addOnly: true);

        Assert.True(result.Skipped >= 1);
        Assert.DoesNotContain("ALTER COLUMN", result.Script);
    }

    [Fact]
    public void ProgrammableCreate_IsIsolatedFromComments()
    {
        // Regression guard: a CREATE VIEW/PROC/FUNCTION must be in its own batch with
        // no leading comment, or SQL Server bakes the comment into sys.sql_modules and
        // the object diffs forever.
        var view = new DbSchemaObject
        {
            Type = DbObjectType.View,
            Schema = "dbo",
            Name = "vTest",
            Definition = "CREATE VIEW dbo.vTest AS SELECT 1 AS X"
        };
        var source = Snapshot("Src", view);
        var target = Snapshot("Tgt");

        var result = _differ.Diff(source, target, includeDrops: false, includeTableDrops: false, allowTableRebuild: false, addOnly: false);

        var batches = SqlBatchExecutor.SplitBatches(result.Script);
        var createBatch = batches.Single(b => b.Contains("CREATE VIEW", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain("--", createBatch);
    }

    [Fact]
    public void ColumnOrderOnlyDifference_IsNotReportedAsChanged()
    {
        // Same columns, different physical order → structurally identical → not drift.
        var source = Snapshot("Src",
            TableObject(Table("T", Col("A", nullable: false), Col("B"), Col("C")), "def-a"));
        var target = Snapshot("Tgt",
            TableObject(Table("T", Col("B"), Col("A", nullable: false), Col("C")), "def-b"));

        var result = _differ.Diff(source, target, includeDrops: false, includeTableDrops: false, allowTableRebuild: false, addOnly: false);

        Assert.Equal(0, result.Changed);
        Assert.Empty(result.ChangedObjects);
    }

    [Fact]
    public void DiffResult_ReportsChangedObjectNames()
    {
        var source = Snapshot("Src",
            TableObject(Table("Employee", Col("Id", nullable: false), NVarchar("Email", 256)), "def-source"));
        var target = Snapshot("Tgt",
            TableObject(Table("Employee", Col("Id", nullable: false)), "def-target"));

        var result = _differ.Diff(source, target, includeDrops: false, includeTableDrops: false, allowTableRebuild: false, addOnly: false);

        Assert.Contains("[dbo].[Employee]", result.ChangedObjects);
    }

    [Fact]
    public void LegacySnapshotWithoutModel_FallsBackToSkip()
    {
        // Table objects without a structured Table model (Table == null) but different definitions.
        var source = Snapshot("Src", new DbSchemaObject
        {
            Type = DbObjectType.Table, Schema = "dbo", Name = "Employee", Definition = "CREATE TABLE A"
        });
        var target = Snapshot("Tgt", new DbSchemaObject
        {
            Type = DbObjectType.Table, Schema = "dbo", Name = "Employee", Definition = "CREATE TABLE B"
        });

        var result = _differ.Diff(source, target, includeDrops: false, includeTableDrops: false, allowTableRebuild: false, addOnly: false);

        Assert.Equal(1, result.Skipped);
        Assert.Contains("was skipped", result.Script);
    }
}
