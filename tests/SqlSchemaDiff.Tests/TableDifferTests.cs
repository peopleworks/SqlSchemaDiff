using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

public class TableDifferTests
{
    private readonly TableDiffer _differ = new();

    [Fact]
    public void IdenticalTables_ProduceNoChanges()
    {
        var source = Table("Employee", Col("Id", identity: true, nullable: false), NVarchar("Name", 100));
        var target = Table("Employee", Col("Id", identity: true, nullable: false), NVarchar("Name", 100));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.False(result.HasChanges);
        Assert.Equal(string.Empty, result.Script);
    }

    [Fact]
    public void MissingColumn_GeneratesAdd()
    {
        var source = Table("Employee", Col("Id", nullable: false), NVarchar("Email", 256));
        var target = Table("Employee", Col("Id", nullable: false));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.True(result.HasChanges);
        Assert.Contains("ALTER TABLE [dbo].[Employee] ADD [Email]", result.Script);
    }

    [Fact]
    public void NewNotNullColumnWithoutDefault_EmitsWarning()
    {
        var source = Table("Employee", Col("Id", nullable: false), Col("Age", "int", nullable: false));
        var target = Table("Employee", Col("Id", nullable: false));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.True(result.WarningCount > 0);
        Assert.Contains("WARNING", result.Script);
        Assert.Contains("NOT NULL without a default", result.Script);
    }

    [Fact]
    public void NewNotNullColumnWithDefault_NoWarning()
    {
        var source = Table("Employee",
            Col("Id", nullable: false),
            Col("IsActive", "bit", nullable: false, defaultName: "DF_IsActive", defaultDefinition: "((1))"));
        var target = Table("Employee", Col("Id", nullable: false));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Equal(0, result.WarningCount);
        Assert.Contains("DEFAULT ((1))", result.Script);
    }

    [Fact]
    public void WidenedColumnType_GeneratesAlterColumn()
    {
        var source = Table("Employee", NVarchar("Name", 200));
        var target = Table("Employee", NVarchar("Name", 50));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("ALTER COLUMN [Name] nvarchar(200)", result.Script);
    }

    [Fact]
    public void NarrowedColumnType_WarnsAboutTruncation()
    {
        var source = Table("Employee", NVarchar("Name", 50));
        var target = Table("Employee", NVarchar("Name", 200));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("narrows", result.Script);
    }

    [Fact]
    public void NullableToNotNull_WarnsAndAlters()
    {
        var source = Table("Employee", NVarchar("Email", 256, nullable: false));
        var target = Table("Employee", NVarchar("Email", 256, nullable: true));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("ALTER COLUMN [Email]", result.Script);
        Assert.Contains("becomes NOT NULL", result.Script);
    }

    [Fact]
    public void TargetOnlyColumn_NotDroppedWithoutIncludeDrops()
    {
        var source = Table("Employee", Col("Id", nullable: false));
        var target = Table("Employee", Col("Id", nullable: false), Col("Legacy", "int"));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.DoesNotContain("DROP COLUMN", result.Script);
        Assert.Contains("exists only on target", result.Script);
    }

    [Fact]
    public void TargetOnlyColumn_DroppedWithIncludeDrops()
    {
        var source = Table("Employee", Col("Id", nullable: false));
        var target = Table("Employee", Col("Id", nullable: false), Col("Legacy", "int"));

        var result = _differ.Diff(source, target, includeDrops: true);

        Assert.Contains("DROP COLUMN [Legacy]", result.Script);
    }

    [Fact]
    public void MissingIndex_GeneratesCreateIndex()
    {
        var source = Table("Employee", NVarchar("Email", 256));
        source.Indexes.Add(Index("UX_Email", unique: true, "Email"));
        var target = Table("Employee", NVarchar("Email", 256));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("CREATE UNIQUE NONCLUSTERED INDEX [UX_Email]", result.Script);
    }

    [Fact]
    public void ChangedDefault_DropsAndReadds()
    {
        var source = Table("Employee",
            Col("Score", "int", nullable: false, defaultName: "DF_Score", defaultDefinition: "((5))"));
        var target = Table("Employee",
            Col("Score", "int", nullable: false, defaultName: "DF_Score_Old", defaultDefinition: "((0))"));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("DROP CONSTRAINT [DF_Score_Old]", result.Script);
        Assert.Contains("DEFAULT ((5)) FOR [Score]", result.Script);
    }

    [Fact]
    public void ChangedSparseFlag_IsADifference()
    {
        var source = Table("Employee", Col("Bonus", "int", sparse: true));
        var target = Table("Employee", Col("Bonus", "int"));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.True(result.HasChanges);
        Assert.Contains("ALTER COLUMN [Bonus] int SPARSE NULL;", result.Script);
    }

    [Fact]
    public void ChangedIndexOptions_AreADifference()
    {
        var source = Table("Employee", NVarchar("Email", 256));
        source.Indexes.Add(Index("UX_Email", unique: true, "Email"));
        source.Indexes[0].FillFactor = 90;
        var target = Table("Employee", NVarchar("Email", 256));
        target.Indexes.Add(Index("UX_Email", unique: true, "Email"));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.True(result.HasChanges);
        Assert.Contains("ALTER INDEX [UX_Email] ON [dbo].[Employee] REBUILD WITH (FILLFACTOR = 90);", result.Script);
    }

    // The index sits on the column being rewritten, so it has to come down first and
    // go back up afterwards - and it goes back up with the source's options.
    [Fact]
    public void RewrittenColumn_TakesItsIndexDownAndRestoresItWithTheNewOptions()
    {
        var source = Table("Employee", NVarchar("Email", 512));
        source.Indexes.Add(Index("UX_Email", unique: true, "Email"));
        source.Indexes[0].FillFactor = 90;
        var target = Table("Employee", NVarchar("Email", 256));
        target.Indexes.Add(Index("UX_Email", unique: true, "Email"));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("DROP INDEX [UX_Email] ON [dbo].[Employee];", result.Script);
        Assert.Contains("ALTER COLUMN [Email] nvarchar(512)", result.Script);
        Assert.Contains("CREATE UNIQUE NONCLUSTERED INDEX [UX_Email] ON [dbo].[Employee] ([Email] ASC) WITH (FILLFACTOR = 90);", result.Script);
        Assert.DoesNotContain("ALTER INDEX", result.Script);
    }
}
