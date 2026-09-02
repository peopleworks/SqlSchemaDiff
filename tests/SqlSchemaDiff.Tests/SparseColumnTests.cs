using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

/// <summary>
/// SPARSE is a column property with two quirks the renderer has to respect: it sits
/// between COLLATE and the nullability, and any ALTER COLUMN that leaves it out
/// silently clears it.
/// </summary>
public class SparseColumnTests
{
    private readonly TableDiffer _differ = new();

    [Fact]
    public void SparseColumn_PutsTheKeywordBetweenCollateAndNullability()
    {
        var column = NVarchar("Note", 100);
        column.IsSparse = true;

        var sql = SqlRender.BuildColumnDefinition(column);

        Assert.Equal("[Note] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS SPARSE NULL", sql);
    }

    [Fact]
    public void SparseColumnWithoutCollation_StillRendersSparseBeforeNullability()
    {
        var sql = SqlRender.BuildColumnDefinition(Col("Amount", "int", sparse: true));

        Assert.Equal("[Amount] int SPARSE NULL", sql);
    }

    [Fact]
    public void OrdinaryColumn_DoesNotMentionSparse()
    {
        var sql = SqlRender.BuildColumnDefinition(Col("Amount", "int"));

        Assert.DoesNotContain("SPARSE", sql);
    }

    [Fact]
    public void AddedSparseColumn_CarriesTheKeyword()
    {
        var source = Table("Wide", Col("Id", nullable: false), Col("Extra", "int", sparse: true));
        var target = Table("Wide", Col("Id", nullable: false));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("ADD [Extra] int SPARSE NULL;", result.Script);
    }

    [Fact]
    public void TurningSparseOn_RewritesTheColumnWithTheKeyword()
    {
        var source = Table("Wide", Col("Amount", "int", sparse: true));
        var target = Table("Wide", Col("Amount", "int"));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.True(result.HasChanges);
        Assert.Contains("ALTER TABLE [dbo].[Wide] ALTER COLUMN [Amount] int SPARSE NULL;", result.Script);
        Assert.DoesNotContain("DROP SPARSE", result.Script);
    }

    [Fact]
    public void TurningSparseOff_EmitsDropSparse()
    {
        var source = Table("Wide", Col("Amount", "int"));
        var target = Table("Wide", Col("Amount", "int", sparse: true));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.True(result.HasChanges);
        Assert.Contains("ALTER TABLE [dbo].[Wide] ALTER COLUMN [Amount] DROP SPARSE;", result.Script);
    }

    // An ALTER COLUMN that restates the type without SPARSE clears the flag, so a
    // column that stays sparse across a type change has to say so again.
    [Fact]
    public void RewritingASparseColumn_RestatesSparse()
    {
        var source = Table("Wide", NVarchar("Note", 200));
        source.Columns[0].IsSparse = true;
        var target = Table("Wide", NVarchar("Note", 100));
        target.Columns[0].IsSparse = true;

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("ALTER COLUMN [Note] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS SPARSE NULL;", result.Script);
        Assert.DoesNotContain("DROP SPARSE", result.Script);
    }

    [Fact]
    public void RewritingAColumnThatLosesSparse_AlsoDropsItExplicitly()
    {
        var source = Table("Wide", NVarchar("Note", 200));
        var target = Table("Wide", NVarchar("Note", 100));
        target.Columns[0].IsSparse = true;

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("ALTER COLUMN [Note] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL;", result.Script);
        Assert.Contains("ALTER COLUMN [Note] DROP SPARSE;", result.Script);
    }

    [Fact]
    public void SparseAndNotNull_Warns()
    {
        var source = Table("Wide", Col("Amount", "int", nullable: false, sparse: true));
        var target = Table("Wide", Col("Amount", "int", nullable: false));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("SPARSE but NOT NULL", result.Script);
        Assert.True(result.WarningCount > 0);
    }

    [Fact]
    public void UnchangedSparseColumn_IsNotADifference()
    {
        var source = Table("Wide", Col("Amount", "int", sparse: true));
        var target = Table("Wide", Col("Amount", "int", sparse: true));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.False(result.HasChanges);
        Assert.Equal(string.Empty, result.Script);
    }
}
