using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

/// <summary>
/// The <c>WITH (...)</c> clause on an index, on a PRIMARY KEY / UNIQUE constraint and
/// on a heap. Two rules run through all of it: only non-default options are scripted,
/// and a change that ALTER INDEX can make is made that way instead of by dropping the
/// index and building it again.
/// </summary>
public class IndexOptionsTests
{
    private readonly TableDiffer _differ = new();

    // ------------------------------------------------------------- rendering

    [Fact]
    public void IndexAtTheDefaults_ScriptsWithNoWithClause()
    {
        var table = Table("Orders", Col("Id", nullable: false));
        var index = Index("IX_Orders_Id", unique: false, "Id");

        var sql = SqlRender.BuildIndexCreate(table, index);

        Assert.Equal("CREATE NONCLUSTERED INDEX [IX_Orders_Id] ON [dbo].[Orders] ([Id] ASC);", sql);
    }

    [Fact]
    public void FillFactor_IsScripted()
    {
        var table = Table("Orders", Col("Id", nullable: false));
        var index = Index("IX_Orders_Id", unique: false, "Id");
        index.FillFactor = 70;

        var sql = SqlRender.BuildIndexCreate(table, index);

        Assert.Equal("CREATE NONCLUSTERED INDEX [IX_Orders_Id] ON [dbo].[Orders] ([Id] ASC) WITH (FILLFACTOR = 70);", sql);
    }

    [Fact]
    public void EveryNonDefaultOption_IsScriptedInOneClause()
    {
        var table = Table("Orders", Col("Id", nullable: false));
        var index = Index("IX_Orders_Id", unique: true, "Id");
        index.FillFactor = 90;
        index.IsPadded = true;
        index.IgnoreDupKey = true;
        index.AllowRowLocks = false;
        index.AllowPageLocks = false;
        index.DataCompression = "PAGE";

        var sql = SqlRender.BuildIndexCreate(table, index);

        Assert.Contains(
            "WITH (FILLFACTOR = 90, PAD_INDEX = ON, IGNORE_DUP_KEY = ON, " +
            "ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF, DATA_COMPRESSION = PAGE);",
            sql);
    }

    [Fact]
    public void OptionsComeAfterTheFilter()
    {
        var table = Table("Orders", Col("Id", nullable: false), Col("Status", "int"));
        var index = Index("IX_Orders_Status", unique: false, "Status");
        index.FilterDefinition = "([Status]>(0))";
        index.DataCompression = "ROW";

        var sql = SqlRender.BuildIndexCreate(table, index);

        Assert.Equal(
            "CREATE NONCLUSTERED INDEX [IX_Orders_Status] ON [dbo].[Orders] ([Status] ASC) " +
            "WHERE ([Status]>(0)) WITH (DATA_COMPRESSION = ROW);",
            sql);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("NONE")]
    public void CompressionThatMeansNone_IsNotScripted(string? compression)
    {
        var table = Table("Orders", Col("Id", nullable: false));
        var index = Index("IX_Orders_Id", unique: false, "Id");
        index.DataCompression = compression;

        Assert.DoesNotContain("DATA_COMPRESSION", SqlRender.BuildIndexCreate(table, index));
    }

    [Fact]
    public void PrimaryKeyWithOptions_CarriesThemOnTheAddConstraint()
    {
        var table = Table("Orders", Col("Id", nullable: false));
        var pk = Key("PK_Orders", "PK", "CLUSTERED", "Id");
        pk.FillFactor = 80;
        pk.IsPadded = true;
        pk.DataCompression = "PAGE";

        var sql = SqlRender.BuildKeyConstraintAdd(table, pk);

        Assert.Equal(
            "ALTER TABLE [dbo].[Orders] ADD CONSTRAINT [PK_Orders] PRIMARY KEY CLUSTERED ([Id] ASC) " +
            "WITH (FILLFACTOR = 80, PAD_INDEX = ON, DATA_COMPRESSION = PAGE);",
            sql);
    }

    [Fact]
    public void PrimaryKeyAtTheDefaults_ScriptsExactlyAsBefore()
    {
        var table = Table("Orders", Col("Id", nullable: false));

        var sql = SqlRender.BuildKeyConstraintAdd(table, Key("PK_Orders", "PK", "CLUSTERED", "Id"));

        Assert.Equal("ALTER TABLE [dbo].[Orders] ADD CONSTRAINT [PK_Orders] PRIMARY KEY CLUSTERED ([Id] ASC);", sql);
    }

    // ------------------------------------------------------------- diffing

    [Fact]
    public void IdenticalOptions_AreNotADifference()
    {
        var source = WithIndex(x => { x.FillFactor = 70; x.DataCompression = "PAGE"; });
        var target = WithIndex(x => { x.FillFactor = 70; x.DataCompression = "PAGE"; });

        Assert.False(_differ.Diff(source, target, includeDrops: false).HasChanges);
    }

    [Fact]
    public void FillFactorChange_RebuildsInsteadOfDroppingTheIndex()
    {
        var source = WithIndex(x => x.FillFactor = 80);
        var target = WithIndex(x => x.FillFactor = 50);

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Equal(1, result.ChangeCount);
        Assert.Contains("ALTER INDEX [IX_Orders_Code] ON [dbo].[Orders] REBUILD WITH (FILLFACTOR = 80);", result.Script);
        Assert.DoesNotContain("DROP INDEX", result.Script);
        Assert.DoesNotContain("CREATE NONCLUSTERED INDEX", result.Script);
    }

    // sys.indexes reports "never set" as 0, which FILLFACTOR itself will not accept.
    [Fact]
    public void FillFactorBackToTheServerDefault_IsWrittenAsOneHundred()
    {
        var source = WithIndex(_ => { });
        var target = WithIndex(x => x.FillFactor = 60);

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("REBUILD WITH (FILLFACTOR = 100);", result.Script);
    }

    [Fact]
    public void CompressionChange_RebuildsTheIndex()
    {
        var source = WithIndex(x => x.DataCompression = "PAGE");
        var target = WithIndex(x => x.DataCompression = "NONE");

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("REBUILD WITH (DATA_COMPRESSION = PAGE);", result.Script);
        Assert.DoesNotContain("DROP INDEX", result.Script);
    }

    [Fact]
    public void CompressionRemoved_RebuildsWithAnExplicitNone()
    {
        var source = WithIndex(_ => { });
        var target = WithIndex(x => x.DataCompression = "PAGE");

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("REBUILD WITH (DATA_COMPRESSION = NONE);", result.Script);
    }

    // The lock options are metadata; SET changes them without touching the pages.
    [Fact]
    public void LockOptionChange_UsesSetRatherThanRebuild()
    {
        var source = WithIndex(x => { x.AllowRowLocks = false; x.AllowPageLocks = false; });
        var target = WithIndex(_ => { });

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("ALTER INDEX [IX_Orders_Code] ON [dbo].[Orders] SET (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF);", result.Script);
        Assert.DoesNotContain("REBUILD", result.Script);
    }

    [Fact]
    public void SettableAndRebuildableChangesTogether_EmitSetThenRebuild()
    {
        var source = WithIndex(x => { x.IgnoreDupKey = true; x.FillFactor = 75; });
        var target = WithIndex(_ => { });

        var result = _differ.Diff(source, target, includeDrops: false);
        var setIndex = result.Script.IndexOf("SET (IGNORE_DUP_KEY = ON)", StringComparison.Ordinal);
        var rebuildIndex = result.Script.IndexOf("REBUILD WITH (FILLFACTOR = 75)", StringComparison.Ordinal);

        Assert.True(setIndex >= 0 && rebuildIndex > setIndex, result.Script);
    }

    [Fact]
    public void ColumnListChange_StillDropsAndRecreates()
    {
        var source = Table("Orders", Col("Id", nullable: false), NVarchar("Code", 20), NVarchar("Alt", 20));
        var sourceIndex = Index("IX_Orders_Code", unique: false, "Alt");
        sourceIndex.FillFactor = 80;
        source.Indexes.Add(sourceIndex);
        var target = WithIndex(x => x.FillFactor = 50);

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("DROP INDEX [IX_Orders_Code]", result.Script);
        Assert.Contains("CREATE NONCLUSTERED INDEX [IX_Orders_Code] ON [dbo].[Orders] ([Alt] ASC) WITH (FILLFACTOR = 80);", result.Script);
    }

    // The index has to come down anyway so the ALTER COLUMN can go through; a rebuild
    // on an index that no longer exists at that point would fail.
    [Fact]
    public void OptionChangeOnARewrittenColumn_FallsBackToDropAndCreate()
    {
        var source = Table("Orders", Col("Id", nullable: false), NVarchar("Code", 40));
        var sourceIndex = Index("IX_Orders_Code", unique: false, "Code");
        sourceIndex.FillFactor = 80;
        source.Indexes.Add(sourceIndex);
        var target = WithIndex(x => x.FillFactor = 50);

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("DROP INDEX [IX_Orders_Code]", result.Script);
        Assert.Contains("ALTER COLUMN [Code] nvarchar(40)", result.Script);
        Assert.DoesNotContain("ALTER INDEX", result.Script);
    }

    // Dropping a key constraint takes every foreign key that points at it with it.
    [Fact]
    public void KeyConstraintOptionChange_RebuildsInsteadOfDroppingTheConstraint()
    {
        var source = Table("Orders", Col("Id", nullable: false));
        var sourcePk = Key("PK_Orders", "PK", "CLUSTERED", "Id");
        sourcePk.FillFactor = 90;
        source.KeyConstraints.Add(sourcePk);

        var target = Table("Orders", Col("Id", nullable: false));
        target.KeyConstraints.Add(Key("PK_Orders", "PK", "CLUSTERED", "Id"));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Equal(1, result.ChangeCount);
        Assert.Contains("ALTER INDEX [PK_Orders] ON [dbo].[Orders] REBUILD WITH (FILLFACTOR = 90);", result.Script);
        Assert.DoesNotContain("DROP CONSTRAINT", result.Script);
    }

    // A system-named constraint has a different name on each side; the rebuild has to
    // use the target's, because that is the one that exists there.
    [Fact]
    public void SystemNamedKeyConstraint_RebuildsUnderTheTargetName()
    {
        var source = Table("Orders", Col("Id", nullable: false));
        var sourcePk = Key("PK__Orders__AAAA", "PK", "CLUSTERED", "Id");
        sourcePk.IsSystemNamed = true;
        sourcePk.DataCompression = "PAGE";
        source.KeyConstraints.Add(sourcePk);

        var target = Table("Orders", Col("Id", nullable: false));
        var targetPk = Key("PK__Orders__BBBB", "PK", "CLUSTERED", "Id");
        targetPk.IsSystemNamed = true;
        target.KeyConstraints.Add(targetPk);

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("ALTER INDEX [PK__Orders__BBBB] ON [dbo].[Orders] REBUILD WITH (DATA_COMPRESSION = PAGE);", result.Script);
    }

    // ---------------------------------------------------------- heap storage

    [Fact]
    public void CompressedHeap_ScriptsTheOptionOnCreateTable()
    {
        var table = Table("Staging", Col("Id", nullable: false));
        table.DataCompression = "PAGE";

        Assert.Contains(") WITH (DATA_COMPRESSION = PAGE);", SqlRender.BuildTableCreateScript(table));
    }

    [Fact]
    public void UncompressedHeap_ScriptsNoTableOptions()
    {
        var table = Table("Staging", Col("Id", nullable: false));
        table.DataCompression = "NONE";

        Assert.DoesNotContain("DATA_COMPRESSION", SqlRender.BuildTableCreateScript(table));
    }

    // With a clustered index the rows belong to that index, and the setting is
    // scripted on the ADD CONSTRAINT rather than twice.
    [Fact]
    public void ClusteredTable_KeepsCompressionOnTheIndexNotTheTable()
    {
        var table = Table("Orders", Col("Id", nullable: false));
        table.DataCompression = "PAGE";
        var pk = Key("PK_Orders", "PK", "CLUSTERED", "Id");
        pk.DataCompression = "PAGE";
        table.KeyConstraints.Add(pk);

        var script = SqlRender.BuildTableCreateScript(table);

        // The CREATE TABLE closes on a line of its own, with nothing after the ");".
        Assert.Contains($"{Environment.NewLine});{Environment.NewLine}", script);
        Assert.Contains("PRIMARY KEY CLUSTERED ([Id] ASC) WITH (DATA_COMPRESSION = PAGE);", script);
    }

    [Fact]
    public void HeapCompressionChange_RebuildsTheTable()
    {
        var source = Table("Staging", Col("Id", nullable: false));
        source.DataCompression = "PAGE";
        var target = Table("Staging", Col("Id", nullable: false));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Equal(1, result.ChangeCount);
        Assert.Contains("ALTER TABLE [dbo].[Staging] REBUILD WITH (DATA_COMPRESSION = PAGE);", result.Script);
    }

    [Fact]
    public void HeapCompressionRemoved_RebuildsWithAnExplicitNone()
    {
        var source = Table("Staging", Col("Id", nullable: false));
        var target = Table("Staging", Col("Id", nullable: false));
        target.DataCompression = "ROW";

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("ALTER TABLE [dbo].[Staging] REBUILD WITH (DATA_COMPRESSION = NONE);", result.Script);
    }

    [Fact]
    public void ClusteredTableCompression_DoesNotAlsoRebuildTheTable()
    {
        var source = Table("Orders", Col("Id", nullable: false));
        source.DataCompression = "PAGE";
        var sourcePk = Key("PK_Orders", "PK", "CLUSTERED", "Id");
        sourcePk.DataCompression = "PAGE";
        source.KeyConstraints.Add(sourcePk);

        var target = Table("Orders", Col("Id", nullable: false));
        target.KeyConstraints.Add(Key("PK_Orders", "PK", "CLUSTERED", "Id"));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.DoesNotContain("ALTER TABLE [dbo].[Orders] REBUILD", result.Script);
        Assert.Contains("ALTER INDEX [PK_Orders] ON [dbo].[Orders] REBUILD WITH (DATA_COMPRESSION = PAGE);", result.Script);
    }

    // Dropping the clustered index leaves a heap behind, and that heap inherits the
    // index's compression, so it still has to be rebuilt afterwards.
    [Fact]
    public void DroppingTheClusteredIndex_StillRebuildsTheHeapItLeavesBehind()
    {
        var source = Table("Orders", Col("Id", nullable: false));

        var target = Table("Orders", Col("Id", nullable: false));
        target.DataCompression = "PAGE";
        var targetPk = Key("PK_Orders", "PK", "CLUSTERED", "Id");
        targetPk.DataCompression = "PAGE";
        target.KeyConstraints.Add(targetPk);

        var result = _differ.Diff(source, target, includeDrops: true);
        var dropIndex = result.Script.IndexOf("DROP CONSTRAINT [PK_Orders]", StringComparison.Ordinal);
        var rebuildIndex = result.Script.IndexOf("ALTER TABLE [dbo].[Orders] REBUILD WITH (DATA_COMPRESSION = NONE);", StringComparison.Ordinal);

        Assert.True(dropIndex >= 0 && rebuildIndex > dropIndex, result.Script);
    }

    private static TableModel WithIndex(Action<IndexModel> configure)
    {
        var table = Table("Orders", Col("Id", nullable: false), NVarchar("Code", 20));
        var index = Index("IX_Orders_Code", unique: false, "Code");
        configure(index);
        table.Indexes.Add(index);
        return table;
    }
}
