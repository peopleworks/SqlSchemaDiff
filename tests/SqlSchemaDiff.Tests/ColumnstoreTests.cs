using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

/// <summary>
/// Columnstore indexes use a grammar of their own: no sort order on the columns, no
/// UNIQUE or INCLUDE, none of the B-tree storage options, and no column list at all
/// when the index is clustered.
/// </summary>
public class ColumnstoreTests
{
    private readonly TableDiffer _differ = new();

    [Fact]
    public void ClusteredColumnstore_ScriptsWithNoColumnList()
    {
        var table = Table("Facts", Col("Id", nullable: false), Col("Measure", "int"));

        var sql = SqlRender.BuildIndexCreate(table, Columnstore("CCI_Facts", clustered: true));

        Assert.Equal("CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Facts] ON [dbo].[Facts];", sql);
    }

    // sys.index_columns marks every columnstore column as "included" because none of
    // them is a key; they all belong in the column list regardless.
    [Fact]
    public void NonclusteredColumnstore_ListsItsColumnsWithoutASortOrder()
    {
        var table = Table("Facts", Col("Id", nullable: false), Col("A", "int"), Col("B", "int"));

        var sql = SqlRender.BuildIndexCreate(table, Columnstore("NCCI_Facts", clustered: false, "A", "B"));

        Assert.Equal("CREATE NONCLUSTERED COLUMNSTORE INDEX [NCCI_Facts] ON [dbo].[Facts] ([A], [B]);", sql);
        Assert.DoesNotContain("ASC", sql);
        Assert.DoesNotContain("INCLUDE", sql);
    }

    [Fact]
    public void FilteredNonclusteredColumnstore_KeepsItsFilter()
    {
        var table = Table("Facts", Col("Id", nullable: false), Col("A", "int"));
        var index = Columnstore("NCCI_Facts", clustered: false, "A");
        index.FilterDefinition = "([A]>(0))";

        var sql = SqlRender.BuildIndexCreate(table, index);

        Assert.Equal("CREATE NONCLUSTERED COLUMNSTORE INDEX [NCCI_Facts] ON [dbo].[Facts] ([A]) WHERE ([A]>(0));", sql);
    }

    // A columnstore index reports allow_row_locks and allow_page_locks as 0, and
    // SQL Server rejects both keywords on one. Scripting them would be a hard error.
    [Fact]
    public void Columnstore_OmitsTheOptionsSqlServerRejectsOnOne()
    {
        var table = Table("Facts", Col("Id", nullable: false), Col("A", "int"));
        var index = Columnstore("NCCI_Facts", clustered: false, "A");
        index.FillFactor = 80;
        index.IsPadded = true;
        index.IgnoreDupKey = true;

        var sql = SqlRender.BuildIndexCreate(table, index);

        Assert.DoesNotContain("ALLOW_ROW_LOCKS", sql);
        Assert.DoesNotContain("ALLOW_PAGE_LOCKS", sql);
        Assert.DoesNotContain("FILLFACTOR", sql);
        Assert.DoesNotContain("PAD_INDEX", sql);
        Assert.DoesNotContain("IGNORE_DUP_KEY", sql);
    }

    // COLUMNSTORE is what an uncompressed columnstore index reports, so it is the
    // default and only the archive setting is worth scripting.
    [Fact]
    public void ColumnstoreCompression_ScriptsOnlyTheArchiveSetting()
    {
        var table = Table("Facts", Col("Id", nullable: false), Col("A", "int"));
        var plain = Columnstore("NCCI_Plain", clustered: false, "A");
        plain.DataCompression = "COLUMNSTORE";
        var archive = Columnstore("NCCI_Archive", clustered: false, "A");
        archive.DataCompression = "COLUMNSTORE_ARCHIVE";

        Assert.DoesNotContain("DATA_COMPRESSION", SqlRender.BuildIndexCreate(table, plain));
        Assert.EndsWith("([A]) WITH (DATA_COMPRESSION = COLUMNSTORE_ARCHIVE);", SqlRender.BuildIndexCreate(table, archive));
    }

    [Fact]
    public void ColumnstoreIndex_IsDroppedLikeAnyOther()
    {
        var table = Table("Facts", Col("Id", nullable: false));

        var sql = SqlRender.BuildIndexDrop(table, Columnstore("CCI_Facts", clustered: true));

        Assert.Equal("DROP INDEX [CCI_Facts] ON [dbo].[Facts];", sql);
    }

    // A primary key on a clustered-columnstore table is necessarily nonclustered, and
    // the table is not a heap either, so its compression stays on the index.
    [Fact]
    public void ClusteredColumnstoreTable_TakesANonclusteredPrimaryKeyAndIsNotAHeap()
    {
        var table = Table("Facts", Col("Id", nullable: false), Col("Measure", "int"));
        table.KeyConstraints.Add(Key("PK_Facts", "PK", "NONCLUSTERED", "Id"));
        table.Indexes.Add(Columnstore("CCI_Facts", clustered: true));
        table.DataCompression = "COLUMNSTORE";

        var script = SqlRender.BuildTableCreateScript(table);

        Assert.False(SqlRender.IsHeap(table));
        Assert.Contains("PRIMARY KEY NONCLUSTERED ([Id] ASC);", script);
        Assert.DoesNotContain("PRIMARY KEY CLUSTERED", script);
        Assert.DoesNotContain(") WITH (DATA_COMPRESSION", script);
        Assert.Contains("CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Facts] ON [dbo].[Facts];", script);
    }

    [Fact]
    public void MissingColumnstoreIndex_IsCreated()
    {
        var source = Table("Facts", Col("Id", nullable: false), Col("A", "int"));
        source.Indexes.Add(Columnstore("NCCI_Facts", clustered: false, "A"));
        var target = Table("Facts", Col("Id", nullable: false), Col("A", "int"));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("CREATE NONCLUSTERED COLUMNSTORE INDEX [NCCI_Facts] ON [dbo].[Facts] ([A]);", result.Script);
    }

    [Fact]
    public void ColumnstoreCompressionChange_RebuildsInsteadOfDropping()
    {
        var source = WithColumnstore("COLUMNSTORE_ARCHIVE");
        var target = WithColumnstore("COLUMNSTORE");

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Equal(1, result.ChangeCount);
        Assert.Contains("ALTER INDEX [NCCI_Facts] ON [dbo].[Facts] REBUILD WITH (DATA_COMPRESSION = COLUMNSTORE_ARCHIVE);", result.Script);
        Assert.DoesNotContain("DROP INDEX", result.Script);
    }

    // Going back to plain columnstore has to name it: there is no "no compression"
    // for a columnstore index.
    [Fact]
    public void ColumnstoreCompressionRemoved_RebuildsWithAnExplicitColumnstore()
    {
        var source = WithColumnstore(null);
        var target = WithColumnstore("COLUMNSTORE_ARCHIVE");

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("REBUILD WITH (DATA_COMPRESSION = COLUMNSTORE);", result.Script);
    }

    [Fact]
    public void ClusteredAndNonclusteredColumnstore_AreDifferentIndexes()
    {
        var source = Table("Facts", Col("Id", nullable: false));
        source.Indexes.Add(Columnstore("CS_Facts", clustered: true));
        var target = Table("Facts", Col("Id", nullable: false));
        target.Indexes.Add(Columnstore("CS_Facts", clustered: false));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.True(result.HasChanges);
        Assert.Contains("DROP INDEX [CS_Facts]", result.Script);
        Assert.Contains("CREATE CLUSTERED COLUMNSTORE INDEX [CS_Facts]", result.Script);
    }

    private static TableModel WithColumnstore(string? compression)
    {
        var table = Table("Facts", Col("Id", nullable: false), Col("A", "int"));
        var index = Columnstore("NCCI_Facts", clustered: false, "A");
        index.DataCompression = compression;
        table.Indexes.Add(index);
        return table;
    }
}
