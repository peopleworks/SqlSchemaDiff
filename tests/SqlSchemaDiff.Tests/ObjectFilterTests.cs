using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

public class ObjectFilterTests
{
    private static DbSchemaObject Obj(DbObjectType type, string schema, string name) => new()
    {
        Type = type,
        Schema = schema,
        Name = name,
        Definition = $"CREATE {type} [{schema}].[{name}]"
    };

    [Fact]
    public void NoPatterns_KeepsEverything()
    {
        var filter = ObjectFilter.Parse(null, null);

        Assert.True(filter.IsEmpty);
        Assert.True(filter.ShouldInclude(Obj(DbObjectType.Table, "dbo", "Anything")));
    }

    [Theory]
    [InlineData("Sales.*", "Sales", "Orders", true)]
    [InlineData("Sales.*", "dbo", "Orders", false)]
    [InlineData("usp_Temp*", "dbo", "usp_TempFix", true)]
    [InlineData("usp_Temp*", "dbo", "usp_GetOrders", false)]
    [InlineData("dbo.T?", "dbo", "T1", true)]
    [InlineData("dbo.T?", "dbo", "T10", false)]
    [InlineData("ORDERS", "dbo", "orders", true)]
    public void IncludeMatchesQualifiedOrBareName(string pattern, string schema, string name, bool expected)
    {
        var filter = ObjectFilter.Parse(pattern, null);

        Assert.Equal(expected, filter.ShouldInclude(Obj(DbObjectType.Table, schema, name)));
    }

    [Fact]
    public void TypePrefixNarrowsToOneKind()
    {
        var filter = ObjectFilter.Parse("proc:*", null);

        Assert.True(filter.ShouldInclude(Obj(DbObjectType.StoredProcedure, "dbo", "usp_Get")));
        Assert.False(filter.ShouldInclude(Obj(DbObjectType.Table, "dbo", "usp_Get")));
    }

    [Fact]
    public void ABareTypePrefixMeansEveryObjectOfThatKind()
    {
        var filter = ObjectFilter.Parse(null, "view:");

        Assert.False(filter.ShouldInclude(Obj(DbObjectType.View, "dbo", "vAnything")));
        Assert.True(filter.ShouldInclude(Obj(DbObjectType.Table, "dbo", "vAnything")));
    }

    [Fact]
    public void ExcludeIsAppliedAfterInclude()
    {
        var filter = ObjectFilter.Parse("dbo.*", "dbo.Audit*");

        Assert.True(filter.ShouldInclude(Obj(DbObjectType.Table, "dbo", "Customer")));
        Assert.False(filter.ShouldInclude(Obj(DbObjectType.Table, "dbo", "AuditTrail")));
        Assert.False(filter.ShouldInclude(Obj(DbObjectType.Table, "Sales", "Customer")));
    }

    [Fact]
    public void CommasSeparateSeveralPatterns()
    {
        var filter = ObjectFilter.Parse("Sales.*, dbo.Customer", null);

        Assert.True(filter.ShouldInclude(Obj(DbObjectType.Table, "Sales", "Orders")));
        Assert.True(filter.ShouldInclude(Obj(DbObjectType.Table, "dbo", "Customer")));
        Assert.False(filter.ShouldInclude(Obj(DbObjectType.Table, "dbo", "Invoice")));
    }

    [Fact]
    public void ANameThatLooksLikeAGlobIsNotTreatedAsOne()
    {
        // Regex metacharacters in an object name must be matched literally.
        var filter = ObjectFilter.Parse("dbo.Order+Item", null);

        Assert.True(filter.ShouldInclude(Obj(DbObjectType.Table, "dbo", "Order+Item")));
        Assert.False(filter.ShouldInclude(Obj(DbObjectType.Table, "dbo", "OrderItem")));
    }

    // The important one: a filter has to hide an object from BOTH sides. Filtering
    // only the source would leave it looking target-only, and --include-drops would
    // then delete the very thing the filter asked to leave alone.
    [Fact]
    public void AnExcludedObjectIsNeverDropped_EvenWithIncludeDrops()
    {
        var keep = TableObject(Table("Customer", Col("Id", nullable: false)));
        var skip = TableObject(Table("AuditTrail", Col("Id", nullable: false)));

        var filter = ObjectFilter.Parse(null, "dbo.Audit*");
        var source = filter.Apply(Snapshot("Source", keep));
        var target = filter.Apply(Snapshot("Target", keep, skip));

        var result = new SchemaDiffer().Diff(source, target, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: false, addOnly: false);

        Assert.Equal(0, result.Removed);
        Assert.DoesNotContain("AuditTrail", result.Script);
    }

    [Fact]
    public void ApplyLeavesTheSnapshotAloneWhenThereIsNoFilter()
    {
        var snapshot = Snapshot("Db", TableObject(Table("T", Col("Id"))));

        Assert.Same(snapshot, ObjectFilter.Parse(null, null).Apply(snapshot));
    }

    [Fact]
    public void ApplyKeepsSchemasAndTypesSoPrerequisitesStillResolve()
    {
        var table = Table("Customer", Col("Id", nullable: false));
        table.Schema = "app";
        var snapshot = Snapshot("Db", TableObject(table));
        snapshot.Schemas.Add("app");

        var filtered = ObjectFilter.Parse("app.*", null).Apply(snapshot);

        Assert.Single(filtered.Objects);
        Assert.Contains("app", filtered.Schemas);
    }
}
