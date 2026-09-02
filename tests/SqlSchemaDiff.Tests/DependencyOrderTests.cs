using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;

namespace SqlSchemaDiff.Tests;

public class DependencyOrderTests
{
    [Fact]
    public void Sort_PutsEveryDependencyBeforeItsDependent()
    {
        var nodes = new[]
        {
            Node("a", "b"),
            Node("b", "c"),
            Node("c")
        };

        var order = Sort(nodes);

        Assert.Equal(new[] { "c", "b", "a" }, Keys(order.Ordered));
        Assert.False(order.HasCycle);
    }

    [Fact]
    public void Sort_IgnoresDependenciesOnObjectsOutsideTheSet()
    {
        // The referenced object already exists on the target, or a filter excluded
        // it. Either way it cannot constrain an order we are not producing for it.
        var nodes = new[]
        {
            Node("a", "not-in-the-set"),
            Node("b", "also-missing")
        };

        var order = Sort(nodes);

        Assert.Equal(new[] { "a", "b" }, Keys(order.Ordered));
        Assert.False(order.HasCycle);
    }

    [Fact]
    public void Sort_IsIndependentOfTheInputOrder()
    {
        var nodes = new[] { Node("a", "b"), Node("b", "c"), Node("c"), Node("d") };

        var forward = Keys(Sort(nodes).Ordered);
        var backward = Keys(Sort(nodes.Reverse().ToArray()).Ordered);

        Assert.Equal(forward, backward);
    }

    [Fact]
    public void Sort_BreaksTiesByRankThenKey()
    {
        var nodes = new[]
        {
            Node("zebra", rank: 1),
            Node("apple", rank: 2),
            Node("mango", rank: 1)
        };

        var order = Sort(nodes);

        Assert.Equal(new[] { "mango", "zebra", "apple" }, Keys(order.Ordered));
    }

    [Fact]
    public void Sort_BreaksACycle_WithoutThrowing_AndReportsItsMembers()
    {
        var nodes = new[] { Node("a", "b"), Node("b", "a"), Node("c") };

        var order = Sort(nodes);

        Assert.True(order.HasCycle);
        Assert.Equal(new[] { "a", "b" }, Keys(order.CycleMembers));
        // Everything is still emitted, with the cycle last.
        Assert.Equal(new[] { "c", "a", "b" }, Keys(order.Ordered));
    }

    [Fact]
    public void Sort_BreaksACycleDeterministically()
    {
        var nodes = new[] { Node("a", "b"), Node("b", "a") };

        var forward = Keys(Sort(nodes).Ordered);
        var backward = Keys(Sort(nodes.Reverse().ToArray()).Ordered);

        Assert.Equal(forward, backward);
    }

    [Fact]
    public void Sort_ReportsObjectsDownstreamOfACycle_AsUnplaceable()
    {
        var nodes = new[] { Node("a", "b"), Node("b", "a"), Node("c", "a") };

        var order = Sort(nodes);

        Assert.Equal(new[] { "a", "b", "c" }, Keys(order.CycleMembers));
    }

    [Fact]
    public void Sort_IgnoresSelfDependencies()
    {
        var nodes = new[] { Node("a", "a") };

        var order = Sort(nodes);

        Assert.False(order.HasCycle);
        Assert.Equal(new[] { "a" }, Keys(order.Ordered));
    }

    [Fact]
    public void Sort_KeepsTheFirstOfTwoItemsWithTheSameKey()
    {
        var nodes = new[] { new TestNode("a", 1, Array.Empty<string>()), new TestNode("A", 9, Array.Empty<string>()) };

        var order = Sort(nodes);

        Assert.Single(order.Ordered);
        Assert.Equal(1, order.Ordered[0].Rank);
    }

    [Fact]
    public void Sort_OfSchemaObjects_LetsADependencyBeatTheRank()
    {
        // A function that reads a view has to come after it, even though functions
        // rank ahead of views.
        var view = new DbSchemaObject { Type = DbObjectType.View, Schema = "dbo", Name = "vBase" };
        var function = new DbSchemaObject
        {
            Type = DbObjectType.Function,
            Schema = "dbo",
            Name = "fnReadsTheView",
            Dependencies = { "View:dbo.vBase" }
        };

        var order = DependencyOrder.Sort(
            new[] { function, view },
            x => x.Type == DbObjectType.Function ? 1 : 2);

        Assert.Equal(new[] { "View:dbo.vBase", "Function:dbo.fnReadsTheView" }, order.Ordered.Select(x => x.Key));
    }

    [Fact]
    public void Sort_OfAnEmptySet_IsEmpty()
    {
        var order = Sort(Array.Empty<TestNode>());

        Assert.Empty(order.Ordered);
        Assert.False(order.HasCycle);
    }

    private static DependencyOrderResult<TestNode> Sort(IEnumerable<TestNode> nodes) =>
        DependencyOrder.Sort(nodes, x => x.Key, x => x.Dependencies, x => x.Rank);

    private static string[] Keys(IEnumerable<TestNode> nodes) => nodes.Select(x => x.Key).ToArray();

    private static TestNode Node(string key, params string[] dependencies) => new(key, 0, dependencies);

    private static TestNode Node(string key, int rank) => new(key, rank, Array.Empty<string>());

    private sealed record TestNode(string Key, int Rank, string[] Dependencies);
}
