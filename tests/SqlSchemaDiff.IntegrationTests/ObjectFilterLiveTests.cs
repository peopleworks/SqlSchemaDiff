using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;

namespace SqlSchemaDiff.IntegrationTests;

/// <summary>
/// A filter has to be applied to both snapshots, not just the source. Filtering
/// only the source would leave every excluded object looking target-only, and a
/// run with drops enabled would then delete exactly what you asked it to skip.
/// </summary>
[Collection(SqlServerCollection.Name)]
public sealed class ObjectFilterLiveTests
{
    private readonly SqlServerFixture _sqlServer;

    public ObjectFilterLiveTests(SqlServerFixture sqlServer) => _sqlServer = sqlServer;

    [LiveFact]
    public async Task FilterRespectsBothSides()
    {
        var sourceConnection = await _sqlServer.CreateDatabaseWithFullSchemaAsync();
        var targetConnection = _sqlServer.CreateDatabase();

        var filter = ObjectFilter.Parse("table:sales.*", null);
        var source = filter.Apply(await SqlServerFixture.ExtractAsync(sourceConnection));
        var target = filter.Apply(await SqlServerFixture.ExtractAsync(targetConnection));

        Assert.Equal(
            new[] { "Customer", "Invoice", "Terms" },
            source.Objects.Select(x => x.Name).OrderBy(x => x, StringComparer.Ordinal));
        Assert.Empty(target.Objects);

        var diff = new SchemaDiffer().Diff(source, target, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: false, addOnly: false);

        Assert.Equal(3, diff.Added);
        Assert.Equal(0, diff.Removed);
        Assert.Equal(
            new[] { "[sales].[Customer]", "[sales].[Invoice]", "[sales].[Terms]" },
            diff.AddedObjects.OrderBy(x => x, StringComparer.Ordinal));

        // Nothing outside the filter may leak into the script — not the ops tables,
        // and not the views and procedures that live in the same schema as the
        // tables that were kept.
        Assert.DoesNotContain("[ops].", diff.Script, StringComparison.Ordinal);
        Assert.DoesNotContain("vInvoiceTotals", diff.Script, StringComparison.Ordinal);
        Assert.DoesNotContain("uspCustomerSummary", diff.Script, StringComparison.Ordinal);

        // The filtered script still has to be deployable on its own: the tables it
        // keeps only reference each other, so it must apply and converge.
        await SqlServerFixture.ApplyAsync(targetConnection, diff.Script, useTransaction: true);

        var deployed = filter.Apply(await SqlServerFixture.ExtractAsync(targetConnection));
        Snapshots.AssertNoChanges(
            "filtered source -> filtered target",
            new SchemaDiffer().Diff(source, deployed, true, true, false, false));

        // And only the filtered tables were created; the ops schema was never touched.
        var everything = await SqlServerFixture.ExtractAsync(targetConnection);
        Assert.Equal(3, everything.Objects.Count);
        Assert.DoesNotContain(everything.Objects, x => x.Type != DbObjectType.Table);
        Assert.DoesNotContain(everything.Objects, x => x.Schema == "ops");
    }
}
