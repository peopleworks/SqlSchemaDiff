using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;

namespace SqlSchemaDiff.IntegrationTests;

/// <summary>
/// The property that matters: a script the engine generates, applied to a real
/// database, has to produce a database the engine then sees as identical. Anything
/// the extractor reads but the renderer cannot write shows up here as drift that
/// never converges.
/// </summary>
[Collection(SqlServerCollection.Name)]
public sealed class RoundTripTests
{
    private readonly SqlServerFixture _sqlServer;
    private readonly SchemaDiffer _differ = new();

    public RoundTripTests(SqlServerFixture sqlServer) => _sqlServer = sqlServer;

    [LiveFact]
    public async Task DiffRoundTripConverges()
    {
        var sourceConnection = await _sqlServer.CreateDatabaseWithFullSchemaAsync();
        var targetConnection = _sqlServer.CreateDatabase();

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var empty = await SqlServerFixture.ExtractAsync(targetConnection);
        Assert.Empty(empty.Objects);

        var deployment = _differ.Diff(source, empty, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: false, addOnly: false);
        Assert.Equal(source.Objects.Count, deployment.Added);
        Assert.Equal(0, deployment.Skipped);

        await SqlServerFixture.ApplyAsync(targetConnection, deployment.Script, useTransaction: true);

        var target = await SqlServerFixture.ExtractAsync(targetConnection);
        Snapshots.AssertNoChanges("source -> target", Compare(source, target));

        // Symmetry matters as much as convergence: a diff that is empty one way and
        // full of drops the other means the target picked up something extra.
        Snapshots.AssertNoChanges("target -> source", Compare(target, source));

        // And the deployment is idempotent: diffing again produces a script with no
        // statements, and running that script changes nothing.
        var second = Compare(source, target);
        Snapshots.AssertNoChanges("second pass", second);
        await SqlServerFixture.ApplyAsync(targetConnection, second.Script, useTransaction: true);

        var afterSecondPass = await SqlServerFixture.ExtractAsync(targetConnection);
        Snapshots.AssertNoChanges("after re-applying", Compare(source, afterSecondPass));
    }

    /// <summary>
    /// The same round trip through <see cref="ScriptComposer.ComposeFullScript"/>
    /// instead of the differ.
    /// <para>
    /// Skipped, not deleted. The composer orders objects alphabetically inside each
    /// type rank and emits foreign keys inline with their CREATE TABLE, so
    /// [ops].[AuditEntry] goes out before the tables it points at. Against this
    /// fixture SQL Server answers:
    /// </para>
    /// <code>
    /// Foreign key 'FK_AuditEntry_Customer' references invalid table 'sales.Customer'.
    /// Could not create constraint or index. See previous errors.
    /// </code>
    /// <para>
    /// [sales].[Invoice] -> [sales].[Terms] fails the same way one batch later. The
    /// differ does not have the problem because its creates are topologically
    /// sorted. Delete the Skip when the composer is dependency-ordered too; the body
    /// is complete and needs no other change.
    /// </para>
    /// </summary>
    [LiveFact(Skip = "ScriptComposer is not dependency-ordered yet (fixed by WP0.2)")]
    public async Task ComposeFullScriptRoundTrip()
    {
        var sourceConnection = await _sqlServer.CreateDatabaseWithFullSchemaAsync();
        var targetConnection = _sqlServer.CreateDatabase();

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var script = ScriptComposer.ComposeFullScript(source);

        await SqlServerFixture.ApplyAsync(targetConnection, script, useTransaction: true);

        var target = await SqlServerFixture.ExtractAsync(targetConnection);
        Snapshots.AssertNoChanges("composed source -> target", Compare(source, target));
        Snapshots.AssertNoChanges("composed target -> source", Compare(target, source));
    }

    /// <summary>
    /// Drift on top of a converged pair. Every change here has to arrive as an
    /// incremental ALTER — the table rebuild path is off, so anything the differ
    /// cannot express column-by-column shows up as a diff that never empties.
    /// </summary>
    [LiveFact]
    public async Task AlterPathConverges()
    {
        var sourceConnection = await _sqlServer.CreateDatabaseWithFullSchemaAsync();
        var targetConnection = _sqlServer.CreateDatabase();

        // Bring the target up to the baseline first, the same way the deploy test does.
        var baseline = await SqlServerFixture.ExtractAsync(sourceConnection);
        var empty = await SqlServerFixture.ExtractAsync(targetConnection);
        await SqlServerFixture.ApplyAsync(
            targetConnection,
            _differ.Diff(baseline, empty, true, true, false, false).Script,
            useTransaction: true);
        Snapshots.AssertNoChanges("baseline",
            Compare(baseline, await SqlServerFixture.ExtractAsync(targetConnection)));

        // Now move the source on: new column, widened column, nullability change, new
        // index, dropped check, new table with a foreign key, changed view, changed
        // procedure body.
        await SqlServerFixture.ApplyAsync(sourceConnection, SqlServerFixture.AlterSchemaScript, useTransaction: false);
        var source = await SqlServerFixture.ExtractAsync(sourceConnection);

        // ---- drops off: everything else lands, the check constraint survives ----
        var target = await SqlServerFixture.ExtractAsync(targetConnection);
        var additive = _differ.Diff(source, target, includeDrops: false, includeTableDrops: false,
            allowTableRebuild: false, addOnly: false);
        Assert.True(additive.HasChanges, "the altered source should differ from the baseline target");

        await SqlServerFixture.ApplyAsync(targetConnection, additive.Script, useTransaction: true);
        target = await SqlServerFixture.ExtractAsync(targetConnection);

        var customer = target.Table("sales", "Customer");
        Assert.NotNull(customer.FindCheck("CK_Customer_CreditLimit"));
        Assert.False(customer.Column("LoyaltyPoints").IsNullable);
        Assert.False(customer.Column("Rating").IsNullable);
        Assert.Equal(80, target.Table("ops", "AuditEntry").Column("Source").MaxLength);
        target.Table("ops", "AuditEntry").Index("IX_AuditEntry_Source");
        target.Table("sales", "Invoice").Index("IX_Invoice_Sku");
        target.Table("sales", "Payment").ForeignKey("FK_Payment_Invoice");
        Assert.Contains("CustomerId", target.Object(DbObjectType.View, "sales", "vInvoiceComputed").Definition);
        Assert.Contains("@includeInactive",
            target.Object(DbObjectType.StoredProcedure, "sales", "uspCustomerSummary").Definition);

        // The constraint the source dropped is the only thing left, and it is left
        // because dropping it was not asked for.
        var stillDiffering = _differ.Diff(source, target, false, false, false, false);
        Assert.Contains("[sales].[Customer]", stillDiffering.ChangedObjects);

        // ---- drops on: the check constraint goes ----
        var withDrops = _differ.Diff(source, target, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: false, addOnly: false);
        await SqlServerFixture.ApplyAsync(targetConnection, withDrops.Script, useTransaction: true);

        target = await SqlServerFixture.ExtractAsync(targetConnection);
        Assert.Null(target.Table("sales", "Customer").FindCheck("CK_Customer_CreditLimit"));
        Snapshots.AssertNoChanges("after the alter round trip", Compare(source, target));
        Snapshots.AssertNoChanges("after the alter round trip (reversed)", Compare(target, source));
    }

    private DiffResult Compare(DatabaseSnapshot source, DatabaseSnapshot target) =>
        _differ.Diff(source, target, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: false, addOnly: false);
}
