using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;

namespace SqlSchemaDiff.IntegrationTests;

/// <summary>
/// The two things about a rebuild that only a real server can settle: that the rows
/// are still there afterwards, and that the state of the constraints around them is
/// what the source says it is. A generated script can be read and still be wrong —
/// <c>sp_rename</c> on a constraint, <c>SET IDENTITY_INSERT</c> across batches, an
/// inbound foreign key standing in the way of a <c>DROP TABLE</c> — so this runs it.
/// </summary>
[Collection(SqlServerCollection.Name)]
public sealed class TableRebuildLiveTests
{
    private readonly SqlServerFixture _sqlServer;
    private readonly SchemaDiffer _differ = new();

    public TableRebuildLiveTests(SqlServerFixture sqlServer) => _sqlServer = sqlServer;

    /// <summary>
    /// A populated table with an inbound foreign key and a trigger, and an identity
    /// change on its key. Before 1.6 <c>--allow-table-rebuild</c> answered this with
    /// <c>DROP TABLE</c> — which would not even have run, because of the foreign key,
    /// and would have taken every row with it if it had.
    /// </summary>
    [LiveFact]
    public async Task RebuildPreservesRows()
    {
        var sourceConnection = _sqlServer.CreateDatabase();
        await SqlServerFixture.ApplyAsync(sourceConnection, SqlServerFixture.RebuildAfterScript, useTransaction: false);

        var targetConnection = _sqlServer.CreateDatabase();
        await SqlServerFixture.ApplyAsync(targetConnection, SqlServerFixture.RebuildBeforeScript, useTransaction: false);

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var target = await SqlServerFixture.ExtractAsync(targetConnection);

        // Without the flag the identity change is reported and refused, which is the
        // behaviour the flag exists to opt out of.
        var refused = _differ.Diff(source, target, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: false, addOnly: false);
        Assert.Contains("Manual table rebuild required", refused.Script);
        Assert.DoesNotContain("DROP TABLE", refused.Script);

        var rebuild = _differ.Diff(source, target, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: true, addOnly: false);
        Assert.Contains("REBUILD [inv].[Widget]", rebuild.Script);

        // One transaction: between the DROP and the rename the table does not exist.
        var applied = await SqlServerFixture.ApplyAsync(targetConnection, rebuild.Script, useTransaction: true);
        Assert.True(applied.Transactional);
        Assert.False(applied.RolledBack);

        await AssertRowsSurvived(targetConnection);

        var rebuilt = await SqlServerFixture.ExtractAsync(targetConnection);
        var widget = rebuilt.Table("inv", "Widget");
        Assert.True(widget.Column("WidgetId").IsIdentity, "the rebuild was supposed to add the identity");
        Assert.False(widget.Column("Colour").IsNullable);
        widget.Key("UQ_Widget_Sku");
        widget.Check("CK_Widget_Price");
        widget.Index("IX_Widget_Sku");

        // The foreign key that had to come down for the DROP is back, and so is the
        // trigger DROP TABLE took with it.
        rebuilt.Table("inv", "WidgetLog").ForeignKey("FK_WidgetLog_Widget");
        rebuilt.Object(DbObjectType.Trigger, "inv", "trWidgetTouch");

        // Back, and working: the trigger writes a log row and the foreign key accepts it.
        await SqlServerFixture.ApplyAsync(targetConnection,
            "UPDATE inv.Widget SET Weight = 9 WHERE WidgetId = 2;", useTransaction: true);
        Assert.Equal(2, await SqlServerFixture.ScalarAsync<int>(targetConnection, "SELECT COUNT(*) FROM inv.WidgetLog;"));

        // And the identity picks up from the rows that were copied in, rather than
        // handing out a key one of them already has.
        await SqlServerFixture.ApplyAsync(targetConnection,
            "INSERT INTO inv.Widget (Sku, Price) VALUES ('W-004', 40.00);", useTransaction: true);
        Assert.Equal(4, await SqlServerFixture.ScalarAsync<int>(targetConnection,
            "SELECT WidgetId FROM inv.Widget WHERE Sku = 'W-004';"));

        Snapshots.AssertNoChanges("after the rebuild", Compare(source, rebuilt));
        Snapshots.AssertNoChanges("after the rebuild (reversed)", Compare(rebuilt, source));

        // Running it again is a no-op: nothing to do, and nothing lost by doing it.
        var second = _differ.Diff(source, rebuilt, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: true, addOnly: false);
        Snapshots.AssertNoChanges("second pass", second);
        await SqlServerFixture.ApplyAsync(targetConnection, second.Script, useTransaction: true);
        Assert.Equal(4, await SqlServerFixture.ScalarAsync<int>(targetConnection, "SELECT COUNT(*) FROM inv.Widget;"));
    }

    /// <summary>
    /// Nothing about a rebuild is worth having if the rows are not there afterwards,
    /// so this checks the copy itself rather than the catalog: the count, a value the
    /// key was carried by, a NULL that stayed NULL, and the default that filled in a
    /// column the old table did not have.
    /// </summary>
    private static async Task AssertRowsSurvived(string connectionString)
    {
        Assert.Equal(3, await SqlServerFixture.ScalarAsync<int>(connectionString, "SELECT COUNT(*) FROM inv.Widget;"));
        Assert.Equal("W-002", await SqlServerFixture.ScalarAsync<string>(connectionString,
            "SELECT Sku FROM inv.Widget WHERE WidgetId = 2;"));
        Assert.Equal(30.25m, await SqlServerFixture.ScalarAsync<decimal>(connectionString,
            "SELECT Price FROM inv.Widget WHERE WidgetId = 3;"));
        Assert.Equal(60.50m, await SqlServerFixture.ScalarAsync<decimal>(connectionString,
            "SELECT Doubled FROM inv.Widget WHERE WidgetId = 3;"));
        Assert.Null(await SqlServerFixture.ScalarAsync<int?>(connectionString,
            "SELECT Weight FROM inv.Widget WHERE WidgetId = 2;"));
        Assert.Equal("grey", await SqlServerFixture.ScalarAsync<string>(connectionString,
            "SELECT Colour FROM inv.Widget WHERE WidgetId = 1;"));
        Assert.Equal(1, await SqlServerFixture.ScalarAsync<int>(connectionString, "SELECT COUNT(*) FROM inv.WidgetLog;"));
    }

    /// <summary>
    /// Whether a constraint is switched on, and whether its rows were ever validated,
    /// is part of what a database enforces. WP0.5 found the differ ignoring all of it:
    /// a target that re-enabled a disabled foreign key compared clean. This moves the
    /// flags on a real target and makes the diff put them back — with state changes,
    /// not by dropping and re-creating constraints that never changed shape.
    /// </summary>
    [LiveFact]
    public async Task ConstraintStateConverges()
    {
        var sourceConnection = await _sqlServer.CreateDatabaseWithFullSchemaAsync();
        var targetConnection = await _sqlServer.CreateDatabaseWithFullSchemaAsync();

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        Snapshots.AssertNoChanges("two databases built the same way",
            Compare(source, await SqlServerFixture.ExtractAsync(targetConnection)));

        // Switch on everything full.sql deliberately left off, validate what it left
        // untrusted, and put an index to sleep.
        await SqlServerFixture.ApplyAsync(targetConnection, """
            ALTER TABLE ops.AuditEntry WITH CHECK CHECK CONSTRAINT FK_AuditEntry_Customer;
            GO
            ALTER TABLE ops.AuditEntry WITH CHECK CHECK CONSTRAINT FK_AuditEntry_Invoice;
            GO
            ALTER TABLE ops.AuditEntry WITH CHECK CHECK CONSTRAINT CK_AuditEntry_EventKind;
            GO
            ALTER INDEX IX_AuditEntry_Source ON ops.AuditEntry DISABLE;
            GO
            """, useTransaction: true);

        var target = await SqlServerFixture.ExtractAsync(targetConnection);
        var drifted = target.Table("ops", "AuditEntry");
        Assert.False(drifted.ForeignKey("FK_AuditEntry_Customer").IsDisabled);
        Assert.False(drifted.ForeignKey("FK_AuditEntry_Invoice").IsNotTrusted);
        Assert.False(drifted.Check("CK_AuditEntry_EventKind").IsDisabled);
        Assert.True(drifted.Index("IX_AuditEntry_Source").IsDisabled);

        var diff = Compare(source, target);
        Assert.True(diff.HasChanges, "moving the flags is drift and has to be reported as drift");
        Assert.Contains("[ops].[AuditEntry]", diff.ChangedObjects);

        // Each one is a state change. Dropping a foreign key and putting it back
        // re-validates every row in the table; the flags are one ALTER each.
        Assert.Contains("NOCHECK CONSTRAINT [FK_AuditEntry_Customer];", diff.Script);
        Assert.Contains("WITH NOCHECK CHECK CONSTRAINT [FK_AuditEntry_Invoice];", diff.Script);
        Assert.Contains("NOCHECK CONSTRAINT [CK_AuditEntry_EventKind];", diff.Script);
        Assert.Contains("ALTER INDEX [IX_AuditEntry_Source] ON [ops].[AuditEntry] REBUILD;", diff.Script);
        Assert.DoesNotContain("DROP CONSTRAINT", diff.Script);
        Assert.DoesNotContain("DROP INDEX", diff.Script);

        await SqlServerFixture.ApplyAsync(targetConnection, diff.Script, useTransaction: true);

        target = await SqlServerFixture.ExtractAsync(targetConnection);
        var settled = target.Table("ops", "AuditEntry");
        Assert.True(settled.ForeignKey("FK_AuditEntry_Customer").IsDisabled);
        Assert.True(settled.ForeignKey("FK_AuditEntry_Invoice").IsNotTrusted);
        Assert.False(settled.ForeignKey("FK_AuditEntry_Invoice").IsDisabled);
        Assert.True(settled.Check("CK_AuditEntry_EventKind").IsDisabled);
        Assert.False(settled.Index("IX_AuditEntry_Source").IsDisabled);

        Snapshots.AssertNoChanges("after the state changes", Compare(source, target));
        Snapshots.AssertNoChanges("after the state changes (reversed)", Compare(target, source));
    }

    private DiffResult Compare(DatabaseSnapshot source, DatabaseSnapshot target) =>
        _differ.Diff(source, target, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: false, addOnly: false);
}
