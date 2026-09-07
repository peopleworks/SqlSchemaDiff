using Microsoft.Data.SqlClient;
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

    /// <summary>
    /// The test this work package exists for. A check constraint and a foreign key that
    /// SQL Server named for itself, both switched off, read out of one database and
    /// written back into another as a from-scratch script.
    /// <para>
    /// Every renderer before 1.7 dropped the <c>NOCHECK</c> on the floor here: the only
    /// name it had was the one the other server invented, which means nothing on this
    /// one. What came back was a database enforcing two constraints its source did not,
    /// and reading the script would not have shown it - the statements were simply not
    /// there. Only applying it and looking again says so.
    /// </para>
    /// </summary>
    [LiveFact]
    public async Task DisabledServerNamedConstraintsSurviveAFromScratchScript()
    {
        var sourceConnection = _sqlServer.CreateDatabase();
        await SqlServerFixture.ApplyAsync(sourceConnection, ServerNamedAndSwitchedOff, useTransaction: false);

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        AssertStillSwitchedOff("the source database itself", source);

        // The composer's phase plan, which is what a restore from a snapshot runs.
        var composed = _sqlServer.CreateDatabase();
        await SqlServerFixture.ApplyAsync(composed, ScriptComposer.ComposeFullScript(source), useTransaction: false);
        AssertStillSwitchedOff("a composed from-scratch script", await SqlServerFixture.ExtractAsync(composed));

        // And the differ against an empty database, which emits each table's own
        // definition instead - a second renderer with the same job and the same bug.
        var created = _sqlServer.CreateDatabase();
        var diff = _differ.Diff(source, await SqlServerFixture.ExtractAsync(created),
            includeDrops: true, includeTableDrops: true, allowTableRebuild: true, addOnly: false);
        await SqlServerFixture.ApplyAsync(created, diff.Script, useTransaction: false);

        var rebuilt = await SqlServerFixture.ExtractAsync(created);
        AssertStillSwitchedOff("a diff against an empty database", rebuilt);

        // Which is the whole point: the two now compare clean, in both directions,
        // instead of reporting a difference nobody introduced.
        Snapshots.AssertNoChanges("after the from-scratch script", Compare(source, rebuilt));
        Snapshots.AssertNoChanges("after the from-scratch script (reversed)", Compare(rebuilt, source));
    }

    /// <summary>
    /// A rebuild run with <c>--include-drops</c> but not <c>--include-table-drops</c>.
    /// The table pointing at the rebuilt one exists only on the target, and the differ
    /// says in as many words that it is being kept - so the foreign key standing on it
    /// has to be there, and enforcing, when the script finishes. Until 1.7 the rebuild
    /// took it down to clear the way for its <c>DROP TABLE</c> and never put it back.
    /// </summary>
    [LiveFact]
    public async Task RebuildKeepsAnInboundKeyOnATableItWasNotAskedToDrop()
    {
        var sourceConnection = _sqlServer.CreateDatabase();
        await SqlServerFixture.ApplyAsync(sourceConnection, """
            CREATE SCHEMA keep;
            GO
            CREATE TABLE keep.Widget
            (
                WidgetId int IDENTITY(1, 1) NOT NULL CONSTRAINT PK_Widget PRIMARY KEY,
                Sku      varchar(40)        NOT NULL
            );
            GO
            """, useTransaction: false);

        var targetConnection = _sqlServer.CreateDatabase();
        await SqlServerFixture.ApplyAsync(targetConnection, """
            CREATE SCHEMA keep;
            GO
            CREATE TABLE keep.Widget
            (
                WidgetId int         NOT NULL CONSTRAINT PK_Widget PRIMARY KEY,
                Sku      varchar(40) NOT NULL
            );
            GO
            CREATE TABLE keep.WidgetLog
            (
                LogId    int IDENTITY(1, 1) NOT NULL CONSTRAINT PK_WidgetLog PRIMARY KEY,
                WidgetId int                NOT NULL CONSTRAINT FK_WidgetLog_Widget
                                                     REFERENCES keep.Widget (WidgetId)
            );
            GO
            INSERT INTO keep.Widget (WidgetId, Sku) VALUES (1, 'W-001'), (2, 'W-002');
            GO
            INSERT INTO keep.WidgetLog (WidgetId) VALUES (1);
            GO
            """, useTransaction: false);

        var source = await SqlServerFixture.ExtractAsync(sourceConnection);
        var target = await SqlServerFixture.ExtractAsync(targetConnection);

        var diff = _differ.Diff(source, target, includeDrops: true, includeTableDrops: false,
            allowTableRebuild: true, addOnly: false);
        Assert.Contains("REBUILD [keep].[Widget]", diff.Script);
        Assert.Contains("table exists only on target and was not dropped: [keep].[WidgetLog]", diff.Script);
        Assert.Contains("FK_WidgetLog_Widget is put back", diff.Script);

        var applied = await SqlServerFixture.ApplyAsync(targetConnection, diff.Script, useTransaction: true);
        Assert.False(applied.RolledBack);

        var rebuilt = await SqlServerFixture.ExtractAsync(targetConnection);
        Assert.True(rebuilt.Table("keep", "Widget").Column("WidgetId").IsIdentity,
            "the rebuild was supposed to add the identity");
        rebuilt.Table("keep", "WidgetLog").ForeignKey("FK_WidgetLog_Widget");

        // There, and enforcing: the log row that pointed at a widget still does, and one
        // that points at nothing is still refused.
        Assert.Equal(2, await SqlServerFixture.ScalarAsync<int>(targetConnection, "SELECT COUNT(*) FROM keep.Widget;"));
        Assert.Equal(1, await SqlServerFixture.ScalarAsync<int>(targetConnection, "SELECT COUNT(*) FROM keep.WidgetLog;"));
        await Assert.ThrowsAnyAsync<SqlException>(() => SqlServerFixture.ApplyAsync(targetConnection,
            "INSERT INTO keep.WidgetLog (WidgetId) VALUES (99);", useTransaction: true));
    }

    /// <summary>
    /// A check constraint and a foreign key with no name of their own, both switched
    /// off by the names this server invented for them - which is exactly the state a
    /// script generated somewhere else cannot name.
    /// </summary>
    private const string ServerNamedAndSwitchedOff = """
        CREATE SCHEMA nn;
        GO

        CREATE TABLE nn.Widget
        (
            WidgetId int            NOT NULL CONSTRAINT PK_Widget PRIMARY KEY,
            Price    decimal(12, 2) NOT NULL,
            CHECK (Price >= 0)
        );
        GO

        CREATE TABLE nn.WidgetLog
        (
            LogId    int NOT NULL CONSTRAINT PK_WidgetLog PRIMARY KEY,
            WidgetId int NOT NULL FOREIGN KEY REFERENCES nn.Widget (WidgetId)
        );
        GO

        DECLARE @sql nvarchar(max);

        SELECT @sql = N'ALTER TABLE nn.Widget NOCHECK CONSTRAINT ' + QUOTENAME(name)
        FROM sys.check_constraints
        WHERE parent_object_id = OBJECT_ID(N'nn.Widget');
        EXEC sys.sp_executesql @sql;

        SELECT @sql = N'ALTER TABLE nn.WidgetLog NOCHECK CONSTRAINT ' + QUOTENAME(name)
        FROM sys.foreign_keys
        WHERE parent_object_id = OBJECT_ID(N'nn.WidgetLog');
        EXEC sys.sp_executesql @sql;
        GO
        """;

    /// <summary>
    /// Both constraints are still nameless and still switched off. The names are not
    /// compared - they cannot be, they are different on every database - which is the
    /// reason the disable has to be resolved on the server in the first place.
    /// </summary>
    private static void AssertStillSwitchedOff(string what, DatabaseSnapshot snapshot)
    {
        var check = Assert.Single(snapshot.Table("nn", "Widget").CheckConstraints);
        Assert.True(check.IsSystemNamed,
            $"{what}: the check constraint was supposed to keep a server-generated name, got [{check.Name}].");
        Assert.True(check.IsDisabled,
            $"{what}: [{check.Name}] on [nn].[Widget] is enforcing again, and the source had it switched off.");

        var foreignKey = Assert.Single(snapshot.Table("nn", "WidgetLog").ForeignKeys);
        Assert.True(foreignKey.IsSystemNamed,
            $"{what}: the foreign key was supposed to keep a server-generated name, got [{foreignKey.Name}].");
        Assert.True(foreignKey.IsDisabled,
            $"{what}: [{foreignKey.Name}] on [nn].[WidgetLog] is enforcing again, and the source had it switched off.");
    }

    private DiffResult Compare(DatabaseSnapshot source, DatabaseSnapshot target) =>
        _differ.Diff(source, target, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: false, addOnly: false);
}
