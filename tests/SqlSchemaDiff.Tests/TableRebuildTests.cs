using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

/// <summary>
/// The rebuild path: what <c>--allow-table-rebuild</c> now means. It used to mean
/// <c>DROP TABLE</c> followed by <c>CREATE TABLE</c> — every row gone, and the
/// statement failing outright the moment another table pointed a foreign key at this
/// one. It now means the table is built again beside the original, the rows are
/// copied across and the copy is renamed into place.
/// </summary>
public class TableRebuildTests
{
    private readonly SchemaDiffer _differ = new();

    // -------------------------------------------------------------- the trigger

    /// <summary>
    /// The rebuild only happens for a change nothing else can express. Adding a
    /// column is not one, so the flag being on changes nothing about how it is done.
    /// </summary>
    [Fact]
    public void ChangeThatAnAlterCanExpress_IsNotRebuiltEvenWithTheFlagOn()
    {
        var source = Snapshot("Src", TableObject(Table("Orders", Col("Id", nullable: false), NVarchar("Note", 40))));
        var target = Snapshot("Tgt", TableObject(Table("Orders", Col("Id", nullable: false))));

        var result = Diff(source, target, allowTableRebuild: true);

        Assert.Contains("ALTER TABLE [dbo].[Orders] ADD [Note]", result.Script);
        Assert.DoesNotContain("tmp_sqldiff_", result.Script);
        Assert.DoesNotContain("DROP TABLE", result.Script);
    }

    [Fact]
    public void IdentityChangeWithoutTheFlag_IsReportedAndRefused()
    {
        var result = Diff(SourceSnapshot(), TargetSnapshot(), allowTableRebuild: false);

        Assert.Contains("Manual table rebuild required", result.Script);
        Assert.DoesNotContain("tmp_sqldiff_", result.Script);
        Assert.DoesNotContain("DROP TABLE", result.Script);
    }

    [Fact]
    public void IdentityChangeWithTheFlag_IsRebuilt()
    {
        var result = Diff(SourceSnapshot(), TargetSnapshot(), allowTableRebuild: true);

        Assert.Equal(1, result.Changed);
        Assert.Contains("[dbo].[Orders]", result.ChangedObjects);
        Assert.Contains("REBUILD [dbo].[Orders]", result.Script);
        Assert.Contains("changes its identity property", result.Script);
    }

    // ------------------------------------------------------------ the statements

    [Fact]
    public void Rebuild_EmitsItsStatementsInTheOnlyOrderThatWorks()
    {
        var result = Diff(SourceSnapshot(), TargetSnapshot(), allowTableRebuild: true);

        AssertOrder(result.Script,
            "CREATE TABLE [dbo].[tmp_sqldiff_Orders]",
            "SET IDENTITY_INSERT [dbo].[tmp_sqldiff_Orders] ON;",
            "INSERT INTO [dbo].[tmp_sqldiff_Orders]",
            "SET IDENTITY_INSERT [dbo].[tmp_sqldiff_Orders] OFF;",
            "ALTER TABLE [dbo].[Shipment] DROP CONSTRAINT [FK_Shipment_Orders];",
            "DROP TABLE [dbo].[Orders];",
            "EXEC sp_rename N'[dbo].[tmp_sqldiff_Orders]', N'Orders';",
            "EXEC sp_rename N'[dbo].[tmp_sqldiff_DF_Orders_Total]', N'DF_Orders_Total', N'OBJECT';",
            "ADD CONSTRAINT [PK_Orders] PRIMARY KEY CLUSTERED",
            "ADD CONSTRAINT [CK_Orders_Total] CHECK",
            "CREATE NONCLUSTERED INDEX [IX_Orders_Note]",
            "ALTER TABLE [dbo].[Shipment] WITH CHECK ADD CONSTRAINT [FK_Shipment_Orders]",
            "CREATE TRIGGER");
    }

    /// <summary>
    /// Only what both sides have, and only what a value can be written into: a
    /// computed column is derived and a rowversion is stamped by the engine, so
    /// naming either in the INSERT is an error, not an optimisation.
    /// </summary>
    [Fact]
    public void Rebuild_CopiesTheColumnsBothSidesHave_AndNothingElse()
    {
        var result = Diff(SourceSnapshot(), TargetSnapshot(), allowTableRebuild: true);

        Assert.Contains(
            "INSERT INTO [dbo].[tmp_sqldiff_Orders] ([OrderId], [Total], [Note])",
            result.Script);
        Assert.Contains(
            "SELECT [OrderId], [Total], [Note] FROM [dbo].[Orders] WITH (HOLDLOCK TABLOCKX);",
            result.Script);

        // [Doubled] is computed, [Stamp] is a rowversion, [Colour] is new on the
        // source and so has nothing on the target to copy from.
        Assert.DoesNotContain("[Doubled]", result.Script.Split("SELECT")[1]);
        Assert.DoesNotContain("[Stamp],", result.Script);
        Assert.DoesNotContain("[Colour],", result.Script);
    }

    /// <summary>
    /// A column the source adds gets no value from the INSERT, so its default has to
    /// be on the temporary table from the start or a NOT NULL one rejects every row.
    /// </summary>
    [Fact]
    public void Rebuild_CarriesTheDefaultsOnTheTemporaryTable()
    {
        var result = Diff(SourceSnapshot(), TargetSnapshot(), allowTableRebuild: true);

        var create = result.Script[..result.Script.IndexOf("SET IDENTITY_INSERT", StringComparison.Ordinal)];
        Assert.Contains("CONSTRAINT [tmp_sqldiff_DF_Orders_Colour] DEFAULT ('grey')", create);
    }

    /// <summary>
    /// Two objects in one schema cannot share a name, and a named default constraint
    /// is an object — so the temporary table cannot carry the original's names while
    /// the original is still there. It carries prefixed ones and they are renamed back
    /// once the original is gone.
    /// </summary>
    [Fact]
    public void Rebuild_RenamesTheDefaultConstraintsSoTheyDoNotCollide()
    {
        var result = Diff(SourceSnapshot(), TargetSnapshot(), allowTableRebuild: true);

        Assert.Contains("CONSTRAINT [tmp_sqldiff_DF_Orders_Total] DEFAULT ((0))", result.Script);
        Assert.Contains("EXEC sp_rename N'[dbo].[tmp_sqldiff_DF_Orders_Total]', N'DF_Orders_Total', N'OBJECT';", result.Script);
        Assert.Contains("EXEC sp_rename N'[dbo].[tmp_sqldiff_DF_Orders_Colour]', N'DF_Orders_Colour', N'OBJECT';", result.Script);
    }

    [Fact]
    public void Rebuild_LeavesIdentityInsertOutWhenTheNewTableHasNoIdentity()
    {
        // Taking the identity off is the other half of the case, and the copy is then
        // an ordinary INSERT: SET IDENTITY_INSERT on a table with no identity column
        // is an error, not a no-op.
        var source = Snapshot("Src", TableObject(Orders(identity: false, withColour: false)));
        var target = Snapshot("Tgt", TableObject(Orders(identity: true, withColour: false)));

        var result = Diff(source, target, allowTableRebuild: true);

        Assert.Contains("INSERT INTO [dbo].[tmp_sqldiff_Orders]", result.Script);
        Assert.DoesNotContain("SET IDENTITY_INSERT", result.Script);
    }

    [Fact]
    public void Rebuild_SaysWhatItDoesNotCarryAcross()
    {
        var result = Diff(SourceSnapshot(), TargetSnapshot(), allowTableRebuild: true);

        Assert.Contains("NOT carried over: permissions (GRANT/DENY/REVOKE), extended properties", result.Script);
        Assert.Contains("Rows are preserved (3 column(s) copied, identity values included).", result.Script);
        Assert.Contains("Foreign keys pointing at it are dropped and re-created: FK_Shipment_Orders.", result.Script);
        Assert.Contains("Triggers re-created: [dbo].[trOrders].", result.Script);
    }

    // ------------------------------------------------------- no duplicated work

    /// <summary>
    /// The rebuild drops the referencing table's foreign key and puts it back. The
    /// referencing table is diffed too, in the same run — and if it does not know, it
    /// adds the key a second time and the script fails.
    /// </summary>
    [Fact]
    public void InboundForeignKey_IsDroppedAndRecreatedExactlyOnce()
    {
        var source = SourceSnapshot();
        var target = TargetSnapshot(withInboundForeignKey: false);

        var result = Diff(source, target, allowTableRebuild: true);

        Assert.Equal(1, Occurrences(result.Script, "ADD CONSTRAINT [FK_Shipment_Orders]"));
        Assert.DoesNotContain("[dbo].[Shipment]", result.ChangedObjects);
    }

    [Fact]
    public void InboundForeignKeyThatAlsoChanged_IsStillOnlyEmittedByTheRebuild()
    {
        var source = SourceSnapshot();
        source.Objects.Single(x => x.Name == "Shipment").Table!.ForeignKeys[0].DeleteActionDesc = "CASCADE";

        var result = Diff(source, TargetSnapshot(), allowTableRebuild: true);

        Assert.Equal(1, Occurrences(result.Script, "ADD CONSTRAINT [FK_Shipment_Orders]"));
        Assert.Contains("ON DELETE CASCADE", result.Script);
        Assert.Equal(1, Occurrences(result.Script, "DROP CONSTRAINT [FK_Shipment_Orders]"));
    }

    [Fact]
    public void TriggerOnARebuiltTable_IsCreatedExactlyOnce()
    {
        var result = Diff(SourceSnapshot(), TargetSnapshot(), allowTableRebuild: true);

        Assert.Equal(1, Occurrences(result.Script, "CREATE TRIGGER [dbo].[trOrders]"));
        Assert.DoesNotContain("[dbo].[trOrders]", result.ChangedObjects);
        Assert.DoesNotContain("[dbo].[trOrders]", result.AddedObjects);
    }

    [Fact]
    public void DisabledTriggerOnARebuiltTable_IsSwitchedOffAgain()
    {
        var source = SourceSnapshot(triggerDisabled: true);

        var result = Diff(source, TargetSnapshot(), allowTableRebuild: true);

        AssertOrder(result.Script,
            "CREATE TRIGGER [dbo].[trOrders]",
            "DISABLE TRIGGER [dbo].[trOrders] ON [dbo].[Orders];");
    }

    /// <summary>
    /// A key the table points at itself goes with the table when it is dropped and
    /// comes back with the table's own keys, so it must not be handled twice.
    /// </summary>
    [Fact]
    public void SelfReferencingForeignKey_IsRecreatedOnceWithTheTablesOwnKeys()
    {
        var sourceTable = Orders(identity: true);
        sourceTable.ForeignKeys.Add(ForeignKey("FK_Orders_Orders", "Orders", "Total", "OrderId"));
        var targetTable = Orders(identity: false);
        targetTable.ForeignKeys.Add(ForeignKey("FK_Orders_Orders", "Orders", "Total", "OrderId"));

        var result = Diff(
            Snapshot("Src", TableObject(sourceTable)),
            Snapshot("Tgt", TableObject(targetTable)),
            allowTableRebuild: true);

        Assert.Equal(1, Occurrences(result.Script, "ADD CONSTRAINT [FK_Orders_Orders]"));
        Assert.Equal(0, Occurrences(result.Script, "DROP CONSTRAINT [FK_Orders_Orders]"));
    }

    // ------------------------------------------------- keys only the target has

    /// <summary>
    /// A key pointing at the table has to come down for the DROP whether or not the
    /// source knows about it. Putting it back is what keeps a rebuild from deleting a
    /// constraint the caller never asked to delete.
    /// </summary>
    [Fact]
    public void TargetOnlyInboundForeignKey_IsPutBackWhenDropsWereNotAskedFor()
    {
        var source = SourceSnapshot(withInboundForeignKey: false);
        var target = TargetSnapshot();

        var result = Diff(source, target, allowTableRebuild: true, includeDrops: false);

        Assert.Contains("ALTER TABLE [dbo].[Shipment] DROP CONSTRAINT [FK_Shipment_Orders];", result.Script);
        Assert.Equal(1, Occurrences(result.Script, "ADD CONSTRAINT [FK_Shipment_Orders]"));
        Assert.Contains("It is put back as the target had it", result.Script);
    }

    [Fact]
    public void TargetOnlyInboundForeignKey_StaysDownWhenDropsWereAskedFor()
    {
        var source = SourceSnapshot(withInboundForeignKey: false);
        var target = TargetSnapshot();

        var result = Diff(source, target, allowTableRebuild: true, includeDrops: true);

        Assert.Contains("ALTER TABLE [dbo].[Shipment] DROP CONSTRAINT [FK_Shipment_Orders];", result.Script);
        Assert.Equal(0, Occurrences(result.Script, "ADD CONSTRAINT [FK_Shipment_Orders]"));
    }

    // ----------------------------------------------------------------- ordering

    /// <summary>
    /// A table created in the same run with a foreign key into the rebuilt one has to
    /// wait: its key would otherwise be in the way of the DROP. The rebuild rides with
    /// the creates rather than the alters precisely so the dependency sort can say so.
    /// </summary>
    [Fact]
    public void Rebuild_RunsBeforeANewTableThatPointsAtIt()
    {
        var source = SourceSnapshot();
        var newTable = Table("Refund", Col("Id", nullable: false), Col("OrderId", "int"));
        newTable.ForeignKeys.Add(ForeignKey("FK_Refund_Orders", "Orders", "OrderId", "OrderId"));
        source.Objects.Add(new DbSchemaObject
        {
            Type = DbObjectType.Table,
            Schema = newTable.Schema,
            Name = newTable.Name,
            Definition = SqlRender.BuildTableCreateScript(newTable),
            Dependencies = { DbSchemaObject.BuildKey(DbObjectType.Table, "dbo", "Orders") },
            Table = newTable
        });

        var result = Diff(source, TargetSnapshot(), allowTableRebuild: true);

        AssertOrder(result.Script,
            "DROP TABLE [dbo].[Orders];",
            "CREATE TABLE [dbo].[Refund]");
    }

    // -------------------------------------------------- snapshots with no model

    /// <summary>
    /// The target snapshot predates structured tables. The source's model is still
    /// enough to build the new table; only the column list has to fall back to the
    /// source's own columns, and the script says so.
    /// </summary>
    [Fact]
    public void TargetWithoutAModel_IsRebuiltFromTheSourceShape()
    {
        var sourceTable = Orders(identity: true);
        var source = Snapshot("Src", TableObject(sourceTable, SqlRender.BuildTableCreateScript(sourceTable)));
        var target = Snapshot("Tgt", new DbSchemaObject
        {
            Type = DbObjectType.Table, Schema = "dbo", Name = "Orders", Definition = "CREATE TABLE [dbo].[Orders] (something else)"
        });

        var result = Diff(source, target, allowTableRebuild: true);

        Assert.Contains("REBUILD [dbo].[Orders]", result.Script);
        Assert.Contains("no structured model for this table", result.Script);
        Assert.Contains("INSERT INTO [dbo].[tmp_sqldiff_Orders]", result.Script);
    }

    /// <summary>
    /// Neither side has a model, so there is no shape to copy into and no column list
    /// to copy through. The destructive form is all that is left, and the script has
    /// to say what it costs rather than do it quietly.
    /// </summary>
    [Fact]
    public void NeitherSideWithAModel_FallsBackToDropAndCreate_AndSaysSo()
    {
        var source = Snapshot("Src", new DbSchemaObject
        {
            Type = DbObjectType.Table, Schema = "dbo", Name = "Orders", Definition = "CREATE TABLE A"
        });
        var target = Snapshot("Tgt", new DbSchemaObject
        {
            Type = DbObjectType.Table, Schema = "dbo", Name = "Orders", Definition = "CREATE TABLE B"
        });

        var result = Diff(source, target, allowTableRebuild: true);

        Assert.Contains("is dropped and recreated, and its rows are lost", result.Script);
        Assert.Contains("DROP TABLE [dbo].[Orders]", result.Script);
        Assert.DoesNotContain("tmp_sqldiff_", result.Script);
    }

    [Fact]
    public void AddOnly_SkipsARebuild()
    {
        var result = _differ.Diff(SourceSnapshot(), TargetSnapshot(), includeDrops: false,
            includeTableDrops: false, allowTableRebuild: true, addOnly: true);

        Assert.True(result.Skipped >= 1);
        Assert.DoesNotContain("tmp_sqldiff_", result.Script);
    }

    // ------------------------------------------------------------------ fixtures

    private DiffResult Diff(DatabaseSnapshot source, DatabaseSnapshot target, bool allowTableRebuild, bool includeDrops = false) =>
        _differ.Diff(source, target, includeDrops, includeTableDrops: includeDrops, allowTableRebuild, addOnly: false);

    /// <summary>
    /// A table with one of everything the rebuild has to carry: an identity, a named
    /// default, a computed column, a rowversion, a key, a check and an index.
    /// </summary>
    private static TableModel Orders(bool identity, bool withColour = true)
    {
        var columns = new List<ColumnModel>
        {
            Col("OrderId", "int", nullable: false, identity: identity, identitySeed: "1000", identityIncrement: "5"),
            Col("Total", "int", nullable: false, defaultName: "DF_Orders_Total", defaultDefinition: "((0))"),
            NVarchar("Note", 100)
        };

        if(withColour)
        {
            columns.Add(Col("Colour", "varchar", 20, nullable: false,
                defaultName: "DF_Orders_Colour", defaultDefinition: "('grey')"));
        }

        columns.Add(Col("Stamp", "timestamp", 8, nullable: false));
        columns.Add(Col("Doubled", "int", computedDefinition: "([Total]*(2))"));

        var table = new TableModel { Schema = "dbo", Name = "Orders", Columns = columns };
        table.KeyConstraints.Add(Key("PK_Orders", "PK", "CLUSTERED", "OrderId"));
        table.CheckConstraints.Add(Check("CK_Orders_Total", "([Total]>=(0))"));
        table.Indexes.Add(Index("IX_Orders_Note", unique: false, "Note"));
        return table;
    }

    private static TableModel Shipment(bool withForeignKey)
    {
        var table = Table("Shipment", Col("Id", nullable: false), Col("OrderId", "int"));
        if(withForeignKey)
            table.ForeignKeys.Add(ForeignKey("FK_Shipment_Orders", "Orders", "OrderId", "OrderId"));
        return table;
    }

    private static DatabaseSnapshot SourceSnapshot(bool withInboundForeignKey = true, bool triggerDisabled = false)
    {
        var orders = Orders(identity: true);
        var shipment = Shipment(withInboundForeignKey);
        return Snapshot("Src",
            TableObject(orders, SqlRender.BuildTableCreateScript(orders)),
            TableObject(shipment, SqlRender.BuildTableCreateScript(shipment)),
            TriggerObject("trOrders", "dbo", "Orders", triggerDisabled));
    }

    private static DatabaseSnapshot TargetSnapshot(bool withInboundForeignKey = true)
    {
        var orders = Orders(identity: false, withColour: false);
        var shipment = Shipment(withInboundForeignKey);
        return Snapshot("Tgt",
            TableObject(orders, SqlRender.BuildTableCreateScript(orders)),
            TableObject(shipment, SqlRender.BuildTableCreateScript(shipment)),
            TriggerObject("trOrders", "dbo", "Orders"));
    }

    private static void AssertOrder(string script, params string[] fragments)
    {
        var previous = -1;
        var previousFragment = string.Empty;
        foreach(var fragment in fragments)
        {
            var index = script.IndexOf(fragment, StringComparison.Ordinal);
            Assert.True(index >= 0, $"the script never says \"{fragment}\":{Environment.NewLine}{script}");
            Assert.True(index > previous,
                $"\"{fragment}\" comes before \"{previousFragment}\" and should not:{Environment.NewLine}{script}");
            previous = index;
            previousFragment = fragment;
        }
    }

    private static int Occurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }
}
