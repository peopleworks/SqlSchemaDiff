using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

/// <summary>
/// Whether a constraint is switched on, and whether SQL Server ever checked the rows
/// that were already there, is part of what a database enforces — so it is part of
/// what the differ compares. Before 1.6 none of it was: a target that re-enabled a
/// disabled foreign key, or validated an untrusted one, compared clean against a
/// source that had it off.
/// </summary>
public class ConstraintStateTests
{
    private readonly TableDiffer _differ = new();

    // ------------------------------------------------------------ foreign keys

    [Fact]
    public void ForeignKeyDisabledOnSource_IsSwitchedOffRatherThanRecreated()
    {
        var source = WithForeignKey(disabled: true, notTrusted: true);
        var target = WithForeignKey(disabled: false, notTrusted: false);

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.True(result.HasChanges);
        Assert.Contains("ALTER TABLE [dbo].[Invoice] NOCHECK CONSTRAINT [FK_Invoice_Customer];", result.Script);
        Assert.DoesNotContain("DROP CONSTRAINT", result.Script);
        Assert.DoesNotContain("ADD CONSTRAINT", result.Script);
    }

    [Fact]
    public void ForeignKeyEnabledOnSource_IsSwitchedBackOnAndValidated()
    {
        var source = WithForeignKey(disabled: false, notTrusted: false);
        var target = WithForeignKey(disabled: true, notTrusted: true);

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("ALTER TABLE [dbo].[Invoice] WITH CHECK CHECK CONSTRAINT [FK_Invoice_Customer];", result.Script);
        Assert.DoesNotContain("DROP CONSTRAINT", result.Script);
    }

    [Fact]
    public void ForeignKeyUntrustedOnSource_ComesBackOnWithoutValidating()
    {
        var source = WithForeignKey(disabled: false, notTrusted: true);
        var target = WithForeignKey(disabled: true, notTrusted: true);

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("WITH NOCHECK CHECK CONSTRAINT [FK_Invoice_Customer];", result.Script);
        Assert.DoesNotContain("WITH CHECK CHECK", result.Script);
    }

    /// <summary>
    /// The awkward direction: the target validated its rows and the source never did.
    /// NOCHECK is the only thing that clears the trusted bit, so the key has to be
    /// switched off and then back on without validation.
    /// </summary>
    [Fact]
    public void ForeignKeyTrustedOnTargetAndUntrustedOnSource_LosesItsTrustInTwoSteps()
    {
        var source = WithForeignKey(disabled: false, notTrusted: true);
        var target = WithForeignKey(disabled: false, notTrusted: false);

        var result = _differ.Diff(source, target, includeDrops: false);

        var noCheck = result.Script.IndexOf("NOCHECK CONSTRAINT [FK_Invoice_Customer];", StringComparison.Ordinal);
        var reEnable = result.Script.IndexOf("WITH NOCHECK CHECK CONSTRAINT [FK_Invoice_Customer];", StringComparison.Ordinal);
        Assert.True(noCheck >= 0, result.Script);
        Assert.True(reEnable > noCheck, result.Script);
        Assert.DoesNotContain("DROP CONSTRAINT", result.Script);
    }

    [Fact]
    public void ForeignKeyWithTheSameState_IsNotADifference()
    {
        var source = WithForeignKey(disabled: true, notTrusted: true);
        var target = WithForeignKey(disabled: true, notTrusted: true);

        Assert.False(_differ.Diff(source, target, includeDrops: false).HasChanges);
    }

    /// <summary>
    /// A shape change is still a drop and re-create — and the disabled state has to
    /// survive it, because ADD CONSTRAINT always leaves a key switched on.
    /// </summary>
    [Fact]
    public void RecreatedForeignKey_IsSwitchedOffAgainAfterwards()
    {
        var source = WithForeignKey(disabled: true, notTrusted: true);
        source.ForeignKeys[0].DeleteActionDesc = "CASCADE";
        var target = WithForeignKey(disabled: true, notTrusted: true);

        var result = _differ.Diff(source, target, includeDrops: false);

        var add = result.Script.IndexOf("ADD CONSTRAINT [FK_Invoice_Customer]", StringComparison.Ordinal);
        var noCheck = result.Script.IndexOf("NOCHECK CONSTRAINT [FK_Invoice_Customer];", StringComparison.Ordinal);
        Assert.True(add >= 0, result.Script);
        Assert.True(noCheck > add, result.Script);
    }

    // -------------------------------------------------------- check constraints

    [Fact]
    public void CheckConstraintDisabledOnSource_IsSwitchedOff()
    {
        var source = WithCheck(disabled: true, notTrusted: true);
        var target = WithCheck(disabled: false, notTrusted: false);

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("ALTER TABLE [dbo].[Invoice] NOCHECK CONSTRAINT [CK_Invoice_Qty];", result.Script);
        Assert.DoesNotContain("DROP CONSTRAINT", result.Script);
    }

    [Fact]
    public void CheckConstraintEnabledOnSource_IsSwitchedBackOnAndValidated()
    {
        var source = WithCheck(disabled: false, notTrusted: false);
        var target = WithCheck(disabled: true, notTrusted: true);

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("WITH CHECK CHECK CONSTRAINT [CK_Invoice_Qty];", result.Script);
    }

    [Fact]
    public void RecreatedCheckConstraint_IsSwitchedOffAgainAfterwards()
    {
        var source = WithCheck(disabled: true, notTrusted: true);
        source.CheckConstraints[0].Definition = "([Qty]>(1))";
        var target = WithCheck(disabled: true, notTrusted: true);

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("DROP CONSTRAINT [CK_Invoice_Qty];", result.Script);
        var add = result.Script.IndexOf("ADD CONSTRAINT [CK_Invoice_Qty] CHECK ([Qty]>(1));", StringComparison.Ordinal);
        var noCheck = result.Script.IndexOf("NOCHECK CONSTRAINT [CK_Invoice_Qty];", StringComparison.Ordinal);
        Assert.True(add >= 0, result.Script);
        Assert.True(noCheck > add, result.Script);
    }

    // ------------------------------------------------------------------ indexes

    [Fact]
    public void IndexDisabledOnSource_IsDisabledRatherThanRecreated()
    {
        var source = WithIndex(disabled: true);
        var target = WithIndex(disabled: false);

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("ALTER INDEX [IX_Invoice_Qty] ON [dbo].[Invoice] DISABLE;", result.Script);
        Assert.DoesNotContain("DROP INDEX", result.Script);
        Assert.DoesNotContain("CREATE NONCLUSTERED INDEX", result.Script);
    }

    [Fact]
    public void IndexEnabledOnSource_IsRebuilt()
    {
        var source = WithIndex(disabled: false);
        var target = WithIndex(disabled: true);

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("ALTER INDEX [IX_Invoice_Qty] ON [dbo].[Invoice] REBUILD;", result.Script);
        Assert.DoesNotContain("DROP INDEX", result.Script);
    }

    /// <summary>
    /// SET and REBUILD both need the index online. An index that is asleep on both
    /// sides and differs in its options is cheaper to drop and re-create than to wake
    /// up, re-option and put back down — and the re-create has to re-disable it.
    /// </summary>
    [Fact]
    public void DisabledIndexWithChangedOptions_IsRecreatedAndDisabledAgain()
    {
        var source = WithIndex(disabled: true);
        source.Indexes[0].FillFactor = 80;
        var target = WithIndex(disabled: true);

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("DROP INDEX [IX_Invoice_Qty] ON [dbo].[Invoice];", result.Script);
        var create = result.Script.IndexOf("CREATE NONCLUSTERED INDEX [IX_Invoice_Qty]", StringComparison.Ordinal);
        var disable = result.Script.IndexOf("ALTER INDEX [IX_Invoice_Qty] ON [dbo].[Invoice] DISABLE;", StringComparison.Ordinal);
        Assert.True(create >= 0, result.Script);
        Assert.True(disable > create, result.Script);
    }

    [Fact]
    public void IndexComingBackOnline_IsRebuiltBeforeItsOptionsAreSet()
    {
        var source = WithIndex(disabled: false);
        source.Indexes[0].FillFactor = 70;
        var target = WithIndex(disabled: true);

        var result = _differ.Diff(source, target, includeDrops: false);

        var rebuild = result.Script.IndexOf("ALTER INDEX [IX_Invoice_Qty] ON [dbo].[Invoice] REBUILD;", StringComparison.Ordinal);
        var options = result.Script.IndexOf("REBUILD WITH (FILLFACTOR = 70);", StringComparison.Ordinal);
        Assert.True(rebuild >= 0, result.Script);
        Assert.True(options > rebuild, result.Script);
    }

    // ------------------------------------------- checks that stand on a column

    /// <summary>
    /// Error 5074: SQL Server refuses ALTER COLUMN while a check constraint mentions
    /// the column. Nothing in the catalog says which columns a check touches, so the
    /// differ reads the expression.
    /// </summary>
    [Fact]
    public void CheckConstraintOnARewrittenColumn_ComesDownAndGoesBackUp()
    {
        var source = Table("Invoice", NVarchar("Sku", 120));
        source.CheckConstraints.Add(Check("CK_Invoice_Sku", "(len([Sku])>(0))"));
        var target = Table("Invoice", NVarchar("Sku", 60));
        target.CheckConstraints.Add(Check("CK_Invoice_Sku", "(len([Sku])>(0))"));

        var result = _differ.Diff(source, target, includeDrops: false);

        var drop = result.Script.IndexOf("DROP CONSTRAINT [CK_Invoice_Sku];", StringComparison.Ordinal);
        var alter = result.Script.IndexOf("ALTER COLUMN [Sku] nvarchar(120)", StringComparison.Ordinal);
        var add = result.Script.IndexOf("ADD CONSTRAINT [CK_Invoice_Sku] CHECK", StringComparison.Ordinal);
        Assert.True(drop >= 0, result.Script);
        Assert.True(alter > drop, result.Script);
        Assert.True(add > alter, result.Script);
    }

    /// <summary>The extractor brackets identifiers, but a hand-written check need not.</summary>
    [Fact]
    public void CheckConstraintNamingTheColumnWithoutBrackets_StillCountsAsAReference()
    {
        var source = Table("Invoice", NVarchar("Sku", 120));
        source.CheckConstraints.Add(Check("CK_Invoice_Sku", "(LEN(Sku) > 0)"));
        var target = Table("Invoice", NVarchar("Sku", 60));
        target.CheckConstraints.Add(Check("CK_Invoice_Sku", "(LEN(Sku) > 0)"));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("DROP CONSTRAINT [CK_Invoice_Sku];", result.Script);
    }

    [Fact]
    public void CheckConstraintOnAnotherColumn_IsLeftWhereItIs()
    {
        var source = Table("Invoice", NVarchar("Sku", 120), Col("Qty", "int"));
        source.CheckConstraints.Add(Check("CK_Invoice_Qty", "([Qty]>(0))"));
        var target = Table("Invoice", NVarchar("Sku", 60), Col("Qty", "int"));
        target.CheckConstraints.Add(Check("CK_Invoice_Qty", "([Qty]>(0))"));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("ALTER COLUMN [Sku] nvarchar(120)", result.Script);
        Assert.DoesNotContain("CK_Invoice_Qty", result.Script);
    }

    /// <summary>
    /// A literal that happens to spell a column name is a value, not a reference, and
    /// dropping the constraint over it would be needless work on a big table.
    /// </summary>
    [Fact]
    public void ColumnNameInsideAStringLiteral_IsNotAReference()
    {
        var source = Table("Invoice", NVarchar("Sku", 120), NVarchar("Kind", 20));
        source.CheckConstraints.Add(Check("CK_Invoice_Kind", "([Kind]<>'Sku')"));
        var target = Table("Invoice", NVarchar("Sku", 60), NVarchar("Kind", 20));
        target.CheckConstraints.Add(Check("CK_Invoice_Kind", "([Kind]<>'Sku')"));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("ALTER COLUMN [Sku] nvarchar(120)", result.Script);
        Assert.DoesNotContain("CK_Invoice_Kind", result.Script);
    }

    // --------------------------------------- names the server made up for itself

    /// <summary>
    /// A constraint SQL Server named carries a per-database random suffix, so the name
    /// in the snapshot belongs to the machine it was read from. Naming it in a
    /// <c>NOCHECK</c> on the target either fails or hits whatever else answers to it -
    /// which is why until 1.7 the renderers emitted nothing at all. The cost of that
    /// was silent: a constraint the source had switched off came back enforcing, and
    /// the script did not mention it. The name is now resolved on the server instead.
    /// </summary>
    [Fact]
    public void DisabledServerNamedCheck_IsAddedAndThenDisabledByTheNameTheServerGave()
    {
        var script = ScriptComposer.ComposeFullScript(Snapshot("Db", TableObject(ServerNamedCheck(disabled: true))));

        AssertOrder(script,
            "ALTER TABLE [dbo].[Invoice] WITH CHECK ADD CHECK ([Qty]>(0));",
            "SELECT @matches = COUNT(*), @name = MIN(cc.name)",
            "WHERE cc.parent_object_id = OBJECT_ID(N'[dbo].[Invoice]')",
            "AND cc.is_system_named = 1",
            "IF @matches = 1",
            "SET @sql = N'ALTER TABLE [dbo].[Invoice] NOCHECK CONSTRAINT ' + QUOTENAME(@name);",
            "EXEC sys.sp_executesql @sql;");

        // Never by the name it answered to on the machine it was read from.
        Assert.DoesNotContain("NOCHECK CONSTRAINT [CK__Invoice__Qty__1A2B3C4D]", script);
    }

    [Fact]
    public void DisabledServerNamedForeignKey_IsAddedAndThenDisabledByTheNameTheServerGave()
    {
        var script = ScriptComposer.ComposeFullScript(
            Snapshot("Db", TableObject(ServerNamedForeignKey(disabled: true))));

        AssertOrder(script,
            "ALTER TABLE [dbo].[Invoice] WITH CHECK ADD FOREIGN KEY ([CustomerId]) REFERENCES [dbo].[Customer] ([Id]);",
            "SELECT @matches = COUNT(*), @name = MIN(fk.name)",
            "WHERE fk.parent_object_id = OBJECT_ID(N'[dbo].[Invoice]')",
            "AND fk.referenced_object_id = OBJECT_ID(N'[dbo].[Customer]')",
            "FROM (VALUES (1, N'CustomerId', N'Id'))",
            "IF @matches = 1",
            "SET @sql = N'ALTER TABLE [dbo].[Invoice] NOCHECK CONSTRAINT ' + QUOTENAME(@name);",
            "EXEC sys.sp_executesql @sql;");

        Assert.DoesNotContain("NOCHECK CONSTRAINT [FK__Invoice__Custom__2B3C4D5E]", script);
    }

    /// <summary>
    /// <see cref="SqlRender.BuildTableCreateScript"/> is the other renderer with the
    /// same job - it is what a snapshot stores as a table's definition, and so what the
    /// differ emits for a table the target does not have at all.
    /// </summary>
    [Fact]
    public void DisabledServerNamedConstraints_AreResolvedInTheStandaloneCreateScriptToo()
    {
        AssertOrder(SqlRender.BuildTableCreateScript(ServerNamedCheck(disabled: true)),
            "ADD CHECK ([Qty]>(0));",
            "FROM sys.check_constraints AS cc",
            "SET @sql = N'ALTER TABLE [dbo].[Invoice] NOCHECK CONSTRAINT ' + QUOTENAME(@name);",
            "EXEC sys.sp_executesql @sql;");

        AssertOrder(SqlRender.BuildTableCreateScript(ServerNamedForeignKey(disabled: true)),
            "ADD FOREIGN KEY ([CustomerId])",
            "FROM sys.foreign_keys AS fk",
            "SET @sql = N'ALTER TABLE [dbo].[Invoice] NOCHECK CONSTRAINT ' + QUOTENAME(@name);",
            "EXEC sys.sp_executesql @sql;");
    }

    /// <summary>
    /// The lookup is only allowed to act on a single answer. Two constraints of the
    /// same shape, or none, and it does nothing and says so at run time: disabling the
    /// wrong constraint is worse than leaving one enabled and complaining about it.
    /// </summary>
    [Fact]
    public void ResolvedDisable_DoesNothingUnlessItMatchesExactlyOneConstraint()
    {
        var script = ScriptComposer.ComposeFullScript(Snapshot("Db", TableObject(ServerNamedCheck(disabled: true))));

        AssertOrder(script,
            "IF @matches = 1",
            "SET @sql = N'ALTER TABLE [dbo].[Invoice] NOCHECK CONSTRAINT ' + QUOTENAME(@name);",
            "EXEC sys.sp_executesql @sql;",
            "ELSE",
            "PRINT N'-- WARNING: the CHECK constraint on [dbo].[Invoice] matched on '",
            "+ N' constraint(s), not one, so it was left enabled.';");
    }

    /// <summary>
    /// An enabled constraint has nothing to switch off, server-named or not. Emitting
    /// the lookup anyway would be a batch that reads as if something were wrong.
    /// </summary>
    [Fact]
    public void EnabledServerNamedConstraints_ProduceNoDisableAtAll()
    {
        var check = ScriptComposer.ComposeFullScript(Snapshot("Db", TableObject(ServerNamedCheck(disabled: false))));
        var foreignKey = ScriptComposer.ComposeFullScript(
            Snapshot("Db", TableObject(ServerNamedForeignKey(disabled: false))));

        Assert.Contains("ADD CHECK ([Qty]>(0));", check);
        Assert.Contains("ADD FOREIGN KEY ([CustomerId])", foreignKey);

        foreach(var script in new[] { check, foreignKey })
        {
            Assert.DoesNotContain("NOCHECK CONSTRAINT", script);
            Assert.DoesNotContain("sys.check_constraints", script);
            Assert.DoesNotContain("sys.foreign_keys", script);
            Assert.DoesNotContain("@matches", script);
        }
    }

    /// <summary>
    /// A constraint with a name of its own is still switched off by that name: it is
    /// stable across databases, and one ALTER reads better than fifteen lines of
    /// catalog lookup.
    /// </summary>
    [Fact]
    public void DisabledConstraintWithANameOfItsOwn_IsStillSwitchedOffByThatName()
    {
        var script = ScriptComposer.ComposeFullScript(
            Snapshot("Db", TableObject(WithCheck(disabled: true, notTrusted: false))));

        Assert.Contains("ALTER TABLE [dbo].[Invoice] NOCHECK CONSTRAINT [CK_Invoice_Qty];", script);
        Assert.DoesNotContain("sys.check_constraints", script);
    }

    // ------------------------------------------------------------------ builders

    private static SqlSchemaDiff.Models.TableModel WithForeignKey(bool disabled, bool notTrusted)
    {
        var table = Table("Invoice", Col("Id", nullable: false), Col("CustomerId", "int"));
        var foreignKey = ForeignKey("FK_Invoice_Customer", "Customer", "CustomerId");
        foreignKey.IsDisabled = disabled;
        foreignKey.IsNotTrusted = notTrusted;
        table.ForeignKeys.Add(foreignKey);
        return table;
    }

    private static SqlSchemaDiff.Models.TableModel WithCheck(bool disabled, bool notTrusted)
    {
        var table = Table("Invoice", Col("Id", nullable: false), Col("Qty", "int"));
        var check = Check("CK_Invoice_Qty", "([Qty]>(0))");
        check.IsDisabled = disabled;
        check.IsNotTrusted = notTrusted;
        table.CheckConstraints.Add(check);
        return table;
    }

    /// <summary>The check constraint of <see cref="WithCheck"/>, named by the server.</summary>
    private static SqlSchemaDiff.Models.TableModel ServerNamedCheck(bool disabled)
    {
        var table = Table("Invoice", Col("Id", nullable: false), Col("Qty", "int"));
        var check = Check("CK__Invoice__Qty__1A2B3C4D", "([Qty]>(0))");
        check.IsSystemNamed = true;
        check.IsDisabled = disabled;
        table.CheckConstraints.Add(check);
        return table;
    }

    /// <summary>The foreign key of <see cref="WithForeignKey"/>, named by the server.</summary>
    private static SqlSchemaDiff.Models.TableModel ServerNamedForeignKey(bool disabled)
    {
        var table = Table("Invoice", Col("Id", nullable: false), Col("CustomerId", "int"));
        var foreignKey = ForeignKey("FK__Invoice__Custom__2B3C4D5E", "Customer", "CustomerId");
        foreignKey.IsSystemNamed = true;
        foreignKey.IsDisabled = disabled;
        table.ForeignKeys.Add(foreignKey);
        return table;
    }

    /// <summary>
    /// Asserts the fragments appear, and appear in this order. The order is the point
    /// for a resolved disable: an ADD that lands after the lookup that is meant to find
    /// what it created disables nothing.
    /// </summary>
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

    private static SqlSchemaDiff.Models.TableModel WithIndex(bool disabled)
    {
        var table = Table("Invoice", Col("Id", nullable: false), Col("Qty", "int"));
        var index = Index("IX_Invoice_Qty", unique: false, "Qty");
        index.IsDisabled = disabled;
        table.Indexes.Add(index);
        return table;
    }
}
