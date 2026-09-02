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

    private static SqlSchemaDiff.Models.TableModel WithIndex(bool disabled)
    {
        var table = Table("Invoice", Col("Id", nullable: false), Col("Qty", "int"));
        var index = Index("IX_Invoice_Qty", unique: false, "Qty");
        index.IsDisabled = disabled;
        table.Indexes.Add(index);
        return table;
    }
}
