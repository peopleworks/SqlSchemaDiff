using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

public class TriggerRenderTests
{
    [Fact]
    public void DisableAndEnableNameBothTheTriggerAndItsParent()
    {
        var trigger = TriggerTestModels.Trigger();

        Assert.Equal(
            "DISABLE TRIGGER [dbo].[trg_Orders_Audit] ON [dbo].[Orders];",
            SqlRender.BuildTriggerDisable("dbo", "trg_Orders_Audit", trigger));
        Assert.Equal(
            "ENABLE TRIGGER [dbo].[trg_Orders_Audit] ON [dbo].[Orders];",
            SqlRender.BuildTriggerEnable("dbo", "trg_Orders_Audit", trigger));
    }

    [Fact]
    public void TheRewriterRecognisesATrigger()
    {
        const string definition = "CREATE TRIGGER dbo.trg ON dbo.Orders AFTER INSERT AS SELECT 1;";

        Assert.Equal(
            "CREATE OR ALTER TRIGGER dbo.trg ON dbo.Orders AFTER INSERT AS SELECT 1;",
            SqlModuleRewriter.ToCreateOrAlter(definition));
    }

    // CREATE OR ALTER TABLE is a syntax error, so the rewriter has to know which
    // kinds it may touch rather than rewriting any leading CREATE it finds.
    [Theory]
    [InlineData("CREATE TABLE dbo.T (Id int);")]
    [InlineData("CREATE SEQUENCE dbo.S AS bigint;")]
    [InlineData("CREATE TYPE dbo.T AS TABLE (Id int);")]
    public void TheRewriterLeavesKindsThatCannotBeAlteredAlone(string definition)
    {
        Assert.Equal(definition, SqlModuleRewriter.ToCreateOrAlter(definition));
    }
}

public class TriggerDifferTests
{
    private readonly SchemaDiffer _differ = new();

    [Fact]
    public void ANewTriggerIsCreated()
    {
        var source = Snapshot("Src", TriggerTestModels.Object());

        var result = _differ.Diff(source, Snapshot("Tgt"), false, false, false, false);

        Assert.Equal(1, result.Added);
        Assert.Contains("CREATE TRIGGER", result.Script);
        Assert.DoesNotContain("DISABLE TRIGGER", result.Script);
    }

    // The disabled state is not in the module text: creating the trigger enables it,
    // so the DISABLE has to follow every create of a disabled one.
    [Fact]
    public void CreatingADisabledTriggerDisablesItAfterwards()
    {
        var source = Snapshot("Src", TriggerTestModels.Object(disabled: true));

        var result = _differ.Diff(source, Snapshot("Tgt"), false, false, false, false);

        Assert.Contains("DISABLE TRIGGER [dbo].[trg_Orders_Audit] ON [dbo].[Orders];", result.Script);
        Assert.True(
            result.Script.IndexOf("CREATE TRIGGER", StringComparison.Ordinal) <
            result.Script.IndexOf("DISABLE TRIGGER", StringComparison.Ordinal),
            "the trigger has to exist before it can be disabled");
    }

    [Fact]
    public void AnIdenticalTriggerIsNotDrift()
    {
        var source = Snapshot("Src", TriggerTestModels.Object());
        var target = Snapshot("Tgt", TriggerTestModels.Object());

        Assert.False(_differ.Diff(source, target, false, false, false, false).HasChanges);
    }

    [Theory]
    [InlineData(true, "DISABLE TRIGGER [dbo].[trg_Orders_Audit] ON [dbo].[Orders];")]
    [InlineData(false, "ENABLE TRIGGER [dbo].[trg_Orders_Audit] ON [dbo].[Orders];")]
    public void AStateOnlyDifferenceIsFixedWithoutTouchingTheBody(bool sourceDisabled, string expected)
    {
        var source = Snapshot("Src", TriggerTestModels.Object(disabled: sourceDisabled));
        var target = Snapshot("Tgt", TriggerTestModels.Object(disabled: !sourceDisabled));

        var result = _differ.Diff(source, target, false, false, false, false);

        Assert.Equal(1, result.Changed);
        Assert.Contains(expected, result.Script);
        Assert.DoesNotContain("CREATE OR ALTER TRIGGER", result.Script);
    }

    [Fact]
    public void ABodyChangeBecomesCreateOrAlter()
    {
        var source = Snapshot("Src", TriggerTestModels.Object(body: "SELECT 1;"));
        var target = Snapshot("Tgt", TriggerTestModels.Object(body: "SELECT 99;"));

        var result = _differ.Diff(source, target, false, false, false, false);

        Assert.Equal(1, result.Changed);
        Assert.Contains("CREATE OR ALTER TRIGGER", result.Script);
    }

    // ALTER TRIGGER re-enables the trigger it touches, so a disabled trigger whose
    // body changed has to be disabled again afterwards.
    [Fact]
    public void ABodyChangeOnADisabledTriggerReDisablesIt()
    {
        var source = Snapshot("Src", TriggerTestModels.Object(body: "SELECT 1;", disabled: true));
        var target = Snapshot("Tgt", TriggerTestModels.Object(body: "SELECT 99;", disabled: true));

        var result = _differ.Diff(source, target, false, false, false, false);

        Assert.Contains("CREATE OR ALTER TRIGGER", result.Script);
        Assert.Contains("DISABLE TRIGGER [dbo].[trg_Orders_Audit] ON [dbo].[Orders];", result.Script);
    }

    [Fact]
    public void AddOnlySkipsAChangedTrigger()
    {
        var source = Snapshot("Src", TriggerTestModels.Object(disabled: true));
        var target = Snapshot("Tgt", TriggerTestModels.Object(disabled: false));

        var result = _differ.Diff(source, target, false, false, false, addOnly: true);

        Assert.Equal(1, result.Changed);
        Assert.Equal(1, result.Skipped);
        Assert.DoesNotContain("DISABLE TRIGGER", result.Script);
    }

    [Fact]
    public void ATargetOnlyTriggerIsDroppedOnlyWithIncludeDrops()
    {
        var target = Snapshot("Tgt", TriggerTestModels.Object());

        var kept = _differ.Diff(Snapshot("Src"), target, includeDrops: false, false, false, false);
        var dropped = _differ.Diff(Snapshot("Src"), target, includeDrops: true, false, false, false);

        Assert.Equal(0, kept.Removed);
        Assert.Equal(1, dropped.Removed);
        Assert.Contains("DROP TRIGGER [dbo].[trg_Orders_Audit];", dropped.Script);
    }

    [Fact]
    public void ATriggerIsCreatedAfterItsParentTable()
    {
        var table = TableObject(Table("Orders", Col("Id", nullable: false)));
        var source = Snapshot("Src", TriggerTestModels.Object(), table);

        var result = _differ.Diff(source, Snapshot("Tgt"), false, false, false, false);

        Assert.True(
            result.Script.IndexOf("CREATE TABLE", StringComparison.Ordinal) <
            result.Script.IndexOf("CREATE TRIGGER", StringComparison.Ordinal),
            "a trigger cannot be created before the table it hangs off");
    }

    // Drops run the other way round: the trigger goes before the table it is on.
    [Fact]
    public void ATriggerIsDroppedBeforeItsParentTable()
    {
        var table = TableObject(Table("Orders", Col("Id", nullable: false)));
        var target = Snapshot("Tgt", table, TriggerTestModels.Object());

        var result = _differ.Diff(Snapshot("Src"), target, includeDrops: true, includeTableDrops: true, false, false);

        Assert.True(
            result.Script.IndexOf("DROP TRIGGER", StringComparison.Ordinal) <
            result.Script.IndexOf("DROP TABLE", StringComparison.Ordinal),
            "dropping the table first would take the trigger with it");
    }

    // A pre-1.6 snapshot has no TriggerModel; the diff still has to work off text.
    [Fact]
    public void ATriggerWithoutAModelFallsBackToTextComparison()
    {
        var source = Snapshot("Src", new DbSchemaObject
        {
            Type = DbObjectType.Trigger, Schema = "dbo", Name = "trg", Definition = "CREATE TRIGGER dbo.trg ON dbo.T AFTER INSERT AS SELECT 1;"
        });
        var target = Snapshot("Tgt", new DbSchemaObject
        {
            Type = DbObjectType.Trigger, Schema = "dbo", Name = "trg", Definition = "CREATE TRIGGER dbo.trg ON dbo.T AFTER INSERT AS SELECT 2;"
        });

        var result = _differ.Diff(source, target, false, false, false, false);

        Assert.Equal(1, result.Changed);
        Assert.Contains("CREATE OR ALTER TRIGGER", result.Script);
    }
}

internal static class TriggerTestModels
{
    public static TriggerModel Trigger(
        string parentSchema = "dbo",
        string parentName = "Orders",
        bool disabled = false,
        bool insteadOf = false)
        => new()
        {
            ParentSchema = parentSchema,
            ParentName = parentName,
            IsDisabled = disabled,
            IsInsteadOf = insteadOf
        };

    public static DbSchemaObject Object(
        string schema = "dbo",
        string name = "trg_Orders_Audit",
        string body = "SELECT 1;",
        bool disabled = false,
        bool insteadOf = false)
    {
        var trigger = Trigger(disabled: disabled, insteadOf: insteadOf);
        var timing = insteadOf ? "INSTEAD OF DELETE" : "AFTER INSERT, UPDATE";
        return new DbSchemaObject
        {
            Type = DbObjectType.Trigger,
            Schema = schema,
            Name = name,
            Definition = $"CREATE TRIGGER {schema}.{name} ON {trigger.ParentSchema}.{trigger.ParentName} {timing} AS {body}",
            Dependencies = { DbSchemaObject.BuildKey(DbObjectType.Table, trigger.ParentSchema, trigger.ParentName) },
            Trigger = trigger
        };
    }
}
