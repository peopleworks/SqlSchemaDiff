using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

public class SequenceRenderTests
{
    [Fact]
    public void RendersEveryClause()
    {
        var script = SqlRender.BuildSequenceCreate(SequenceTestModels.Sequence());

        Assert.Equal(
            "CREATE SEQUENCE [dbo].[OrderNumbers] AS bigint START WITH 100 INCREMENT BY 5 " +
            "MINVALUE 1 MAXVALUE 999999 CYCLE CACHE 20;",
            script);
    }

    [Fact]
    public void AbsentBoundsAndCachingRenderAsTheirNegatives()
    {
        var sequence = SequenceTestModels.Sequence(minValue: null, maxValue: null, cycling: false, cached: false, cacheSize: null);

        var script = SqlRender.BuildSequenceCreate(sequence);

        Assert.Contains("NO MINVALUE NO MAXVALUE NO CYCLE NO CACHE;", script);
    }

    [Fact]
    public void CachedWithoutASizeLetsTheServerChooseOne()
    {
        var sequence = SequenceTestModels.Sequence(cached: true, cacheSize: null);

        Assert.EndsWith(" CACHE;", SqlRender.BuildSequenceCreate(sequence));
    }

    // decimal(38,0) is a legal sequence type and its bounds run to 10^38-1, past
    // both long and decimal. Carrying the values as text is what keeps them exact.
    [Fact]
    public void ADecimalSequenceKeepsAllThirtyEightDigits()
    {
        const string maximum = "99999999999999999999999999999999999999";
        var sequence = SequenceTestModels.Sequence(
            typeName: "decimal", precision: 38, scale: 0, minValue: "-" + maximum, maxValue: maximum);

        var script = SqlRender.BuildSequenceCreate(sequence);

        Assert.Contains("AS decimal(38,0)", script);
        Assert.Contains($"MAXVALUE {maximum}", script);
        Assert.Contains($"MINVALUE -{maximum}", script);
    }

    // The bounds are text that goes straight into generated DDL, so anything that
    // is not an integer literal is dropped rather than pasted into a statement.
    [Fact]
    public void ABoundThatIsNotANumberIsNeverPastedIntoTheScript()
    {
        var sequence = SequenceTestModels.Sequence(maxValue: "1); DROP TABLE Orders; --");

        var script = SqlRender.BuildSequenceCreate(sequence);

        Assert.DoesNotContain("DROP TABLE", script);
        Assert.Contains("NO MAXVALUE", script);
    }

    // current_value is the last value handed out, so restarting at it would hand the
    // same number out twice. One increment past it can only ever skip a value.
    [Fact]
    public void RestartResumesOneIncrementPastTheCapturedValue()
    {
        var sequence = SequenceTestModels.Sequence(currentValue: "105");

        Assert.Equal(
            "ALTER SEQUENCE [dbo].[OrderNumbers] RESTART WITH 110;",
            SqlRender.BuildSequenceRestart(sequence));
    }

    [Fact]
    public void RestartIsClampedToTheSequenceCeiling()
    {
        var sequence = SequenceTestModels.Sequence(currentValue: "999999", maxValue: "999999");

        Assert.Equal(
            "ALTER SEQUENCE [dbo].[OrderNumbers] RESTART WITH 999999;",
            SqlRender.BuildSequenceRestart(sequence));
    }

    [Fact]
    public void RestartIsNullWhenNothingWasCaptured()
    {
        Assert.Null(SqlRender.BuildSequenceRestart(SequenceTestModels.Sequence(currentValue: null)));
    }

    [Fact]
    public void RestartHandlesValuesWiderThanLong()
    {
        var sequence = SequenceTestModels.Sequence(
            typeName: "decimal", precision: 38, scale: 0,
            increment: "1", currentValue: "99999999999999999999999999999999999998",
            maxValue: "99999999999999999999999999999999999999");

        Assert.Equal(
            "ALTER SEQUENCE [dbo].[OrderNumbers] RESTART WITH 99999999999999999999999999999999999999;",
            SqlRender.BuildSequenceRestart(sequence));
    }
}

public class SequenceDifferTests
{
    private readonly SchemaDiffer _differ = new();

    [Fact]
    public void ANewSequenceIsCreated()
    {
        var source = Snapshot("Src", SequenceTestModels.Object(SequenceTestModels.Sequence()));

        var result = _differ.Diff(source, Snapshot("Tgt"), false, false, false, false);

        Assert.Equal(1, result.Added);
        Assert.Contains("CREATE SEQUENCE [dbo].[OrderNumbers]", result.Script);
    }

    [Fact]
    public void AlterableDifferencesBecomeOneAlterSequence()
    {
        var source = Snapshot("Src", SequenceTestModels.Object(SequenceTestModels.Sequence()));
        var target = Snapshot("Tgt", SequenceTestModels.Object(
            SequenceTestModels.Sequence(increment: "1", cycling: false, cacheSize: 50)));

        var result = _differ.Diff(source, target, false, false, false, false);

        Assert.Equal(1, result.Changed);
        Assert.Contains("ALTER SEQUENCE [dbo].[OrderNumbers] INCREMENT BY 5 CYCLE CACHE 20;", result.Script);
        Assert.DoesNotContain("DROP SEQUENCE", result.Script);
    }

    [Fact]
    public void DroppedBoundsAlterToTheirNegatives()
    {
        var source = Snapshot("Src", SequenceTestModels.Object(
            SequenceTestModels.Sequence(minValue: null, maxValue: null)));
        var target = Snapshot("Tgt", SequenceTestModels.Object(SequenceTestModels.Sequence()));

        var result = _differ.Diff(source, target, false, false, false, false);

        Assert.Contains("NO MINVALUE NO MAXVALUE", result.Script);
    }

    // The current value moves every time the sequence is used. That is not drift.
    [Fact]
    public void ADifferentCurrentValueIsNotAChange()
    {
        var source = Snapshot("Src", SequenceTestModels.Object(SequenceTestModels.Sequence(currentValue: "500")));
        var target = Snapshot("Tgt", SequenceTestModels.Object(SequenceTestModels.Sequence(currentValue: "100")));

        var result = _differ.Diff(source, target, false, false, false, false);

        Assert.Equal(0, result.Changed);
        Assert.False(result.HasChanges);
    }

    [Theory]
    [InlineData("int", "100", "type")]
    [InlineData("bigint", "1", "start value")]
    public void TypeAndStartValueChangesForceARecreate(string typeName, string startValue, string expectedReason)
    {
        var source = Snapshot("Src", SequenceTestModels.Object(SequenceTestModels.Sequence()));
        var target = Snapshot("Tgt", SequenceTestModels.Object(
            SequenceTestModels.Sequence(typeName: typeName, startValue: startValue)));

        var result = _differ.Diff(source, target, false, false, false, false);

        Assert.Equal(1, result.Changed);
        Assert.Contains("DROP SEQUENCE [dbo].[OrderNumbers]", result.Script);
        Assert.Contains("CREATE SEQUENCE [dbo].[OrderNumbers]", result.Script);
        Assert.Contains($"is recreated ({expectedReason}", result.Script);
        Assert.True(
            result.Script.IndexOf("DROP SEQUENCE", StringComparison.Ordinal) <
            result.Script.IndexOf("CREATE SEQUENCE", StringComparison.Ordinal),
            "the old sequence has to go before the new one is created");
    }

    [Fact]
    public void AddOnlySkipsAChangedSequence()
    {
        var source = Snapshot("Src", SequenceTestModels.Object(SequenceTestModels.Sequence()));
        var target = Snapshot("Tgt", SequenceTestModels.Object(SequenceTestModels.Sequence(increment: "1")));

        var result = _differ.Diff(source, target, false, false, false, addOnly: true);

        Assert.Equal(1, result.Changed);
        Assert.Equal(1, result.Skipped);
        Assert.DoesNotContain("ALTER SEQUENCE", result.Script);
    }

    [Fact]
    public void ATargetOnlySequenceIsDroppedOnlyWithIncludeDrops()
    {
        var target = Snapshot("Tgt", SequenceTestModels.Object(SequenceTestModels.Sequence()));

        var kept = _differ.Diff(Snapshot("Src"), target, includeDrops: false, false, false, false);
        var dropped = _differ.Diff(Snapshot("Src"), target, includeDrops: true, false, false, false);

        Assert.Equal(0, kept.Removed);
        Assert.Equal(1, dropped.Removed);
        Assert.Contains("IF OBJECT_ID(N'[dbo].[OrderNumbers]') IS NOT NULL", dropped.Script);
        Assert.Contains("DROP SEQUENCE [dbo].[OrderNumbers];", dropped.Script);
    }

    // The whole point of the sequence edge: a table whose column defaults to
    // NEXT VALUE FOR cannot be created before the sequence it draws from.
    [Fact]
    public void ASequenceIsCreatedBeforeTheTableThatDefaultsToIt()
    {
        var table = Table("Orders", Col("Id", nullable: false), new ColumnModel
        {
            Name = "OrderNo",
            TypeSchema = "sys",
            TypeName = "bigint",
            IsNullable = false,
            DefaultName = "DF_Orders_OrderNo",
            DefaultDefinition = "(NEXT VALUE FOR [dbo].[OrderNumbers])"
        });

        var tableObject = new DbSchemaObject
        {
            Type = DbObjectType.Table,
            Schema = table.Schema,
            Name = table.Name,
            Definition = SqlRender.BuildTableCreateScript(table),
            Dependencies = { DbSchemaObject.BuildKey(DbObjectType.Sequence, "dbo", "OrderNumbers") },
            Table = table
        };

        var source = Snapshot("Src", tableObject, SequenceTestModels.Object(SequenceTestModels.Sequence()));

        var result = _differ.Diff(source, Snapshot("Tgt"), false, false, false, false);

        Assert.True(
            result.Script.IndexOf("CREATE SEQUENCE", StringComparison.Ordinal) <
            result.Script.IndexOf("CREATE TABLE", StringComparison.Ordinal),
            "the sequence must exist before the table whose default draws from it");
    }
}

public class SequenceReferenceFinderTests
{
    private static readonly List<SequenceModel> Sequences = new()
    {
        SequenceTestModels.Sequence(schema: "dbo", name: "OrderNumbers"),
        SequenceTestModels.Sequence(schema: "dbo", name: "PlainSeq"),
        SequenceTestModels.Sequence(schema: "app", name: "AppSeq")
    };

    [Theory]
    [InlineData("(NEXT VALUE FOR [dbo].[OrderNumbers])", "dbo", "OrderNumbers")]
    [InlineData("(NEXT VALUE FOR dbo.OrderNumbers)", "dbo", "OrderNumbers")]
    [InlineData("(NEXT VALUE FOR [PlainSeq])", null, "PlainSeq")]
    [InlineData("(NEXT VALUE FOR PlainSeq)", null, "PlainSeq")]
    [InlineData("(next  value\nfor [dbo] . [OrderNumbers])", "dbo", "OrderNumbers")]
    [InlineData("(NEXT VALUE FOR [MyDb].[dbo].[OrderNumbers])", "dbo", "OrderNumbers")]
    public void ParsesEveryShapeTheCatalogStores(string definition, string? schema, string name)
    {
        var found = Assert.Single(SequenceReferenceFinder.Find(definition));

        Assert.Equal(schema, found.Schema);
        Assert.Equal(name, found.Name);
    }

    [Fact]
    public void NothingIsFoundInAnOrdinaryDefault()
    {
        Assert.Empty(SequenceReferenceFinder.Find("((0))"));
        Assert.Empty(SequenceReferenceFinder.Find(null));
    }

    [Fact]
    public void AnUnqualifiedNameResolvesAgainstTheTablesOwnSchemaFirst()
    {
        var sequences = new List<SequenceModel>
        {
            SequenceTestModels.Sequence(schema: "dbo", name: "Shared"),
            SequenceTestModels.Sequence(schema: "app", name: "Shared")
        };

        var keys = SequenceReferenceFinder.FindDependencyKeys("(NEXT VALUE FOR Shared)", sequences, "app");

        Assert.Equal(new[] { "Sequence:app.Shared" }, keys);
    }

    [Fact]
    public void AQualifiedNameIgnoresTheDefaultSchema()
    {
        var keys = SequenceReferenceFinder.FindDependencyKeys("(NEXT VALUE FOR [app].[AppSeq])", Sequences, "dbo");

        Assert.Equal(new[] { "Sequence:app.AppSeq" }, keys);
    }

    [Fact]
    public void AReferenceToASequenceThatIsNotInTheSnapshotIsDropped()
    {
        Assert.Empty(SequenceReferenceFinder.FindDependencyKeys("(NEXT VALUE FOR [dbo].[Missing])", Sequences, "dbo"));
    }
}

internal static class SequenceTestModels
{
    public static SequenceModel Sequence(
        string schema = "dbo",
        string name = "OrderNumbers",
        string typeName = "bigint",
        byte precision = 19,
        byte scale = 0,
        string startValue = "100",
        string increment = "5",
        string? minValue = "1",
        string? maxValue = "999999",
        bool cycling = true,
        bool cached = true,
        int? cacheSize = 20,
        string? currentValue = "100")
        => new()
        {
            Schema = schema,
            Name = name,
            TypeName = typeName,
            Precision = precision,
            Scale = scale,
            StartValue = startValue,
            Increment = increment,
            MinValue = minValue,
            MaxValue = maxValue,
            IsCycling = cycling,
            IsCached = cached,
            CacheSize = cacheSize,
            CurrentValue = currentValue
        };

    public static DbSchemaObject Object(SequenceModel sequence) => new()
    {
        Type = DbObjectType.Sequence,
        Schema = sequence.Schema,
        Name = sequence.Name,
        Definition = SqlRender.BuildSequenceCreate(sequence),
        Sequence = sequence
    };
}
