using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

public class TableTypeRenderTests
{
    [Fact]
    public void RendersColumnsAndConstraintsInline()
    {
        var script = SqlRender.BuildTableTypeCreateScript(TableTypeTestModels.Full());

        Assert.Contains("CREATE TYPE [dbo].[OrderLineTvp] AS TABLE", script);
        Assert.Contains("    [LineId] int NOT NULL,", script);
        Assert.Contains("    [Sku] nvarchar(50) NOT NULL,", script);
        Assert.Contains("    PRIMARY KEY CLUSTERED ([LineId] ASC),", script);
        Assert.Contains("    UNIQUE NONCLUSTERED ([Sku] ASC),", script);
        Assert.Contains("    CHECK ([Qty]>(0))", script);
        Assert.EndsWith(");", script);
    }

    // Auto-generated constraint names carry a per-database random suffix; a table
    // type's constraints are always system-named, so the name is left off.
    [Fact]
    public void SystemNamedConstraintsAreRenderedWithoutTheirNames()
    {
        var script = SqlRender.BuildTableTypeCreateScript(TableTypeTestModels.Full());

        Assert.DoesNotContain("CONSTRAINT", script);
    }

    [Fact]
    public void AnExplicitlyNamedConstraintKeepsItsName()
    {
        var model = TableTypeTestModels.Full();
        model.KeyConstraints[0].IsSystemNamed = false;
        model.KeyConstraints[0].Name = "PK_OrderLineTvp";

        var script = SqlRender.BuildTableTypeCreateScript(model);

        Assert.Contains("CONSTRAINT [PK_OrderLineTvp] PRIMARY KEY CLUSTERED ([LineId] ASC)", script);
    }

    [Fact]
    public void MemoryOptimizedTypesCarryTheirWithClause()
    {
        var model = TableTypeTestModels.Full();
        model.IsMemoryOptimized = true;

        Assert.EndsWith(") WITH (MEMORY_OPTIMIZED = ON);", SqlRender.BuildTableTypeCreateScript(model));
    }

    [Fact]
    public void ATypeWithNoConstraintsIsStillValid()
    {
        var model = TableTypeTestModels.Simple();

        Assert.Equal(
            "CREATE TYPE [dbo].[SmallTvp] AS TABLE" + Environment.NewLine +
            "(" + Environment.NewLine +
            "    [Id] int NOT NULL" + Environment.NewLine +
            ");",
            SqlRender.BuildTableTypeCreateScript(model));
    }
}

public class TableTypeDifferTests
{
    private readonly SchemaDiffer _differ = new();

    [Fact]
    public void ANewTableTypeIsCreated()
    {
        var source = Snapshot("Src", TableTypeTestModels.Object(TableTypeTestModels.Simple()));

        var result = _differ.Diff(source, Snapshot("Tgt"), false, false, false, false);

        Assert.Equal(1, result.Added);
        Assert.Contains("CREATE TYPE [dbo].[SmallTvp] AS TABLE", result.Script);
    }

    [Fact]
    public void AnIdenticalTableTypeIsNotDrift()
    {
        var source = Snapshot("Src", TableTypeTestModels.Object(TableTypeTestModels.Full()));
        var target = Snapshot("Tgt", TableTypeTestModels.Object(TableTypeTestModels.Full()));

        Assert.False(_differ.Diff(source, target, false, false, false, false).HasChanges);
    }

    // There is no ALTER for a table type, so every difference is a recreate.
    [Fact]
    public void AnyDifferenceBecomesDropAndCreate()
    {
        var changed = TableTypeTestModels.Simple();
        changed.Columns.Add(new ColumnModel { Name = "Extra", TypeSchema = "sys", TypeName = "int", IsNullable = true });

        var source = Snapshot("Src", TableTypeTestModels.Object(changed));
        var target = Snapshot("Tgt", TableTypeTestModels.Object(TableTypeTestModels.Simple()));

        var result = _differ.Diff(source, target, false, false, false, false);

        Assert.Equal(1, result.Changed);
        Assert.Contains("DROP TYPE [dbo].[SmallTvp];", result.Script);
        Assert.Contains("CREATE TYPE [dbo].[SmallTvp] AS TABLE", result.Script);
        Assert.Contains("a table type cannot be altered", result.Script);
        Assert.True(
            result.Script.IndexOf("DROP TYPE", StringComparison.Ordinal) <
            result.Script.IndexOf("CREATE TYPE", StringComparison.Ordinal));
    }

    // DROP TYPE fails while a module still references the type, so the warning has
    // to say which modules the caller must deal with.
    [Fact]
    public void TheRecreateWarningNamesTheModulesThatUseTheType()
    {
        var changed = TableTypeTestModels.Simple();
        changed.Columns[0].IsNullable = true;

        var procedure = new DbSchemaObject
        {
            Type = DbObjectType.StoredProcedure,
            Schema = "dbo",
            Name = "usp_UsesSmall",
            Definition = "CREATE PROCEDURE dbo.usp_UsesSmall @s dbo.SmallTvp READONLY AS SELECT 1;",
            Dependencies = { DbSchemaObject.BuildKey(DbObjectType.TableType, "dbo", "SmallTvp") }
        };

        var source = Snapshot("Src", TableTypeTestModels.Object(changed), procedure);
        var target = Snapshot("Tgt", TableTypeTestModels.Object(TableTypeTestModels.Simple()), procedure);

        var result = _differ.Diff(source, target, false, false, false, false);

        Assert.Contains("DROP TYPE fails while any module still references it", result.Script);
        Assert.Contains("-- Referenced by: [dbo].[usp_UsesSmall]", result.Script);
    }

    [Fact]
    public void AddOnlySkipsAChangedTableType()
    {
        var changed = TableTypeTestModels.Simple();
        changed.Columns[0].IsNullable = true;

        var source = Snapshot("Src", TableTypeTestModels.Object(changed));
        var target = Snapshot("Tgt", TableTypeTestModels.Object(TableTypeTestModels.Simple()));

        var result = _differ.Diff(source, target, false, false, false, addOnly: true);

        Assert.Equal(1, result.Changed);
        Assert.Equal(1, result.Skipped);
        Assert.DoesNotContain("DROP TYPE", result.Script);
    }

    // OBJECT_ID never finds a type, so the guard has to be TYPE_ID.
    [Fact]
    public void ATargetOnlyTableTypeIsDroppedThroughTypeId()
    {
        var target = Snapshot("Tgt", TableTypeTestModels.Object(TableTypeTestModels.Simple()));

        var kept = _differ.Diff(Snapshot("Src"), target, includeDrops: false, false, false, false);
        var dropped = _differ.Diff(Snapshot("Src"), target, includeDrops: true, false, false, false);

        Assert.Equal(0, kept.Removed);
        Assert.Equal(1, dropped.Removed);
        Assert.Contains("IF TYPE_ID(N'[dbo].[SmallTvp]') IS NOT NULL", dropped.Script);
        Assert.DoesNotContain("OBJECT_ID(N'[dbo].[SmallTvp]')", dropped.Script);
    }

    [Fact]
    public void ATableTypeIsCreatedBeforeTheProcedureThatTakesIt()
    {
        var procedure = new DbSchemaObject
        {
            Type = DbObjectType.StoredProcedure,
            Schema = "dbo",
            Name = "usp_UsesSmall",
            Definition = "CREATE PROCEDURE dbo.usp_UsesSmall @s dbo.SmallTvp READONLY AS SELECT 1;",
            Dependencies = { DbSchemaObject.BuildKey(DbObjectType.TableType, "dbo", "SmallTvp") }
        };

        var source = Snapshot("Src", procedure, TableTypeTestModels.Object(TableTypeTestModels.Simple()));

        var result = _differ.Diff(source, Snapshot("Tgt"), false, false, false, false);

        Assert.True(
            result.Script.IndexOf("CREATE TYPE", StringComparison.Ordinal) <
            result.Script.IndexOf("CREATE PROCEDURE", StringComparison.Ordinal),
            "a table-valued parameter needs its type to exist first");
    }

    [Fact]
    public void ATableTypeIsDroppedAfterTheProcedureThatTakesIt()
    {
        var procedure = new DbSchemaObject
        {
            Type = DbObjectType.StoredProcedure,
            Schema = "dbo",
            Name = "usp_UsesSmall",
            Definition = "CREATE PROCEDURE dbo.usp_UsesSmall @s dbo.SmallTvp READONLY AS SELECT 1;",
            Dependencies = { DbSchemaObject.BuildKey(DbObjectType.TableType, "dbo", "SmallTvp") }
        };

        var target = Snapshot("Tgt", TableTypeTestModels.Object(TableTypeTestModels.Simple()), procedure);

        var result = _differ.Diff(Snapshot("Src"), target, includeDrops: true, false, false, false);

        Assert.True(
            result.Script.IndexOf("DROP PROCEDURE", StringComparison.Ordinal) <
            result.Script.IndexOf("DROP TYPE", StringComparison.Ordinal),
            "the type cannot be dropped while the procedure still takes it");
    }
}

public class TableTypeFilterTests
{
    private static DbSchemaObject Obj(DbObjectType type, string name) => new()
    {
        Type = type, Schema = "dbo", Name = name, Definition = string.Empty
    };

    [Theory]
    [InlineData("trigger:*", DbObjectType.Trigger)]
    [InlineData("triggers:*", DbObjectType.Trigger)]
    [InlineData("sequence:*", DbObjectType.Sequence)]
    [InlineData("seq:*", DbObjectType.Sequence)]
    [InlineData("tabletype:*", DbObjectType.TableType)]
    [InlineData("type:*", DbObjectType.TableType)]
    [InlineData("types:*", DbObjectType.TableType)]
    [InlineData("tvp:*", DbObjectType.TableType)]
    public void TheNewTypePrefixesNarrowToOneKind(string pattern, DbObjectType expected)
    {
        var filter = ObjectFilter.Parse(pattern, null);

        Assert.True(filter.ShouldInclude(Obj(expected, "Anything")));
        Assert.False(filter.ShouldInclude(Obj(DbObjectType.Table, "Anything")));
    }

    [Fact]
    public void ABareNewTypePrefixMeansEveryObjectOfThatKind()
    {
        var filter = ObjectFilter.Parse(null, "trigger:");

        Assert.False(filter.ShouldInclude(Obj(DbObjectType.Trigger, "trg_Anything")));
        Assert.True(filter.ShouldInclude(Obj(DbObjectType.Table, "trg_Anything")));
    }

    [Fact]
    public void FilteringHidesTheNewTypesFromBothSides()
    {
        var filter = ObjectFilter.Parse(null, "sequence:");
        var target = filter.Apply(Snapshot("Tgt", SequenceTestModels.Object(SequenceTestModels.Sequence())));

        var result = new SchemaDiffer().Diff(filter.Apply(Snapshot("Src")), target, true, true, false, false);

        Assert.Equal(0, result.Removed);
        Assert.DoesNotContain("OrderNumbers", result.Script);
    }
}

internal static class TableTypeTestModels
{
    public static TableTypeModel Simple() => new()
    {
        Schema = "dbo",
        Name = "SmallTvp",
        Columns = { new ColumnModel { Name = "Id", TypeSchema = "sys", TypeName = "int", IsNullable = false } }
    };

    public static TableTypeModel Full() => new()
    {
        Schema = "dbo",
        Name = "OrderLineTvp",
        Columns =
        {
            new ColumnModel { Name = "LineId", TypeSchema = "sys", TypeName = "int", IsNullable = false },
            new ColumnModel { Name = "Sku", TypeSchema = "sys", TypeName = "nvarchar", MaxLength = 100, IsNullable = false },
            new ColumnModel { Name = "Qty", TypeSchema = "sys", TypeName = "int", IsNullable = false }
        },
        KeyConstraints =
        {
            new KeyConstraintModel
            {
                TypeCode = "PK",
                Name = "PK__TT_Order__2EAE6529",
                IsSystemNamed = true,
                IndexTypeDesc = "CLUSTERED",
                Columns = { new IndexColumnModel { Name = "LineId", KeyOrdinal = 1, IndexColumnId = 1 } }
            },
            new KeyConstraintModel
            {
                TypeCode = "UQ",
                Name = "UQ__TT_Order__CA1FD3C5",
                IsSystemNamed = true,
                IndexTypeDesc = "NONCLUSTERED",
                Columns = { new IndexColumnModel { Name = "Sku", KeyOrdinal = 1, IndexColumnId = 1 } }
            }
        },
        CheckConstraints =
        {
            new CheckConstraintModel { Name = "CK__TT_OrderLin__Qty", IsSystemNamed = true, Definition = "([Qty]>(0))" }
        }
    };

    public static DbSchemaObject Object(TableTypeModel tableType) => new()
    {
        Type = DbObjectType.TableType,
        Schema = tableType.Schema,
        Name = tableType.Name,
        Definition = SqlRender.BuildTableTypeCreateScript(tableType),
        TableType = tableType
    };
}
