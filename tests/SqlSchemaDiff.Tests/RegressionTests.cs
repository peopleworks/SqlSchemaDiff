using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

/// <summary>
/// Each test here pins a defect that was reproduced against a live SQL Server
/// before it was fixed. The comment on each one is the error the old build
/// produced.
/// </summary>
public class RegressionTests
{
    private readonly TableDiffer _differ = new();

    // "The specified schema name 'app' either does not exist or you do not have
    // permission to use it." — the script created a table in a schema the target
    // did not have.
    [Fact]
    public void Diff_CreatesMissingSchema_BeforeTheTableThatNeedsIt()
    {
        var table = Table("Customer", Col("Id", nullable: false));
        table.Schema = "app";
        var source = Snapshot("Source", TableObject(table));
        var target = Snapshot("Target");

        var result = new SchemaDiffer().Diff(source, target, false, false, false, false);

        Assert.Contains("IF SCHEMA_ID(N'app') IS NULL", result.Script);
        Assert.Contains("CREATE SCHEMA [app]", result.Script);
        Assert.True(
            result.Script.IndexOf("CREATE SCHEMA", StringComparison.Ordinal) <
            result.Script.IndexOf("[app].[Customer]", StringComparison.Ordinal),
            "the schema must be created before the table that lives in it");
    }

    [Fact]
    public void Diff_CreatesAliasTypesUsedByEmittedTables()
    {
        var table = Table("Employee", new ColumnModel
        {
            Name = "Phone",
            TypeSchema = "dbo",
            TypeName = "PhoneNumber",
            IsUserDefinedType = true,
            IsNullable = false
        });

        var source = Snapshot("Source", TableObject(table));
        source.Types.Add(new AliasTypeModel
        {
            Schema = "dbo",
            Name = "PhoneNumber",
            BaseTypeName = "varchar",
            MaxLength = 25,
            IsNullable = false
        });

        var result = new SchemaDiffer().Diff(source, Snapshot("Target"), false, false, false, false);

        Assert.Contains("IF TYPE_ID(N'dbo.PhoneNumber') IS NULL", result.Script);
        Assert.Contains("CREATE TYPE [dbo].[PhoneNumber] FROM varchar(25) NOT NULL;", result.Script);
    }

    [Fact]
    public void Diff_DoesNotEmitPrerequisites_WhenNothingIsCreated()
    {
        var table = Table("Customer", Col("Id", nullable: false));
        table.Schema = "app";
        var source = Snapshot("Source", TableObject(table));
        var target = Snapshot("Target", TableObject(table));

        var result = new SchemaDiffer().Diff(source, target, false, false, false, false);

        Assert.DoesNotContain("CREATE SCHEMA", result.Script);
    }

    // "COLLATE clause cannot be used on user-defined data types."
    [Fact]
    public void Column_OfAliasType_DoesNotRestateCollation()
    {
        var column = new ColumnModel
        {
            Name = "Phone",
            TypeSchema = "dbo",
            TypeName = "PhoneNumber",
            IsUserDefinedType = true,
            CollationName = "SQL_Latin1_General_CP1_CI_AS",
            IsNullable = false
        };

        var definition = SqlRender.BuildColumnDefinition(column);

        Assert.Equal("[Phone] [dbo].[PhoneNumber] NOT NULL", definition);
    }

    // "There is already an object named 'GetItems' in the database." — a header
    // comment ahead of CREATE stopped the CREATE OR ALTER rewrite.
    [Theory]
    [InlineData("CREATE PROCEDURE dbo.P AS SELECT 1;")]
    [InlineData("/* header */ CREATE PROCEDURE dbo.P AS SELECT 1;")]
    [InlineData("-- header\nCREATE PROCEDURE dbo.P AS SELECT 1;")]
    [InlineData("\r\n/* a */\r\n-- b\r\n  /* c */ CREATE   VIEW dbo.V AS SELECT 1 AS X;")]
    public void ToCreateOrAlter_RewritesTheFirstCreate_PastAnyLeadingComments(string definition)
    {
        var rewritten = SqlModuleRewriter.ToCreateOrAlter(definition);

        Assert.Contains("CREATE OR ALTER", rewritten);
        // The original text is preserved apart from the two inserted words.
        Assert.Equal(definition.Replace("CREATE", "CREATE OR ALTER"), rewritten);
    }

    [Fact]
    public void ToCreateOrAlter_LeavesAnExistingCreateOrAlterAlone()
    {
        const string definition = "CREATE OR ALTER PROCEDURE dbo.P AS SELECT 1;";

        Assert.Equal(definition, SqlModuleRewriter.ToCreateOrAlter(definition));
    }

    [Fact]
    public void ToCreateOrAlter_LeavesTextThatDoesNotStartWithCreateAlone()
    {
        const string definition = "ALTER PROCEDURE dbo.P AS SELECT 1;";

        Assert.Equal(definition, SqlModuleRewriter.ToCreateOrAlter(definition));
    }

    // "Table 'Item' already has a primary key defined on it." — the auto-generated
    // name differs per database, so the same PK looked like two different ones.
    [Fact]
    public void SystemNamedPrimaryKeys_WithTheSameShape_AreTheSameConstraint()
    {
        var source = Table("Orders", Col("Id", nullable: false));
        source.KeyConstraints.Add(SystemNamedPk("PK__Orders__3214EC07CF883821", "Id"));

        var target = Table("Orders", Col("Id", nullable: false));
        target.KeyConstraints.Add(SystemNamedPk("PK__Orders__3214EC073F741784", "Id"));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.False(result.HasChanges);
        Assert.Equal(0, result.WarningCount);
    }

    [Fact]
    public void SystemNamedConstraints_AreCreatedWithoutTheirGeneratedName()
    {
        var table = Table("Orders", Col("Id", nullable: false));
        var constraint = SystemNamedPk("PK__Orders__3214EC07CF883821", "Id");

        var sql = SqlRender.BuildKeyConstraintAdd(table, constraint);

        Assert.DoesNotContain("PK__Orders", sql);
        Assert.Equal("ALTER TABLE [dbo].[Orders] ADD PRIMARY KEY CLUSTERED ([Id] ASC);", sql);
    }

    [Fact]
    public void ExplicitlyNamedConstraints_KeepTheirName()
    {
        var table = Table("Orders", Col("Id", nullable: false));
        var constraint = SystemNamedPk("PK_Orders", "Id");
        constraint.IsSystemNamed = false;

        var sql = SqlRender.BuildKeyConstraintAdd(table, constraint);

        Assert.Equal("ALTER TABLE [dbo].[Orders] ADD CONSTRAINT [PK_Orders] PRIMARY KEY CLUSTERED ([Id] ASC);", sql);
    }

    // A clustered PK compared equal to a nonclustered one, because
    // "NONCLUSTERED".Contains("CLUSTERED") is true.
    [Fact]
    public void ChangingAPrimaryKeyBetweenClusteredAndNonclustered_IsDetected()
    {
        var source = Table("Region", Col("Code", nullable: false));
        var sourcePk = SystemNamedPk("PK_Region", "Code");
        sourcePk.IsSystemNamed = false;
        sourcePk.IndexTypeDesc = "NONCLUSTERED";
        source.KeyConstraints.Add(sourcePk);

        var target = Table("Region", Col("Code", nullable: false));
        var targetPk = SystemNamedPk("PK_Region", "Code");
        targetPk.IsSystemNamed = false;
        target.KeyConstraints.Add(targetPk); // CLUSTERED

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.True(result.HasChanges);
        Assert.Contains("DROP CONSTRAINT [PK_Region]", result.Script);
        Assert.Contains("PRIMARY KEY NONCLUSTERED", result.Script);
    }

    [Theory]
    [InlineData("CLUSTERED", true)]
    [InlineData("NONCLUSTERED", false)]
    [InlineData("clustered", true)]
    public void IsClustered_DoesNotTreatNonclusteredAsClustered(string typeDesc, bool expected)
        => Assert.Equal(expected, SqlRender.IsClustered(typeDesc));

    // "ALTER TABLE ALTER COLUMN Sku failed because one or more objects access this
    // column. The index 'IX_Item_Sku' is dependent on column 'Sku'."
    [Fact]
    public void ChangingAnIndexedColumn_DropsAndRecreatesTheIndexAroundTheAlter()
    {
        var source = Table("Item", Col("Id", nullable: false), NVarchar("Sku", 60, nullable: false));
        source.Indexes.Add(Index("IX_Item_Sku", unique: false, "Sku"));

        var target = Table("Item", Col("Id", nullable: false), NVarchar("Sku", 20, nullable: false));
        target.Indexes.Add(Index("IX_Item_Sku", unique: false, "Sku"));

        var result = _differ.Diff(source, target, includeDrops: false);

        var drop = result.Script.IndexOf("DROP INDEX [IX_Item_Sku]", StringComparison.Ordinal);
        var alter = result.Script.IndexOf("ALTER COLUMN [Sku]", StringComparison.Ordinal);
        var create = result.Script.IndexOf("CREATE NONCLUSTERED INDEX [IX_Item_Sku]", StringComparison.Ordinal);

        Assert.True(drop >= 0 && alter >= 0 && create >= 0, "all three statements must be present");
        Assert.True(drop < alter, "the index must be dropped before the column is altered");
        Assert.True(alter < create, "the index must be recreated after the column is altered");
    }

    [Fact]
    public void AnIndexThatDoesNotTouchTheAlteredColumn_IsLeftAlone()
    {
        var source = Table("Item", Col("Id", nullable: false), NVarchar("Sku", 60, nullable: false), Col("Qty"));
        source.Indexes.Add(Index("IX_Item_Qty", unique: false, "Qty"));

        var target = Table("Item", Col("Id", nullable: false), NVarchar("Sku", 20, nullable: false), Col("Qty"));
        target.Indexes.Add(Index("IX_Item_Qty", unique: false, "Qty"));

        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.DoesNotContain("DROP INDEX", result.Script);
    }

    [Fact]
    public void ATargetOnlyIndexOnAnAlteredColumn_IsRestoredAfterTheAlter()
    {
        var source = Table("Item", Col("Id", nullable: false), NVarchar("Sku", 60, nullable: false));
        var target = Table("Item", Col("Id", nullable: false), NVarchar("Sku", 20, nullable: false));
        target.Indexes.Add(Index("IX_Item_Sku", unique: false, "Sku"));

        // Without --include-drops the index is not ours to remove, so it has to come
        // back exactly as the target had it.
        var result = _differ.Diff(source, target, includeDrops: false);

        Assert.Contains("DROP INDEX [IX_Item_Sku]", result.Script);
        Assert.Contains("CREATE NONCLUSTERED INDEX [IX_Item_Sku]", result.Script);
    }

    // A script run through sqlcmd inherits QUOTED_IDENTIFIER OFF, which makes
    // filtered indexes and persisted computed columns fail to create.
    [Fact]
    public void EveryGeneratedScript_SetsTheRequiredSessionOptions()
    {
        var snapshot = Snapshot("Db", TableObject(Table("T", Col("Id", nullable: false))));

        var full = ScriptComposer.ComposeFullScript(snapshot);
        var diff = new SchemaDiffer().Diff(snapshot, Snapshot("Target"), false, false, false, false).Script;

        foreach(var script in new[] { full, diff })
        {
            Assert.Contains("SET ANSI_NULLS ON;", script);
            Assert.Contains("SET QUOTED_IDENTIFIER ON;", script);
        }
    }

    private static KeyConstraintModel SystemNamedPk(string name, params string[] columns) => new()
    {
        Name = name,
        TypeCode = "PK",
        IndexTypeDesc = "CLUSTERED",
        IsSystemNamed = true,
        Columns = columns.Select((c, i) => new IndexColumnModel
        {
            Name = c,
            KeyOrdinal = (byte)(i + 1),
            IndexColumnId = i + 1
        }).ToList()
    };
}
