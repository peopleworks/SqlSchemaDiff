using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

/// <summary>
/// The composer has one job the old fixed type ranking could not do: emit a
/// script that runs top to bottom on an empty database. These tests pin the
/// orderings that make that true, and the phase plan a restore uses.
/// </summary>
public class ScriptComposerTests
{
    // ------------------------------------------------------------- ordering

    [Fact]
    public void AForeignKey_ComesAfterBothTables_EvenWhenItPointsAtALaterName()
    {
        // Ordered by name alone, [dbo].[Aaa] is created first and its foreign key
        // fails: "Foreign key references invalid table [dbo].[Zzz]".
        var snapshot = Snapshot("Db",
            TableWithForeignKey("Aaa", "Zzz"),
            SimpleTable("Zzz"));

        var script = ScriptComposer.ComposeFullScript(snapshot);

        var child = script.IndexOf("CREATE TABLE [dbo].[Aaa]", StringComparison.Ordinal);
        var parent = script.IndexOf("CREATE TABLE [dbo].[Zzz]", StringComparison.Ordinal);
        var foreignKey = script.IndexOf("FOREIGN KEY", StringComparison.Ordinal);

        Assert.True(child >= 0 && parent >= 0 && foreignKey >= 0);
        Assert.True(child < foreignKey, "the child table must exist before its foreign key");
        Assert.True(parent < foreignKey, "the referenced table must exist before the foreign key");
    }

    [Fact]
    public void AViewOnAView_ComesAfterItsBase()
    {
        // Alphabetically vAlpha comes first; the dependency says otherwise.
        var snapshot = Snapshot("Db",
            Module(DbObjectType.View, "vAlpha", "CREATE VIEW dbo.vAlpha AS SELECT * FROM dbo.vZulu", "View:dbo.vZulu"),
            Module(DbObjectType.View, "vZulu", "CREATE VIEW dbo.vZulu AS SELECT 1 AS X"));

        var script = ScriptComposer.ComposeFullScript(snapshot);

        Assert.True(
            script.IndexOf("CREATE VIEW dbo.vZulu", StringComparison.Ordinal) <
            script.IndexOf("CREATE VIEW dbo.vAlpha", StringComparison.Ordinal));
    }

    [Fact]
    public void AFunctionUsedByAView_ComesBeforeIt()
    {
        var snapshot = Snapshot("Db",
            Module(DbObjectType.View, "aView", "CREATE VIEW dbo.aView AS SELECT dbo.zFn() AS X", "Function:dbo.zFn"),
            Module(DbObjectType.Function, "zFn", "CREATE FUNCTION dbo.zFn() RETURNS int AS BEGIN RETURN 1 END"));

        var script = ScriptComposer.ComposeFullScript(snapshot);

        Assert.True(
            script.IndexOf("CREATE FUNCTION dbo.zFn", StringComparison.Ordinal) <
            script.IndexOf("CREATE VIEW dbo.aView", StringComparison.Ordinal));
    }

    [Fact]
    public void ATableWithAComputedColumn_IsRetryable()
    {
        // The expression can call a scalar function that only exists after the
        // modules phase, so the restore driver has to be allowed a second attempt.
        var table = Table("Invoice", Col("Id", nullable: false), new ColumnModel
        {
            Name = "Total",
            IsComputed = true,
            ComputedDefinition = "([dbo].[fnTotal]([Id]))"
        });

        var phases = ScriptComposer.ComposePhases(Snapshot("Db", TableObject(table)));

        var batch = Assert.Single(Phase(phases, "tables").Batches);
        Assert.True(batch.Retryable);
    }

    [Fact]
    public void ACheckConstraint_IsRetryable_ButAPlainTableIsNot()
    {
        var table = Table("Item", Col("Id", nullable: false));
        table.CheckConstraints.Add(new CheckConstraintModel { Name = "CK_Item", Definition = "([Id] > 0)" });

        var batches = Phase(ScriptComposer.ComposePhases(Snapshot("Db", TableObject(table))), "tables").Batches;

        Assert.False(batches[0].Retryable);
        Assert.True(batches[1].Retryable);
    }

    [Fact]
    public void AModuleIsRetryable()
    {
        var snapshot = Snapshot("Db", Module(DbObjectType.View, "v", "CREATE VIEW dbo.v AS SELECT 1 AS X"));

        var batch = Assert.Single(Phase(ScriptComposer.ComposePhases(snapshot), "modules").Batches);

        Assert.True(batch.Retryable);
    }

    [Fact]
    public void AForeignKeyCycleBetweenTwoTables_Composes()
    {
        var snapshot = Snapshot("Db",
            TableWithForeignKey("Left", "Right"),
            TableWithForeignKey("Right", "Left"));

        var script = ScriptComposer.ComposeFullScript(snapshot);

        Assert.Contains("CREATE TABLE [dbo].[Left]", script);
        Assert.Contains("CREATE TABLE [dbo].[Right]", script);
        Assert.Contains("REFERENCES [dbo].[Right]", script);
        Assert.Contains("REFERENCES [dbo].[Left]", script);

        // Both tables exist before either foreign key is added, which is exactly
        // why deferring them is worth doing.
        var lastCreate = script.LastIndexOf("CREATE TABLE", StringComparison.Ordinal);
        Assert.True(lastCreate < script.IndexOf("FOREIGN KEY", StringComparison.Ordinal));
    }

    [Fact]
    public void AModuleCycle_IsReported_AndOrderedDeterministically()
    {
        var snapshot = Snapshot("Db",
            Module(DbObjectType.View, "vOne", "CREATE VIEW dbo.vOne AS SELECT 1 AS X", "View:dbo.vTwo"),
            Module(DbObjectType.View, "vTwo", "CREATE VIEW dbo.vTwo AS SELECT 2 AS X", "View:dbo.vOne"));

        var script = ScriptComposer.ComposeFullScript(snapshot);
        var again = ScriptComposer.ComposeFullScript(snapshot);

        Assert.Contains("dependency cycle detected", script);
        Assert.Contains("CREATE VIEW dbo.vOne", script);
        Assert.Contains("CREATE VIEW dbo.vTwo", script);
        Assert.Equal(script, again);
    }

    // --------------------------------------------------------------- phases

    [Fact]
    public void ThePhases_AreAlwaysTheSameFilesInTheSameOrder()
    {
        var phases = ScriptComposer.ComposePhases(Snapshot("Empty"));

        Assert.Equal(
            new[]
            {
                "010_schemas.sql",
                "020_types.sql",
                "030_sequences.sql",
                "040_tables.sql",
                "050_indexes.sql",
                "060_checks.sql",
                "070_foreignkeys.sql",
                "080_modules.sql",
                "085_triggers.sql",
                "090_finalize.sql"
            },
            phases.Select(x => x.FileName));

        Assert.All(phases, phase => Assert.Empty(phase.Batches));
    }

    [Fact]
    public void ByDefault_ATableKeepsItsKeysChecksAndIndexes_AndOnlyLosesItsForeignKeys()
    {
        var phases = ScriptComposer.ComposePhases(Snapshot("Db", TableObject(FullyDressedTable())));

        var tables = Sql(phases, "tables");
        Assert.Contains("CREATE TABLE [dbo].[Orders]", tables);
        Assert.Contains("ADD CONSTRAINT [PK_Orders] PRIMARY KEY", tables);
        Assert.Contains("CHECK ([Total] > 0)", tables);
        Assert.Contains("CREATE NONCLUSTERED INDEX [IX_Orders_Total]", tables);
        Assert.DoesNotContain("FOREIGN KEY", tables);

        Assert.Empty(Phase(phases, "indexes").Batches);
        Assert.Empty(Phase(phases, "checks").Batches);
        Assert.Contains("FOREIGN KEY", Sql(phases, "foreign_keys"));
    }

    [Fact]
    public void WithConstraintsAfterData_TheTablesPhaseIsBareCreateTables()
    {
        var options = new ComposeOptions { ConstraintsAfterData = true };

        var phases = ScriptComposer.ComposePhases(Snapshot("Db", TableObject(FullyDressedTable())), options);

        var tables = Sql(phases, "tables");
        Assert.Contains("CREATE TABLE [dbo].[Orders]", tables);
        Assert.Contains("DEFAULT ((0))", tables); // inline defaults stay on the column
        Assert.DoesNotContain("ALTER TABLE", tables);
        Assert.DoesNotContain("ADD CONSTRAINT", tables);
        Assert.DoesNotContain("CREATE NONCLUSTERED INDEX", tables);

        Assert.Contains("ADD CONSTRAINT [PK_Orders] PRIMARY KEY", Sql(phases, "indexes"));
        Assert.Contains("CREATE NONCLUSTERED INDEX [IX_Orders_Total]", Sql(phases, "indexes"));
        Assert.Contains("CHECK ([Total] > 0)", Sql(phases, "checks"));
        Assert.Contains("FOREIGN KEY", Sql(phases, "foreign_keys"));
    }

    [Fact]
    public void SchemasAndTypes_ArePrerequisitePhases()
    {
        var table = Table("Customer", Col("Id", nullable: false));
        table.Schema = "app";
        var snapshot = Snapshot("Db", TableObject(table));
        snapshot.Schemas.Add("app");
        snapshot.Types.Add(new AliasTypeModel { Schema = "app", Name = "Phone", BaseTypeName = "varchar", MaxLength = 25 });

        var phases = ScriptComposer.ComposePhases(snapshot);
        var script = ScriptComposer.ComposeFullScript(snapshot);

        Assert.Contains("IF SCHEMA_ID(N'app') IS NULL", Sql(phases, "schemas"));
        Assert.Contains("IF TYPE_ID(N'app.Phone') IS NULL", Sql(phases, "types"));

        Assert.Contains("-- Prerequisites (schemas and user-defined types)", script);
        Assert.True(
            script.IndexOf("CREATE SCHEMA", StringComparison.Ordinal) <
            script.IndexOf("CREATE TABLE", StringComparison.Ordinal));
    }

    [Fact]
    public void AnObjectOfAnUnknownType_IsPlannedLast_InsteadOfBeingDropped()
    {
        // A snapshot written by a newer build: the type is not in this enum, but the
        // object still has to reach the script.
        var exotic = new DbSchemaObject
        {
            Type = (DbObjectType)9999,
            Schema = "dbo",
            Name = "Whatever",
            Definition = "CREATE WHATEVER dbo.Whatever;"
        };

        var snapshot = Snapshot("Db", exotic, SimpleTable("T"));

        Assert.Contains("CREATE WHATEVER", Sql(ScriptComposer.ComposePhases(snapshot), "finalize"));

        var script = ScriptComposer.ComposeFullScript(snapshot);
        Assert.True(
            script.IndexOf("CREATE TABLE", StringComparison.Ordinal) <
            script.IndexOf("CREATE WHATEVER", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("Sequence", nameof(PhaseId.Sequences))]
    [InlineData("TableType", nameof(PhaseId.Types))]
    [InlineData("Trigger", nameof(PhaseId.Triggers))]
    [InlineData("Table", nameof(PhaseId.Tables))]
    [InlineData("View", nameof(PhaseId.Modules))]
    [InlineData("Function", nameof(PhaseId.Modules))]
    [InlineData("StoredProcedure", nameof(PhaseId.Modules))]
    [InlineData("SomethingElse", nameof(PhaseId.Finalize))]
    public void ObjectTypesAreRoutedByName_SoNewOnesNeedNoChangeHere(string typeName, string expected) =>
        Assert.Equal(expected, ScriptComposer.PhaseForTypeName(typeName).ToString());

    [Theory]
    [InlineData("Sequence")]
    [InlineData("TableType")]
    public void TypesThatTablesDependOn_RankBeforeTables(string typeName) =>
        Assert.True(ScriptComposer.RankForTypeName(typeName) < ScriptComposer.RankForTypeName("Table"));

    [Fact]
    public void TriggersRankAfterModules_AndUnknownTypesAfterTables()
    {
        Assert.True(ScriptComposer.RankForTypeName("Trigger") > ScriptComposer.RankForTypeName("StoredProcedure"));
        Assert.True(ScriptComposer.RankForTypeName("Anything") > ScriptComposer.RankForTypeName("Table"));
    }

    [Fact]
    public void ATableWithoutAStructuredModel_StillHasItsForeignKeysDeferred()
    {
        var legacy = new DbSchemaObject
        {
            Type = DbObjectType.Table,
            Schema = "dbo",
            Name = "Legacy",
            Definition = "CREATE TABLE [dbo].[Legacy] ([Id] int NOT NULL);\nGO\n\n" +
                         "ALTER TABLE [dbo].[Legacy] WITH CHECK ADD CONSTRAINT [FK_Legacy] FOREIGN KEY ([Id]) REFERENCES [dbo].[Other] ([Id]);\nGO"
        };

        var phases = ScriptComposer.ComposePhases(Snapshot("Db", legacy));

        Assert.Contains("CREATE TABLE [dbo].[Legacy]", Sql(phases, "tables"));
        Assert.DoesNotContain("FOREIGN KEY", Sql(phases, "tables"));
        Assert.Contains("FOREIGN KEY", Sql(phases, "foreign_keys"));
    }

    // ------------------------------------------------- module session options

    [Fact]
    public void AModuleCreatedWithQuotedIdentifierOff_IsRecreatedThatWay()
    {
        var view = new DbSchemaObject
        {
            Type = DbObjectType.View,
            Schema = "dbo",
            Name = "vLegacy",
            Definition = "CREATE VIEW dbo.vLegacy AS SELECT 1 AS X",
            UsesAnsiNulls = true,
            UsesQuotedIdentifier = false
        };

        var script = ScriptComposer.ComposeFullScript(Snapshot("Db", view));

        var off = script.IndexOf("SET QUOTED_IDENTIFIER OFF;", StringComparison.Ordinal);
        var create = script.IndexOf("CREATE VIEW dbo.vLegacy", StringComparison.Ordinal);
        var restored = script.LastIndexOf("SET QUOTED_IDENTIFIER ON;", StringComparison.Ordinal);

        Assert.True(off > 0, "the OFF setting must be emitted");
        Assert.True(off < create && create < restored, "the module must be created between the two SET batches");
        Assert.DoesNotContain("SET ANSI_NULLS OFF;", script);
    }

    [Fact]
    public void AModuleWithTheDefaultOptions_GetsNoExtraSetStatements()
    {
        var view = new DbSchemaObject
        {
            Type = DbObjectType.View,
            Schema = "dbo",
            Name = "v",
            Definition = "CREATE VIEW dbo.v AS SELECT 1 AS X",
            UsesAnsiNulls = true,
            UsesQuotedIdentifier = true
        };

        var script = ScriptComposer.ComposeFullScript(Snapshot("Db", view));

        Assert.DoesNotContain("OFF;", script);
    }

    [Fact]
    public void TheDiffScript_AlsoRecreatesAModuleWithItsOwnOptions()
    {
        var view = new DbSchemaObject
        {
            Type = DbObjectType.View,
            Schema = "dbo",
            Name = "vLegacy",
            Definition = "CREATE VIEW dbo.vLegacy AS SELECT 1 AS X",
            UsesAnsiNulls = false,
            UsesQuotedIdentifier = false
        };

        var result = new SchemaDiffer().Diff(Snapshot("Src", view), Snapshot("Tgt"), false, false, false, false);

        Assert.Contains("SET ANSI_NULLS OFF;", result.Script);
        Assert.Contains("SET QUOTED_IDENTIFIER OFF;", result.Script);
        Assert.True(
            result.Script.IndexOf("SET ANSI_NULLS OFF;", StringComparison.Ordinal) <
            result.Script.IndexOf("CREATE VIEW dbo.vLegacy", StringComparison.Ordinal));
    }

    // --------------------------------------------------------- determinism

    [Fact]
    public void TheSameSnapshot_AlwaysComposesTheSameBytes()
    {
        var snapshot = MixedSnapshot();

        Assert.Equal(ScriptComposer.ComposeFullScript(snapshot), ScriptComposer.ComposeFullScript(snapshot));
    }

    [Fact]
    public void TheOrderTheObjectsArrivedIn_DoesNotChangeTheScript()
    {
        var snapshot = MixedSnapshot();
        var shuffled = new DatabaseSnapshot
        {
            DatabaseName = snapshot.DatabaseName,
            GeneratedAtUtc = snapshot.GeneratedAtUtc,
            Schemas = snapshot.Schemas,
            Types = snapshot.Types,
            Objects = Enumerable.Reverse(snapshot.Objects).ToList()
        };

        Assert.Equal(ScriptComposer.ComposeFullScript(snapshot), ScriptComposer.ComposeFullScript(shuffled));
    }

    [Fact]
    public void TheScriptStillOpensWithItsHeaderAndSessionOptions()
    {
        var script = ScriptComposer.ComposeFullScript(Snapshot("Db", SimpleTable("T")));

        Assert.StartsWith("-- Snapshot database: [Db]", script);
        Assert.Contains("SET ANSI_NULLS ON;", script);
        Assert.Contains("SET QUOTED_IDENTIFIER ON;", script);
        Assert.Contains("-- Table [dbo].[T]", script);
    }

    // -------------------------------------------------------------- helpers

    private static DatabaseSnapshot MixedSnapshot()
    {
        var snapshot = Snapshot("Db",
            TableWithForeignKey("Aaa", "Zzz"),
            SimpleTable("Zzz"),
            TableObject(FullyDressedTable()),
            Module(DbObjectType.View, "vAlpha", "CREATE VIEW dbo.vAlpha AS SELECT * FROM dbo.vZulu", "View:dbo.vZulu"),
            Module(DbObjectType.View, "vZulu", "CREATE VIEW dbo.vZulu AS SELECT 1 AS X"),
            Module(DbObjectType.Function, "fn", "CREATE FUNCTION dbo.fn() RETURNS int AS BEGIN RETURN 1 END"),
            Module(DbObjectType.StoredProcedure, "usp", "CREATE PROCEDURE dbo.usp AS SELECT 1"));

        snapshot.Schemas.Add("app");
        return snapshot;
    }

    private static ScriptPhase Phase(IReadOnlyList<ScriptPhase> phases, string name) =>
        phases.Single(x => x.Name == name);

    private static string Sql(IReadOnlyList<ScriptPhase> phases, string name) =>
        string.Join(Environment.NewLine, Phase(phases, name).Batches.Select(x => x.Sql));

    private static DbSchemaObject SimpleTable(string name) =>
        TableObject(Table(name, Col("Id", nullable: false)));

    private static DbSchemaObject Module(DbObjectType type, string name, string definition, params string[] dependencies) =>
        new()
        {
            Type = type,
            Schema = "dbo",
            Name = name,
            Definition = definition,
            Dependencies = dependencies.ToList()
        };

    private static DbSchemaObject TableWithForeignKey(string name, string referencedTable)
    {
        var table = Table(name, Col("Id", nullable: false), Col("ParentId"));
        table.ForeignKeys.Add(new ForeignKeyModel
        {
            Name = $"FK_{name}_{referencedTable}",
            ReferencedSchema = "dbo",
            ReferencedTable = referencedTable,
            DeleteActionDesc = "NO_ACTION",
            UpdateActionDesc = "NO_ACTION",
            Columns = { new ForeignKeyColumnModel { ParentColumn = "ParentId", ReferencedColumn = "Id" } }
        });

        return new DbSchemaObject
        {
            Type = DbObjectType.Table,
            Schema = table.Schema,
            Name = table.Name,
            Definition = SqlRender.BuildTableCreateScript(table),
            Dependencies = { $"Table:dbo.{referencedTable}" },
            Table = table
        };
    }

    /// <summary>A table with one of everything the composer has to place.</summary>
    private static TableModel FullyDressedTable()
    {
        var table = Table("Orders",
            Col("Id", nullable: false),
            Col("CustomerId"),
            Col("Total", defaultDefinition: "((0))"));

        table.KeyConstraints.Add(new KeyConstraintModel
        {
            Name = "PK_Orders",
            TypeCode = "PK",
            IndexTypeDesc = "CLUSTERED",
            Columns = { new IndexColumnModel { Name = "Id", KeyOrdinal = 1, IndexColumnId = 1 } }
        });
        table.CheckConstraints.Add(new CheckConstraintModel { Name = "CK_Orders_Total", Definition = "([Total] > 0)" });
        table.Indexes.Add(Index("IX_Orders_Total", unique: false, "Total"));
        table.ForeignKeys.Add(new ForeignKeyModel
        {
            Name = "FK_Orders_Customer",
            ReferencedSchema = "dbo",
            ReferencedTable = "Customer",
            DeleteActionDesc = "NO_ACTION",
            UpdateActionDesc = "NO_ACTION",
            Columns = { new ForeignKeyColumnModel { ParentColumn = "CustomerId", ReferencedColumn = "Id" } }
        });

        return table;
    }
}
