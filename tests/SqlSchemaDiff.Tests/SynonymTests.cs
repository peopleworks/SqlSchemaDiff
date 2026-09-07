using System.Text.Json;
using System.Text.Json.Serialization;
using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

public class SynonymRenderTests
{
    [Fact]
    public void RendersCreateSynonym()
    {
        var script = SqlRender.BuildSynonymCreate(SynonymTestModels.Synonym());

        Assert.Equal("CREATE SYNONYM [dbo].[Customer] FOR [sales].[Customer];", script);
    }

    // sys.synonyms.base_object_name is already bracket-quoted and one to four parts
    // long. Quoting it again would produce [[Archive]].[[dbo]].[[Ledger]], and
    // splitting it would have to guess whether the leading part is a database or a
    // linked server. It goes out exactly as the catalog reported it.
    [Theory]
    [InlineData("[sales].[Customer]")]
    [InlineData("[Archive].[dbo].[Ledger]")]
    [InlineData("[REPORTING01].[Archive].[dbo].[Ledger]")]
    [InlineData("[Customer]")]
    public void TheBaseObjectNameIsPassedThroughUntouched(string baseObjectName)
    {
        var script = SqlRender.BuildSynonymCreate(SynonymTestModels.Synonym(baseObjectName: baseObjectName));

        Assert.Equal($"CREATE SYNONYM [dbo].[Customer] FOR {baseObjectName};", script);
        Assert.DoesNotContain("[[", script, StringComparison.Ordinal);
    }

    // The synonym's own name is an identifier this tool writes, so it is quoted the
    // way every other identifier is - including the ] that would end the bracket.
    [Fact]
    public void TheSynonymsOwnNameIsQuotedLikeAnyOtherIdentifier()
    {
        var script = SqlRender.BuildSynonymCreate(
            SynonymTestModels.Synonym(schema: "odd schema", name: "we]rd"));

        Assert.Equal("CREATE SYNONYM [odd schema].[we]]rd] FOR [sales].[Customer];", script);
    }
}

public class SynonymDifferTests
{
    private readonly SchemaDiffer _differ = new();

    [Fact]
    public void ANewSynonymIsCreated()
    {
        var source = Snapshot("Src", SynonymTestModels.Object(SynonymTestModels.Synonym()));

        var result = _differ.Diff(source, Snapshot("Tgt"), false, false, false, false);

        Assert.Equal(1, result.Added);
        Assert.Contains("CREATE SYNONYM [dbo].[Customer] FOR [sales].[Customer];", result.Script);
        Assert.DoesNotContain("DROP SYNONYM", result.Script);
    }

    // There is no ALTER SYNONYM, so the only way to repoint one is to take it down
    // and put it back - in that order.
    [Fact]
    public void ARepointedSynonymIsDroppedThenCreated()
    {
        var source = Snapshot("Src", SynonymTestModels.Object(SynonymTestModels.Synonym()));
        var target = Snapshot("Tgt", SynonymTestModels.Object(
            SynonymTestModels.Synonym(baseObjectName: "[Archive].[dbo].[Customer]")));

        var result = _differ.Diff(source, target, false, false, false, false);

        Assert.Equal(1, result.Changed);
        Assert.Contains("[dbo].[Customer]", result.ChangedObjects);
        Assert.Contains("DROP SYNONYM [dbo].[Customer];", result.Script);
        Assert.Contains("CREATE SYNONYM [dbo].[Customer] FOR [sales].[Customer];", result.Script);
        Assert.DoesNotContain("ALTER SYNONYM", result.Script);
        Assert.True(
            result.Script.IndexOf("DROP SYNONYM", StringComparison.Ordinal) <
            result.Script.IndexOf("CREATE SYNONYM", StringComparison.Ordinal),
            "the old synonym has to go before the new one is created");
    }

    [Fact]
    public void ASynonymPointingAtTheSameObjectIsNotDrift()
    {
        var source = Snapshot("Src", SynonymTestModels.Object(SynonymTestModels.Synonym()));
        var target = Snapshot("Tgt", SynonymTestModels.Object(SynonymTestModels.Synonym()));

        var result = _differ.Diff(source, target, includeDrops: true, true, false, false);

        Assert.Equal(0, result.Changed);
        Assert.False(result.HasChanges);
    }

    [Fact]
    public void AddOnlySkipsARepointedSynonym()
    {
        var source = Snapshot("Src", SynonymTestModels.Object(SynonymTestModels.Synonym()));
        var target = Snapshot("Tgt", SynonymTestModels.Object(
            SynonymTestModels.Synonym(baseObjectName: "[Archive].[dbo].[Customer]")));

        var result = _differ.Diff(source, target, false, false, false, addOnly: true);

        Assert.Equal(1, result.Changed);
        Assert.Equal(1, result.Skipped);
        Assert.DoesNotContain("DROP SYNONYM", result.Script);
    }

    [Fact]
    public void ATargetOnlySynonymIsDroppedOnlyWithIncludeDrops()
    {
        var target = Snapshot("Tgt", SynonymTestModels.Object(SynonymTestModels.Synonym()));

        var kept = _differ.Diff(Snapshot("Src"), target, includeDrops: false, false, false, false);
        var dropped = _differ.Diff(Snapshot("Src"), target, includeDrops: true, false, false, false);

        Assert.Equal(0, kept.Removed);
        Assert.DoesNotContain("DROP SYNONYM", kept.Script);

        Assert.Equal(1, dropped.Removed);
        Assert.Contains("IF OBJECT_ID(N'[dbo].[Customer]') IS NOT NULL", dropped.Script);
        Assert.Contains("DROP SYNONYM [dbo].[Customer];", dropped.Script);
    }

    // A view may be written against a synonym, so the name has to exist first. The
    // catalog records no edge for it, which is why the type rank has to carry it.
    [Fact]
    public void ASynonymIsCreatedBeforeTablesAndModules()
    {
        var source = Snapshot(
            "Src",
            new DbSchemaObject
            {
                Type = DbObjectType.View,
                Schema = "dbo",
                Name = "vCustomer",
                Definition = "CREATE VIEW [dbo].[vCustomer] AS SELECT * FROM [dbo].[Customer];"
            },
            TableObject(Table("Orders", Col("Id", nullable: false))),
            SynonymTestModels.Object(SynonymTestModels.Synonym()));

        var script = _differ.Diff(source, Snapshot("Tgt"), false, false, false, false).Script;

        var synonym = script.IndexOf("CREATE SYNONYM", StringComparison.Ordinal);
        Assert.True(synonym >= 0, "the synonym must reach the script");
        Assert.True(synonym < script.IndexOf("CREATE TABLE", StringComparison.Ordinal),
            "the synonym must be created before the tables");
        Assert.True(synonym < script.IndexOf("CREATE VIEW", StringComparison.Ordinal),
            "the synonym must be created before a module that may be written against it");
    }

    // Drops run the other way round: the view that names the synonym goes first.
    [Fact]
    public void ASynonymIsDroppedAfterTheModulesAndTables()
    {
        var target = Snapshot(
            "Tgt",
            SynonymTestModels.Object(SynonymTestModels.Synonym()),
            new DbSchemaObject
            {
                Type = DbObjectType.View,
                Schema = "dbo",
                Name = "vCustomer",
                Definition = "CREATE VIEW [dbo].[vCustomer] AS SELECT * FROM [dbo].[Customer];"
            },
            TableObject(Table("Orders", Col("Id", nullable: false))));

        var script = _differ.Diff(Snapshot("Src"), target, includeDrops: true, includeTableDrops: true,
            allowTableRebuild: false, addOnly: false).Script;

        var synonym = script.IndexOf("DROP SYNONYM", StringComparison.Ordinal);
        Assert.True(synonym > script.IndexOf("DROP VIEW", StringComparison.Ordinal),
            "a view that names the synonym has to go first");
        Assert.True(synonym > script.IndexOf("DROP TABLE", StringComparison.Ordinal),
            "the synonym is dropped after the tables");
    }
}

public class SynonymComposerTests
{
    [Fact]
    public void ASynonymGetsItsOwnPhaseFileBeforeTheTables()
    {
        var snapshot = Snapshot("Db",
            SynonymTestModels.Object(SynonymTestModels.Synonym()),
            TableObject(Table("Orders", Col("Id", nullable: false))));

        var phases = ScriptComposer.ComposePhases(snapshot);
        var synonyms = phases.Single(x => x.Name == "synonyms");

        Assert.Equal("035_synonyms.sql", synonyms.FileName);
        Assert.Contains("CREATE SYNONYM [dbo].[Customer] FOR [sales].[Customer];",
            Assert.Single(synonyms.Batches).Sql);

        Assert.True(
            phases.ToList().IndexOf(synonyms) < phases.ToList().FindIndex(x => x.Name == "tables"),
            "the synonyms phase has to run before the tables phase");
        Assert.True(
            phases.ToList().IndexOf(synonyms) > phases.ToList().FindIndex(x => x.Name == "sequences"),
            "the synonyms phase sits between sequences and tables");
    }

    [Fact]
    public void TheComposedScriptCreatesTheSynonymBeforeTheTablesAndModules()
    {
        var snapshot = Snapshot("Db",
            SynonymTestModels.Object(SynonymTestModels.Synonym()),
            TableObject(Table("Orders", Col("Id", nullable: false)),
                "CREATE TABLE [dbo].[Orders] ([Id] int NOT NULL);"),
            new DbSchemaObject
            {
                Type = DbObjectType.View,
                Schema = "dbo",
                Name = "vCustomer",
                Definition = "CREATE VIEW [dbo].[vCustomer] AS SELECT * FROM [dbo].[Customer];"
            });

        var script = ScriptComposer.ComposeFullScript(snapshot);

        var synonym = script.IndexOf("CREATE SYNONYM", StringComparison.Ordinal);
        Assert.True(synonym > 0);
        Assert.True(synonym < script.IndexOf("CREATE TABLE", StringComparison.Ordinal));
        Assert.True(synonym < script.IndexOf("CREATE VIEW", StringComparison.Ordinal));
    }

    [Fact]
    public void TheSynonymTypeIsRoutedByNameLikeEveryOtherType()
    {
        Assert.Equal(PhaseId.Synonyms, ScriptComposer.PhaseForTypeName("Synonym"));
        Assert.True(ScriptComposer.RankForTypeName("Synonym") < ScriptComposer.RankForTypeName("Table"));
        Assert.True(ScriptComposer.RankForTypeName("Synonym") > ScriptComposer.RankForTypeName("Sequence"));
    }
}

public class SynonymFilterTests
{
    [Theory]
    [InlineData("synonym")]
    [InlineData("synonyms")]
    [InlineData("syn")]
    public void EveryTokenNarrowsToSynonyms(string token)
    {
        var filter = ObjectFilter.Parse($"{token}:*", null);

        Assert.True(filter.ShouldInclude(SynonymTestModels.Object(SynonymTestModels.Synonym())));
        Assert.False(filter.ShouldInclude(TableObject(Table("Customer", Col("Id")))));
    }

    [Fact]
    public void ExcludingSynonymsLeavesEverythingElse()
    {
        var filter = ObjectFilter.Parse(null, "synonym:");

        Assert.False(filter.ShouldInclude(SynonymTestModels.Object(SynonymTestModels.Synonym())));
        Assert.True(filter.ShouldInclude(TableObject(Table("Customer", Col("Id")))));
    }
}

public class SynonymSnapshotJsonTests
{
    // The CLI's options, so the test reads and writes exactly what ships.
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    // A snapshot from 1.5 knows nothing about synonyms - or about the enum members
    // 1.6 and 1.7 appended. It still has to load, with Synonym left null on every
    // object, or every consumer of an older snapshot breaks on upgrade.
    private const string LegacySnapshotJson = """
        {
          "DatabaseName": "Legacy",
          "GeneratedAtUtc": "2025-01-01T00:00:00+00:00",
          "Schemas": [],
          "Types": [],
          "Objects": [
            {
              "Type": "Table",
              "Schema": "dbo",
              "Name": "Orders",
              "Definition": "CREATE TABLE [dbo].[Orders] (...)",
              "Dependencies": [],
              "Table": {
                "Schema": "dbo",
                "Name": "Orders",
                "Columns": [
                  {
                    "Name": "Id",
                    "TypeSchema": "sys",
                    "TypeName": "int",
                    "MaxLength": 4,
                    "Precision": 10,
                    "Scale": 0,
                    "IsNullable": false
                  }
                ],
                "KeyConstraints": [],
                "ForeignKeys": [],
                "CheckConstraints": [],
                "Indexes": []
              }
            },
            {
              "Type": "View",
              "Schema": "dbo",
              "Name": "vOrders",
              "Definition": "CREATE VIEW [dbo].[vOrders] AS SELECT 1 AS X",
              "Dependencies": []
            }
          ]
        }
        """;

    [Fact]
    public void ALegacySnapshotStillDeserializes_WithNoSynonyms()
    {
        var snapshot = SnapshotSerializer.Deserialize(LegacySnapshotJson);

        Assert.Equal(2, snapshot.Objects.Count);
        Assert.All(snapshot.Objects, x => Assert.Null(x.Synonym));
        Assert.DoesNotContain(snapshot.Objects, x => x.Type == DbObjectType.Synonym);
        Assert.Equal(DbObjectType.Table, snapshot.Objects[0].Type);
        Assert.Equal(DbObjectType.View, snapshot.Objects[1].Type);
    }

    // Appending Synonym to the enum must not move any member that came before it,
    // or a snapshot whose producer wrote the enum as a number changes meaning.
    [Fact]
    public void AppendingSynonymLeftEveryExistingEnumValueWhereItWas()
    {
        Assert.Equal(0, (int)DbObjectType.Table);
        Assert.Equal(1, (int)DbObjectType.View);
        Assert.Equal(2, (int)DbObjectType.StoredProcedure);
        Assert.Equal(3, (int)DbObjectType.Function);
        Assert.Equal(4, (int)DbObjectType.Trigger);
        Assert.Equal(5, (int)DbObjectType.Sequence);
        Assert.Equal(6, (int)DbObjectType.TableType);
        Assert.Equal(7, (int)DbObjectType.Synonym);
    }

    [Fact]
    public void ASynonymSurvivesASnapshotSerializerRoundTrip()
    {
        var synonym = SynonymTestModels.Synonym(
            schema: "ops", name: "RemoteLedger", baseObjectName: "[REPORTING01].[Archive].[dbo].[Ledger]");
        var snapshot = Snapshot("Round", SynonymTestModels.Object(synonym));

        var reloaded = SnapshotSerializer.Deserialize(SnapshotSerializer.Serialize(snapshot));

        var roundTripped = Assert.Single(reloaded.Objects);
        Assert.Equal(DbObjectType.Synonym, roundTripped.Type);
        Assert.NotNull(roundTripped.Synonym);
        Assert.Equal("ops", roundTripped.Synonym!.Schema);
        Assert.Equal("RemoteLedger", roundTripped.Synonym.Name);
        Assert.Equal("[REPORTING01].[Archive].[dbo].[Ledger]", roundTripped.Synonym.BaseObjectName);
        Assert.Equal(SqlRender.BuildSynonymCreate(synonym), roundTripped.Definition);

        // And a reloaded snapshot compares equal to the one it came from.
        Assert.False(new SchemaDiffer()
            .Diff(snapshot, reloaded, includeDrops: true, true, false, false)
            .HasChanges);
    }

    // The enum is serialized by name, so a snapshot that already carries a synonym
    // has to read back as one rather than as an unknown ordinal.
    [Fact]
    public void TheSynonymTypeIsWrittenByName()
    {
        var json = JsonSerializer.Serialize(
            Snapshot("Round", SynonymTestModels.Object(SynonymTestModels.Synonym())), JsonOptions);

        Assert.Contains("\"Type\": \"Synonym\"", json);
        Assert.Contains("\"BaseObjectName\": \"[sales].[Customer]\"", json);
    }
}

internal static class SynonymTestModels
{
    public static SynonymModel Synonym(
        string schema = "dbo",
        string name = "Customer",
        string baseObjectName = "[sales].[Customer]")
        => new()
        {
            Schema = schema,
            Name = name,
            BaseObjectName = baseObjectName
        };

    public static DbSchemaObject Object(SynonymModel synonym) => new()
    {
        Type = DbObjectType.Synonym,
        Schema = synonym.Schema,
        Name = synonym.Name,
        Definition = SqlRender.BuildSynonymCreate(synonym),
        Synonym = synonym
    };
}
