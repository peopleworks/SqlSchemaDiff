using System.Text.Json;
using System.Text.Json.Serialization;
using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;

namespace SqlSchemaDiff.Tests;

/// <summary>
/// Snapshots are written to disk and read back by other builds and by other tools
/// (the MCP server consumes this package), so the JSON shape is a compatibility
/// surface: new properties may be added, none may become required.
/// </summary>
public class SnapshotJsonTests
{
    // The same options Program.cs uses for extract and for --source-snapshot.
    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    private static DatabaseSnapshot FullSnapshot() => new()
    {
        DatabaseName = "Db",
        GeneratedAtUtc = DateTimeOffset.UnixEpoch,
        Schemas = { "app" },
        SchemaOwners = new Dictionary<string, string> { ["app"] = "app_owner" },
        Objects =
        {
            TriggerTestModels.Object(disabled: true, insteadOf: true),
            SequenceTestModels.Object(SequenceTestModels.Sequence()),
            TableTypeTestModels.Object(TableTypeTestModels.Full())
        }
    };

    [Fact]
    public void TheNewTypesRoundTripWithTheirModels()
    {
        var json = JsonSerializer.Serialize(FullSnapshot(), Options);
        var restored = JsonSerializer.Deserialize<DatabaseSnapshot>(json, Options);

        Assert.NotNull(restored);
        Assert.Equal("app_owner", restored!.SchemaOwners!["app"]);

        var trigger = restored.Objects.Single(x => x.Type == DbObjectType.Trigger);
        Assert.Equal("Orders", trigger.Trigger!.ParentName);
        Assert.True(trigger.Trigger.IsDisabled);
        Assert.True(trigger.Trigger.IsInsteadOf);

        var sequence = restored.Objects.Single(x => x.Type == DbObjectType.Sequence);
        Assert.Equal("999999", sequence.Sequence!.MaxValue);
        Assert.Equal(20, sequence.Sequence.CacheSize);

        var tableType = restored.Objects.Single(x => x.Type == DbObjectType.TableType);
        Assert.Equal(3, tableType.TableType!.Columns.Count);
        Assert.Equal(2, tableType.TableType.KeyConstraints.Count);
        Assert.Single(tableType.TableType.CheckConstraints);
    }

    [Fact]
    public void TheNewObjectTypesAreWrittenByName()
    {
        var json = JsonSerializer.Serialize(FullSnapshot(), Options);

        Assert.Contains("\"Type\": \"Trigger\"", json);
        Assert.Contains("\"Type\": \"Sequence\"", json);
        Assert.Contains("\"Type\": \"TableType\"", json);
    }

    // A snapshot from 1.5.0 has no SchemaOwners and no Trigger/Sequence/TableType
    // properties. It has to keep loading, with the new members simply null.
    [Fact]
    public void AOnePointFiveSnapshotStillLoads()
    {
        const string legacyJson = """
                                  {
                                    "DatabaseName": "Legacy",
                                    "GeneratedAtUtc": "2025-01-01T00:00:00+00:00",
                                    "Schemas": [ "app" ],
                                    "Types": [
                                      {
                                        "Schema": "dbo",
                                        "Name": "PhoneNumber",
                                        "BaseTypeName": "varchar",
                                        "MaxLength": 25,
                                        "Precision": 0,
                                        "Scale": 0,
                                        "IsNullable": false,
                                        "CollationName": null
                                      }
                                    ],
                                    "Objects": [
                                      {
                                        "Type": "Table",
                                        "Schema": "app",
                                        "Name": "Customer",
                                        "Definition": "CREATE TABLE [app].[Customer] ([Id] int NOT NULL);",
                                        "Dependencies": [],
                                        "Table": {
                                          "Schema": "app",
                                          "Name": "Customer",
                                          "Columns": [
                                            {
                                              "Name": "Id",
                                              "TypeSchema": "sys",
                                              "TypeName": "int",
                                              "IsUserDefinedType": false,
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
                                        "Type": "StoredProcedure",
                                        "Schema": "dbo",
                                        "Name": "usp_Get",
                                        "Definition": "CREATE PROCEDURE dbo.usp_Get AS SELECT 1;",
                                        "Dependencies": []
                                      }
                                    ]
                                  }
                                  """;

        var restored = JsonSerializer.Deserialize<DatabaseSnapshot>(legacyJson, Options);

        Assert.NotNull(restored);
        Assert.Null(restored!.SchemaOwners);
        Assert.Equal(new[] { "app" }, restored.Schemas);
        Assert.Single(restored.Types);
        Assert.Equal(2, restored.Objects.Count);

        var table = restored.Objects.Single(x => x.Type == DbObjectType.Table);
        Assert.NotNull(table.Table);
        Assert.Null(table.Trigger);
        Assert.Null(table.Sequence);
        Assert.Null(table.TableType);

        // And it still diffs: a legacy snapshot is a usable source.
        var result = new SchemaDiffer().Diff(restored, new DatabaseSnapshot { DatabaseName = "Empty" }, false, false, false, false);
        Assert.Equal(2, result.Added);
        Assert.Contains("CREATE SCHEMA [app]", result.Script);
    }

    // Appending to the enum keeps every existing member's numeric value, so a
    // consumer that serialized it as a number still reads the same object kinds.
    [Fact]
    public void TheExistingEnumValuesDidNotMove()
    {
        Assert.Equal(0, (int)DbObjectType.Table);
        Assert.Equal(1, (int)DbObjectType.View);
        Assert.Equal(2, (int)DbObjectType.StoredProcedure);
        Assert.Equal(3, (int)DbObjectType.Function);
    }

    [Fact]
    public void AKeyIsTheTypeNameFollowedByTheQualifiedName()
    {
        Assert.Equal("Sequence:dbo.OrderNumbers", DbSchemaObject.BuildKey(DbObjectType.Sequence, "dbo", "OrderNumbers"));
        Assert.Equal(
            DbSchemaObject.BuildKey(DbObjectType.TableType, "dbo", "Tvp"),
            new DbSchemaObject { Type = DbObjectType.TableType, Schema = "dbo", Name = "Tvp" }.Key);
    }
}
