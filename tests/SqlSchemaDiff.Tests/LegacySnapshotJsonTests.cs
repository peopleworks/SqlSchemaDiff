using System.Text.Json;
using System.Text.Json.Serialization;
using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

/// <summary>
/// A snapshot written by an older release knows nothing about sparse columns, index
/// storage options or compression. Loading one has to leave every new property at
/// the value SQL Server itself would use, so the old snapshot still compares equal to
/// a database that is genuinely unchanged instead of reading as drift on every table.
/// </summary>
public class LegacySnapshotJsonTests
{
    // The CLI's options, so the test reads and writes exactly what ships.
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

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
                    "IsUserDefinedType": false,
                    "MaxLength": 4,
                    "Precision": 10,
                    "Scale": 0,
                    "IsNullable": false,
                    "IsIdentity": false,
                    "IsComputed": false,
                    "CollationName": null,
                    "IsRowGuid": false,
                    "ComputedDefinition": null,
                    "IsPersisted": false,
                    "DefaultName": null,
                    "DefaultDefinition": null,
                    "DefaultIsSystemNamed": false,
                    "IdentitySeed": null,
                    "IdentityIncrement": null
                  },
                  {
                    "Name": "Code",
                    "TypeSchema": "sys",
                    "TypeName": "nvarchar",
                    "IsUserDefinedType": false,
                    "MaxLength": 40,
                    "Precision": 0,
                    "Scale": 0,
                    "IsNullable": true,
                    "IsIdentity": false,
                    "IsComputed": false,
                    "CollationName": "SQL_Latin1_General_CP1_CI_AS",
                    "IsRowGuid": false,
                    "ComputedDefinition": null,
                    "IsPersisted": false,
                    "DefaultName": null,
                    "DefaultDefinition": null,
                    "DefaultIsSystemNamed": false,
                    "IdentitySeed": null,
                    "IdentityIncrement": null
                  }
                ],
                "KeyConstraints": [
                  {
                    "TypeCode": "PK",
                    "Name": "PK_Orders",
                    "IsSystemNamed": false,
                    "IndexTypeDesc": "CLUSTERED",
                    "Columns": [
                      { "Name": "Id", "KeyOrdinal": 1, "IsDescending": false, "IsIncluded": false, "IndexColumnId": 1 }
                    ]
                  }
                ],
                "ForeignKeys": [],
                "CheckConstraints": [],
                "Indexes": [
                  {
                    "Name": "IX_Orders_Code",
                    "IsUnique": false,
                    "TypeDesc": "NONCLUSTERED",
                    "FilterDefinition": null,
                    "IsDisabled": false,
                    "Columns": [
                      { "Name": "Code", "KeyOrdinal": 1, "IsDescending": false, "IsIncluded": false, "IndexColumnId": 1 }
                    ]
                  }
                ]
              }
            }
          ]
        }
        """;

    [Fact]
    public void LoadingALegacySnapshot_LeavesTheNewPropertiesAtTheirServerDefaults()
    {
        var table = LoadLegacyTable();

        Assert.All(table.Columns, column => Assert.False(column.IsSparse));
        Assert.Null(table.DataCompression);

        foreach(IIndexStorageOptions options in new IIndexStorageOptions[] { table.KeyConstraints[0], table.Indexes[0] })
        {
            Assert.Equal(0, options.FillFactor);
            Assert.False(options.IsPadded);
            Assert.False(options.IgnoreDupKey);
            // The one property whose CLR default is the wrong answer.
            Assert.True(options.AllowRowLocks);
            Assert.True(options.AllowPageLocks);
            Assert.Null(options.DataCompression);
        }
    }

    [Fact]
    public void ALegacySnapshotComparedWithItself_ReportsNoChange()
    {
        var table = LoadLegacyTable();

        var result = new TableDiffer().Diff(table, LoadLegacyTable(), includeDrops: true);

        Assert.False(result.HasChanges);
        Assert.Equal(string.Empty, result.Script);
    }

    // The real regression this guards: a database that was never touched, extracted
    // today, must not read as drift against a snapshot taken by the old version.
    [Fact]
    public void ALegacySnapshotComparedWithAFreshlyExtractedEquivalent_ReportsNoChange()
    {
        var legacy = LoadLegacyTable();

        var fresh = Table("Orders", Col("Id", nullable: false), NVarchar("Code", 20));
        fresh.KeyConstraints.Add(Key("PK_Orders", "PK", "CLUSTERED", "Id"));
        fresh.Indexes.Add(Index("IX_Orders_Code", unique: false, "Code"));
        // What the extractor reads back for an uncompressed table and index.
        fresh.DataCompression = "NONE";
        fresh.KeyConstraints[0].DataCompression = "NONE";
        fresh.Indexes[0].DataCompression = "NONE";

        Assert.False(new TableDiffer().Diff(fresh, legacy, includeDrops: true).HasChanges);
        Assert.False(new TableDiffer().Diff(legacy, fresh, includeDrops: true).HasChanges);
    }

    [Fact]
    public void ALegacySnapshot_ScriptsWithoutAnyNewClauses()
    {
        var script = SqlRender.BuildTableCreateScript(LoadLegacyTable());

        Assert.DoesNotContain("SPARSE", script);
        Assert.DoesNotContain("DATA_COMPRESSION", script);
        Assert.DoesNotContain("WITH (", script);
    }

    [Fact]
    public void TheNewPropertiesSurviveASerializeAndReloadRoundTrip()
    {
        var table = Table("Orders", Col("Id", nullable: false), Col("Note", "int", sparse: true));
        table.DataCompression = "PAGE";
        var pk = Key("PK_Orders", "PK", "CLUSTERED", "Id");
        pk.FillFactor = 85;
        pk.IsPadded = true;
        pk.DataCompression = "ROW";
        table.KeyConstraints.Add(pk);
        var index = Index("IX_Orders_Note", unique: false, "Note");
        index.AllowRowLocks = false;
        index.AllowPageLocks = false;
        index.IgnoreDupKey = true;
        table.Indexes.Add(index);

        var snapshot = Snapshot("Round", TableObject(table));
        var reloaded = JsonSerializer.Deserialize<DatabaseSnapshot>(
            JsonSerializer.Serialize(snapshot, JsonOptions), JsonOptions);

        var roundTripped = reloaded!.Objects[0].Table!;
        Assert.False(new TableDiffer().Diff(table, roundTripped, includeDrops: true).HasChanges);
        Assert.Equal(SqlRender.BuildTableCreateScript(table), SqlRender.BuildTableCreateScript(roundTripped));
        Assert.True(roundTripped.Columns[1].IsSparse);
        Assert.Equal((byte)85, roundTripped.KeyConstraints[0].FillFactor);
        Assert.False(roundTripped.Indexes[0].AllowPageLocks);
    }

    private static TableModel LoadLegacyTable() =>
        JsonSerializer.Deserialize<DatabaseSnapshot>(LegacySnapshotJson, JsonOptions)!.Objects[0].Table!;
}
