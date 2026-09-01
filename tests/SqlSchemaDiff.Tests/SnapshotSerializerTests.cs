using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

public class SnapshotSerializerTests
{
    [Fact]
    public void RoundTripPreservesTableConstraintsIndexesAndViewDependencies()
    {
        var table = new TableModel
        {
            Schema = "dbo",
            Name = "Orders",
            Columns = new()
            {
                Col("Id", "int", identity: true, nullable: false),
                Col("CustomerId", "int", nullable: false),
                NVarchar("Notes", 200)
            },
            KeyConstraints = new()
            {
                new KeyConstraintModel
                {
                    TypeCode = "PK",
                    Name = "PK_Orders",
                    IsSystemNamed = false,
                    IndexTypeDesc = "CLUSTERED",
                    Columns = new() { new IndexColumnModel { Name = "Id", KeyOrdinal = 1, IndexColumnId = 1 } }
                }
            },
            ForeignKeys = new()
            {
                new ForeignKeyModel
                {
                    Name = "FK_Orders_Customers",
                    ReferencedSchema = "dbo",
                    ReferencedTable = "Customers",
                    DeleteActionDesc = "CASCADE",
                    UpdateActionDesc = "NO_ACTION",
                    Columns = new() { new ForeignKeyColumnModel { ParentColumn = "CustomerId", ReferencedColumn = "Id" } }
                }
            },
            CheckConstraints = new()
            {
                new CheckConstraintModel
                {
                    Name = "CK_Orders_Notes",
                    Definition = "([Notes] IS NULL OR LEN([Notes])>(0))"
                }
            },
            Indexes = new() { Index("IX_Orders_CustomerId", unique: false, "CustomerId") }
        };

        var tableObject = TableObject(table, "CREATE TABLE [dbo].[Orders] (...)");
        var viewObject = new DbSchemaObject
        {
            Type = DbObjectType.View,
            Schema = "dbo",
            Name = "vOrderSummary",
            Definition = "CREATE VIEW [dbo].[vOrderSummary] AS SELECT * FROM [dbo].[Orders]",
            Dependencies = new() { "dbo.Orders" }
        };

        var original = new DatabaseSnapshot
        {
            DatabaseName = "Northwind",
            GeneratedAtUtc = DateTimeOffset.Parse("2026-01-01T00:00:00+00:00"),
            Schemas = new() { "sales" },
            Types = new()
            {
                new AliasTypeModel
                {
                    Schema = "dbo",
                    Name = "PhoneNumber",
                    BaseTypeName = "varchar",
                    MaxLength = 20,
                    IsNullable = true
                }
            },
            Objects = new() { tableObject, viewObject },
            GeneratedBy = "sqldiff 1.6.0"
        };

        var json = SnapshotSerializer.Serialize(original);
        var roundTripped = SnapshotSerializer.Deserialize(json);

        // Re-serialization equality catches drift anywhere in the object graph.
        Assert.Equal(json, SnapshotSerializer.Serialize(roundTripped));

        // A handful of targeted checks make a future failure easier to diagnose.
        Assert.Equal(original.DatabaseName, roundTripped.DatabaseName);
        Assert.Equal(original.GeneratedAtUtc, roundTripped.GeneratedAtUtc);
        Assert.Equal(original.FormatVersion, roundTripped.FormatVersion);
        Assert.Equal(original.GeneratedBy, roundTripped.GeneratedBy);
        Assert.Equal(original.Schemas, roundTripped.Schemas);

        var roundTrippedTable = roundTripped.Objects.Single(o => o.Type == DbObjectType.Table).Table;
        Assert.NotNull(roundTrippedTable);
        Assert.Equal(3, roundTrippedTable!.Columns.Count);
        Assert.Single(roundTrippedTable.KeyConstraints);
        Assert.Single(roundTrippedTable.ForeignKeys);
        Assert.Equal("Customers", roundTrippedTable.ForeignKeys[0].ReferencedTable);
        Assert.Single(roundTrippedTable.CheckConstraints);
        Assert.Single(roundTrippedTable.Indexes);

        var roundTrippedView = roundTripped.Objects.Single(o => o.Type == DbObjectType.View);
        Assert.Equal(new List<string> { "dbo.Orders" }, roundTrippedView.Dependencies);
    }

    [Fact]
    public void PreVersioningJsonLoadsAsFormatVersion1()
    {
        const string json = """
            {
              "DatabaseName": "Legacy",
              "GeneratedAtUtc": "2024-01-01T00:00:00+00:00",
              "Schemas": [],
              "Types": [],
              "Objects": []
            }
            """;

        var snapshot = SnapshotSerializer.Deserialize(json);

        Assert.Equal(1, snapshot.FormatVersion);
        Assert.Null(snapshot.GeneratedBy);
        Assert.Equal("Legacy", snapshot.DatabaseName);
    }

    [Fact]
    public void UnknownPropertiesAreIgnored()
    {
        const string json = """
            {
              "DatabaseName": "Foo",
              "GeneratedAtUtc": "2024-01-01T00:00:00+00:00",
              "SomethingFromAFutureVersion": { "nested": true, "list": [1, 2, 3] },
              "Objects": []
            }
            """;

        var snapshot = SnapshotSerializer.Deserialize(json);

        Assert.Equal("Foo", snapshot.DatabaseName);
        Assert.Empty(snapshot.Objects);
    }

    [Fact]
    public void EnumsSerializeAsStrings()
    {
        var snapshot = Snapshot("Db", TableObject(Table("Widgets", Col("Id"))));

        var json = SnapshotSerializer.Serialize(snapshot);

        Assert.Contains("\"Table\"", json);
        Assert.DoesNotContain("\"Type\": 0", json);
    }

    [Fact]
    public void NewerFormatVersionThrowsNamingBothVersions()
    {
        var futureVersion = SnapshotSerializer.CurrentFormatVersion + 1;
        var json = $$"""
            {
              "DatabaseName": "Foo",
              "FormatVersion": {{futureVersion}},
              "Objects": []
            }
            """;

        var ex = Assert.Throws<SnapshotFormatException>(() => SnapshotSerializer.Deserialize(json));

        Assert.Contains(SnapshotSerializer.CurrentFormatVersion.ToString(), ex.Message);
        Assert.Contains(futureVersion.ToString(), ex.Message);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("null")]
    [InlineData("[]")]
    [InlineData("42")]
    [InlineData("\"just a string\"")]
    public void EmptyOrNonObjectJsonThrows(string json)
    {
        Assert.Throws<SnapshotFormatException>(() => SnapshotSerializer.Deserialize(json));
    }

    [Fact]
    public async Task SaveAsyncThenLoadAsyncRoundTripThroughATempFile()
    {
        var snapshot = Snapshot("Db", TableObject(Table("Widgets", Col("Id"))));
        var path = Path.Combine(Path.GetTempPath(), $"snapshot-{Guid.NewGuid():N}.json");

        try
        {
            await SnapshotSerializer.SaveAsync(snapshot, path, CancellationToken.None);
            var loaded = await SnapshotSerializer.LoadAsync(path, CancellationToken.None);

            Assert.Equal(SnapshotSerializer.Serialize(snapshot), SnapshotSerializer.Serialize(loaded));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task WriteAsyncThenReadAsyncRoundTripThroughAStream()
    {
        var snapshot = Snapshot("Db", TableObject(Table("Widgets", Col("Id"))));

        using var stream = new MemoryStream();
        await SnapshotSerializer.WriteAsync(snapshot, stream, CancellationToken.None);
        stream.Position = 0;
        var loaded = await SnapshotSerializer.ReadAsync(stream, CancellationToken.None);

        Assert.Equal(SnapshotSerializer.Serialize(snapshot), SnapshotSerializer.Serialize(loaded));
    }
}
