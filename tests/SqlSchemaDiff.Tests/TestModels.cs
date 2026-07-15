using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.Tests;

/// <summary>Small builders to keep the tests readable.</summary>
internal static class TestModels
{
    public static ColumnModel Col(
        string name,
        string typeName = "int",
        short maxLength = 4,
        byte precision = 10,
        byte scale = 0,
        bool nullable = true,
        bool identity = false,
        string? collation = null,
        string? defaultName = null,
        string? defaultDefinition = null)
        => new()
        {
            Name = name,
            TypeSchema = "sys",
            TypeName = typeName,
            MaxLength = maxLength,
            Precision = precision,
            Scale = scale,
            IsNullable = nullable,
            IsIdentity = identity,
            IdentitySeed = identity ? "1" : null,
            IdentityIncrement = identity ? "1" : null,
            CollationName = collation,
            DefaultName = defaultName,
            DefaultDefinition = defaultDefinition
        };

    public static ColumnModel NVarchar(string name, short lengthChars, bool nullable = true, string collation = "SQL_Latin1_General_CP1_CI_AS")
        => new()
        {
            Name = name,
            TypeSchema = "sys",
            TypeName = "nvarchar",
            MaxLength = (short)(lengthChars * 2),
            IsNullable = nullable,
            CollationName = collation
        };

    public static TableModel Table(string name, params ColumnModel[] columns)
        => new() { Schema = "dbo", Name = name, Columns = columns.ToList() };

    public static IndexModel Index(string name, bool unique, params string[] keyColumns)
        => new()
        {
            Name = name,
            IsUnique = unique,
            TypeDesc = "NONCLUSTERED",
            Columns = keyColumns.Select((c, i) => new IndexColumnModel
            {
                Name = c,
                KeyOrdinal = (byte)(i + 1),
                IndexColumnId = i + 1
            }).ToList()
        };

    public static DbSchemaObject TableObject(TableModel table, string definition = "")
        => new()
        {
            Type = DbObjectType.Table,
            Schema = table.Schema,
            Name = table.Name,
            Definition = string.IsNullOrEmpty(definition) ? $"CREATE TABLE [{table.Schema}].[{table.Name}] (...) {table.Columns.Count}" : definition,
            Table = table
        };

    public static DatabaseSnapshot Snapshot(string name, params DbSchemaObject[] objects)
        => new() { DatabaseName = name, Objects = objects.ToList() };
}
