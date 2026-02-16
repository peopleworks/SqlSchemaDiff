namespace SqlSchemaDiff.Models;

public sealed class DatabaseSnapshot
{
    public string DatabaseName { get; init; } = string.Empty;

    public DateTimeOffset GeneratedAtUtc { get; init; }

    public List<DbSchemaObject> Objects { get; init; } = new();
}
