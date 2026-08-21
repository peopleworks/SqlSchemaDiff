namespace SqlSchemaDiff.Models;

public sealed class DatabaseSnapshot
{
    public string DatabaseName { get; init; } = string.Empty;

    public DateTimeOffset GeneratedAtUtc { get; init; }

    /// <summary>
    /// Non-<c>dbo</c> schemas that own at least one captured object. Emitted as a
    /// guarded preamble so a target database that lacks them can still receive the
    /// script. Empty on snapshots produced before v1.3.
    /// </summary>
    public List<string> Schemas { get; init; } = new();

    /// <summary>
    /// User-defined alias types referenced by captured tables. Like schemas, these
    /// are prerequisites rather than diffable objects. Empty on pre-v1.3 snapshots.
    /// </summary>
    public List<AliasTypeModel> Types { get; init; } = new();

    public List<DbSchemaObject> Objects { get; init; } = new();
}
