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

    /// <summary>
    /// Version of the snapshot JSON shape itself, not of the tool that wrote it.
    /// A file with no <c>FormatVersion</c> property predates this field and is
    /// treated as version 1 — see <see cref="Services.SnapshotSerializer"/>.
    /// </summary>
    public int FormatVersion { get; init; } = 1;

    /// <summary>
    /// Free-form "tool name + version" stamp (e.g. <c>"sqldiff 1.6.0"</c>) recorded
    /// for diagnosing which producer wrote a snapshot. The extractor does not set
    /// this; callers that care — the CLI, other consumers — do.
    /// </summary>
    public string? GeneratedBy { get; init; }
}
