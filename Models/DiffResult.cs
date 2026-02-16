namespace SqlSchemaDiff.Models;

public sealed class DiffResult
{
    public string Script { get; init; } = string.Empty;

    public int Added { get; init; }

    public int Changed { get; init; }

    public int Removed { get; init; }

    public int Skipped { get; init; }

    public bool HasChanges => Added > 0 || Changed > 0 || Removed > 0;
}
