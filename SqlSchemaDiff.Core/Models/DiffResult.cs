namespace SqlSchemaDiff.Models;

public sealed class DiffResult
{
    public string Script { get; init; } = string.Empty;

    public int Added { get; init; }

    public int Changed { get; init; }

    public int Removed { get; init; }

    public int Skipped { get; init; }

    /// <summary>Identifiers of objects created on the target (missing before).</summary>
    public List<string> AddedObjects { get; init; } = new();

    /// <summary>Identifiers of objects that differ and were altered/updated.</summary>
    public List<string> ChangedObjects { get; init; } = new();

    /// <summary>Identifiers of objects dropped from the target.</summary>
    public List<string> RemovedObjects { get; init; } = new();

    public bool HasChanges => Added > 0 || Changed > 0 || Removed > 0;
}
