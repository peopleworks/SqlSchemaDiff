namespace SqlSchemaDiff.Models;

/// <summary>
/// One stage of a schema script. A restore runs the phases in order, so that
/// everything a statement needs already exists when it runs: schemas and types
/// before tables, rows before index maintenance, foreign keys once every table
/// they point at is there.
/// <para>
/// <see cref="FileName"/> is the conventional name for the phase when the script
/// is written out as a directory of files; the numeric prefix keeps them in
/// order in a listing.
/// </para>
/// </summary>
public sealed class ScriptPhase
{
    public string Name { get; init; } = string.Empty;

    public string FileName { get; init; } = string.Empty;

    public IReadOnlyList<ScriptBatch> Batches { get; init; } = Array.Empty<ScriptBatch>();
}

/// <summary>
/// One executable unit of a phase, with a human-readable description used as its
/// comment in the concatenated script.
/// </summary>
public sealed class ScriptBatch
{
    /// <summary>What this batch creates, e.g. <c>Table [dbo].[Orders]</c>.</summary>
    public string Describe { get; init; } = string.Empty;

    /// <summary>
    /// The SQL. Normally a single batch; it carries its own <c>GO</c> separators
    /// only when the statements cannot be split without changing their meaning —
    /// a module whose <c>SET</c> options have to take effect in an earlier batch.
    /// </summary>
    public string Sql { get; init; } = string.Empty;

    /// <summary>
    /// True when the batch may legitimately fail on a first pass and succeed on a
    /// later one, because it can reference an object that a later phase creates:
    /// a module, a computed column or a check constraint calling a scalar
    /// function. A restore driver re-runs these until a pass makes no progress.
    /// </summary>
    public bool Retryable { get; init; }
}

/// <summary>Shapes the phases <c>ScriptComposer.ComposePhases</c> produces.</summary>
public sealed class ComposeOptions
{
    public static ComposeOptions Default { get; } = new();

    /// <summary>
    /// When false (the default), a table is created with its keys, checks and
    /// indexes attached, and only foreign keys are deferred — the shape a
    /// schema-only script wants.
    /// <para>
    /// When true, the tables phase creates bare tables with inline defaults only,
    /// and keys, indexes, checks and foreign keys each move to their own later
    /// phase. That is the shape a restore wants: rows load into a table with no
    /// index to maintain and no constraint to validate per row.
    /// </para>
    /// </summary>
    public bool ConstraintsAfterData { get; init; }
}
