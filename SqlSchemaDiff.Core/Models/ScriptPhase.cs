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

    /// <summary>
    /// When true, the finalize phase restarts every sequence at the value the
    /// source had handed out, so numbering continues where it left off. Off by
    /// default: a schema-only script has no reason to move a sequence, and a
    /// restore that loads rows is the one caller that wants it.
    /// </summary>
    public bool RestartSequences { get; init; }

    /// <summary>
    /// When false (the default), a table that declares a <c>SYSTEM_TIME</c> period
    /// carries it inline in <c>CREATE TABLE</c>, which is what makes its two period
    /// columns <c>GENERATED ALWAYS</c> — the shape a schema-only script wants, and
    /// the only shape this engine emitted before 1.8.
    /// <para>
    /// When true, the tables phase emits those two columns as plain
    /// <c>datetime2 NOT NULL</c> and the period itself moves to the finalize phase as
    /// <c>ALTER TABLE ... ADD PERIOD FOR SYSTEM_TIME</c>, in front of the
    /// <c>SYSTEM_VERSIONING = ON</c> that is already there. That is the shape a
    /// restore wants: SQL Server refuses an INSERT that names a <c>GENERATED ALWAYS</c>
    /// column (error 13536), so with the period inline every restored row is stamped
    /// with the instant of the restore and the timeline between the end of the
    /// archived history and that instant answers nothing.
    /// </para>
    /// </summary>
    /// <remarks>
    /// <c>ADD PERIOD</c> on a populated table enforces conditions the loaded rows have
    /// to meet — see <see cref="Services.SqlRender.BuildPeriodAdd"/>, which lists the
    /// ones measured against SQL Server 2025.
    /// </remarks>
    public bool PeriodAfterData { get; init; }
}
