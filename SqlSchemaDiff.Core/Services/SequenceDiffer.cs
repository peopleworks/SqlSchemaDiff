using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.Services;

/// <summary>
/// Compares two sequences and decides between an in-place <c>ALTER SEQUENCE</c>
/// and a recreate.
/// <para>
/// The split follows what SQL Server allows: increment, bounds, cycling and
/// caching can be altered on a live sequence; the declared type and the start
/// value cannot. <c>current_value</c> is captured but never compared — it moves
/// with every use of the sequence, and that is not schema drift.
/// </para>
/// </summary>
public static class SequenceDiffer
{
    public sealed class SequenceDiffResult
    {
        public string? Script { get; init; }

        /// <summary>True when the change cannot be made with ALTER: DROP + CREATE.</summary>
        public bool RequiresRecreate { get; init; }

        public List<string> Warnings { get; init; } = new();

        public bool HasChanges => RequiresRecreate || Script is not null;
    }

    private static readonly SequenceDiffResult NoChanges = new();

    public static SequenceDiffResult Diff(SequenceModel source, SequenceModel target)
    {
        if(RequiresRecreate(source, target, out var reason))
        {
            return new SequenceDiffResult
            {
                RequiresRecreate = true,
                Warnings =
                {
                    $"-- WARNING: sequence {SqlRender.Quote(source.Schema, source.Name)} is recreated ({reason}); " +
                    "a sequence's type and start value cannot be altered.",
                    "-- The current value is lost: the recreated sequence restarts at START WITH. " +
                    "DROP also fails while a column default still references it."
                }
            };
        }

        var alter = SqlRender.BuildSequenceAlter(source, target);
        return alter is null ? NoChanges : new SequenceDiffResult { Script = alter };
    }

    private static bool RequiresRecreate(SequenceModel source, SequenceModel target, out string reason)
    {
        if(!string.Equals(source.TypeName, target.TypeName, StringComparison.OrdinalIgnoreCase) ||
            source.Precision != target.Precision ||
            source.Scale != target.Scale)
        {
            reason = $"type {SqlRender.BuildSequenceTypeName(target)} -> {SqlRender.BuildSequenceTypeName(source)}";
            return true;
        }

        if(!SqlRender.NumericEquals(source.StartValue, target.StartValue))
        {
            reason = $"start value {target.StartValue} -> {source.StartValue}";
            return true;
        }

        reason = string.Empty;
        return false;
    }
}
