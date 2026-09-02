namespace SqlSchemaDiff.Models;

/// <summary>
/// A sequence object (<c>sys.sequences</c>).
/// <para>
/// The numeric properties are <b>strings</b>, not integers. A sequence may be
/// declared <c>AS decimal(38,0)</c>, whose bounds run to 10^38-1: too wide for
/// <see cref="long"/> and also too wide for <see cref="decimal"/> (29 significant
/// digits). SQL Server requires scale 0 for a sequence, so every value is a whole
/// number and a string round-trips it exactly — through JSON and straight into the
/// generated DDL — with no precision loss and no culture-dependent formatting.
/// Arithmetic (see <c>SqlRender.BuildSequenceRestart</c>) uses <c>BigInteger</c>.
/// </para>
/// </summary>
public sealed class SequenceModel
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;

    /// <summary>The declared type name, e.g. <c>bigint</c> or <c>decimal</c>.</summary>
    public string TypeName { get; set; } = string.Empty;

    public byte Precision { get; set; }
    public byte Scale { get; set; }

    public string StartValue { get; set; } = string.Empty;
    public string Increment { get; set; } = string.Empty;

    /// <summary>Null or empty renders as <c>NO MINVALUE</c>.</summary>
    public string? MinValue { get; set; }

    /// <summary>Null or empty renders as <c>NO MAXVALUE</c>.</summary>
    public string? MaxValue { get; set; }

    public bool IsCycling { get; set; }
    public bool IsCached { get; set; }

    /// <summary>Null with <see cref="IsCached"/> true means <c>CACHE</c> with a server-chosen size.</summary>
    public int? CacheSize { get; set; }

    /// <summary>
    /// The last value handed out, captured for a dump/restore tool so it can
    /// resume the sequence. Deliberately <b>excluded</b> from diff equality: it
    /// moves every time the sequence is used, and that is not schema drift.
    /// </summary>
    public string? CurrentValue { get; set; }
}
