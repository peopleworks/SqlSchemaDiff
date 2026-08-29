namespace SqlSchemaDiff.Models;

/// <summary>
/// A user-defined alias type (<c>CREATE TYPE ... FROM ...</c>). Tables reference
/// these by name, so a target database that lacks the type cannot receive the
/// table at all — they are captured as a prerequisite of the snapshot rather than
/// as a diffable object.
/// </summary>
public sealed class AliasTypeModel
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string BaseTypeName { get; set; } = string.Empty;
    public short MaxLength { get; set; }
    public byte Precision { get; set; }
    public byte Scale { get; set; }
    public bool IsNullable { get; set; }
    public string? CollationName { get; set; }
}
