namespace SqlSchemaDiff.Models;

/// <summary>
/// Structured representation of a table, captured in the snapshot so the differ
/// can compare column-by-column and emit incremental ALTER statements instead of
/// treating the whole CREATE TABLE script as an opaque text blob.
/// </summary>
public sealed class TableModel
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public List<ColumnModel> Columns { get; set; } = new();
    public List<KeyConstraintModel> KeyConstraints { get; set; } = new();
    public List<ForeignKeyModel> ForeignKeys { get; set; } = new();
    public List<CheckConstraintModel> CheckConstraints { get; set; } = new();
    public List<IndexModel> Indexes { get; set; } = new();
}

public sealed class ColumnModel
{
    public string Name { get; set; } = string.Empty;
    public string TypeSchema { get; set; } = string.Empty;
    public string TypeName { get; set; } = string.Empty;
    public bool IsUserDefinedType { get; set; }
    public short MaxLength { get; set; }
    public byte Precision { get; set; }
    public byte Scale { get; set; }
    public bool IsNullable { get; set; }
    public bool IsIdentity { get; set; }
    public bool IsComputed { get; set; }
    public string? CollationName { get; set; }
    public bool IsRowGuid { get; set; }
    public string? ComputedDefinition { get; set; }
    public bool IsPersisted { get; set; }
    public string? DefaultName { get; set; }
    public string? DefaultDefinition { get; set; }
    public string? IdentitySeed { get; set; }
    public string? IdentityIncrement { get; set; }
}

public sealed class KeyConstraintModel
{
    /// <summary>"PK" or "UQ".</summary>
    public string TypeCode { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string IndexTypeDesc { get; set; } = string.Empty;
    public List<IndexColumnModel> Columns { get; set; } = new();
}

public sealed class ForeignKeyModel
{
    public string Name { get; set; } = string.Empty;
    public string ReferencedSchema { get; set; } = string.Empty;
    public string ReferencedTable { get; set; } = string.Empty;
    public string DeleteActionDesc { get; set; } = string.Empty;
    public string UpdateActionDesc { get; set; } = string.Empty;
    public bool IsNotForReplication { get; set; }
    public bool IsNotTrusted { get; set; }
    public bool IsDisabled { get; set; }
    public List<ForeignKeyColumnModel> Columns { get; set; } = new();
}

public sealed class ForeignKeyColumnModel
{
    public string ParentColumn { get; set; } = string.Empty;
    public string ReferencedColumn { get; set; } = string.Empty;
}

public sealed class CheckConstraintModel
{
    public string Name { get; set; } = string.Empty;
    public string Definition { get; set; } = string.Empty;
    public bool IsNotTrusted { get; set; }
    public bool IsDisabled { get; set; }
}

public sealed class IndexModel
{
    public string Name { get; set; } = string.Empty;
    public bool IsUnique { get; set; }
    public string TypeDesc { get; set; } = string.Empty;
    public string? FilterDefinition { get; set; }
    public bool IsDisabled { get; set; }
    public List<IndexColumnModel> Columns { get; set; } = new();
}

public sealed class IndexColumnModel
{
    public string Name { get; set; } = string.Empty;
    public byte KeyOrdinal { get; set; }
    public bool IsDescending { get; set; }
    public bool IsIncluded { get; set; }
    public int IndexColumnId { get; set; }
}
