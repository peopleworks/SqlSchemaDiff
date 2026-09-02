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

    /// <summary>
    /// Row or page compression on the table's own data, taken from
    /// <c>sys.partitions.data_compression_desc</c> for partition 1 of the heap
    /// (<c>index_id 0</c>) or the clustered index (<c>index_id 1</c>). Null on a
    /// snapshot written before this was captured and on an uncompressed table.
    /// Only a heap renders it on CREATE TABLE: when the table has a clustered
    /// index the setting belongs to that index and is scripted there.
    /// </summary>
    public string? DataCompression { get; set; }
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
    /// <summary>True when SQL Server generated the default constraint name (e.g. <c>DF__Orders__Total__5629CD9C</c>).</summary>
    public bool DefaultIsSystemNamed { get; set; }
    public string? IdentitySeed { get; set; }
    public string? IdentityIncrement { get; set; }

    /// <summary>
    /// <c>sys.columns.is_sparse</c>. A sparse column costs no storage for its NULLs
    /// and in exchange must be nullable. False on a snapshot written before this
    /// was captured, which is also the value for every ordinary column.
    /// </summary>
    public bool IsSparse { get; set; }
}

public sealed class KeyConstraintModel : IIndexStorageOptions
{
    /// <summary>"PK" or "UQ".</summary>
    public string TypeCode { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    /// <summary>True when SQL Server generated the name; such constraints are matched by shape, not by name.</summary>
    public bool IsSystemNamed { get; set; }
    public string IndexTypeDesc { get; set; } = string.Empty;
    public List<IndexColumnModel> Columns { get; set; } = new();

    // The index behind a PRIMARY KEY or UNIQUE constraint is a real index and
    // carries the same storage options as a standalone one.
    public byte FillFactor { get; set; }
    public bool IsPadded { get; set; }
    public bool IgnoreDupKey { get; set; }
    public bool AllowRowLocks { get; set; } = true;
    public bool AllowPageLocks { get; set; } = true;
    public string? DataCompression { get; set; }
}

public sealed class ForeignKeyModel
{
    public string Name { get; set; } = string.Empty;
    /// <summary>True when SQL Server generated the name; such constraints are matched by shape, not by name.</summary>
    public bool IsSystemNamed { get; set; }
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
    /// <summary>True when SQL Server generated the name; such constraints are matched by shape, not by name.</summary>
    public bool IsSystemNamed { get; set; }
    public string Definition { get; set; } = string.Empty;
    public bool IsNotTrusted { get; set; }
    public bool IsDisabled { get; set; }
}

/// <summary>
/// The options an index carries in its <c>WITH (...)</c> clause. A standalone index
/// and the index behind a PRIMARY KEY or UNIQUE constraint accept the same clause,
/// so both models expose it through this interface and share one renderer and one
/// comparison.
/// </summary>
/// <remarks>
/// Every member's default is the value SQL Server itself uses when the option is
/// left unsaid — which is why <see cref="AllowRowLocks"/> and
/// <see cref="AllowPageLocks"/> start out true. A snapshot written before these were
/// captured therefore deserializes to "everything at its default" and compares equal
/// to a freshly extracted index that is also using the defaults, instead of reading
/// as drift on every table.
/// </remarks>
public interface IIndexStorageOptions
{
    /// <summary><c>sys.indexes.fill_factor</c>; 0 means the server default.</summary>
    byte FillFactor { get; set; }

    /// <summary><c>PAD_INDEX</c>; off by default.</summary>
    bool IsPadded { get; set; }

    /// <summary><c>IGNORE_DUP_KEY</c>; off by default, and only legal on a unique index.</summary>
    bool IgnoreDupKey { get; set; }

    /// <summary><c>ALLOW_ROW_LOCKS</c>; on by default.</summary>
    bool AllowRowLocks { get; set; }

    /// <summary><c>ALLOW_PAGE_LOCKS</c>; on by default.</summary>
    bool AllowPageLocks { get; set; }

    /// <summary>
    /// <c>sys.partitions.data_compression_desc</c> for partition 1: NONE, ROW, PAGE,
    /// COLUMNSTORE or COLUMNSTORE_ARCHIVE. Null means it was never captured; null,
    /// NONE and (for a columnstore index) COLUMNSTORE all mean "not scripted".
    /// </summary>
    string? DataCompression { get; set; }
}

public sealed class IndexModel : IIndexStorageOptions
{
    public string Name { get; set; } = string.Empty;
    public bool IsUnique { get; set; }
    /// <summary>
    /// <c>sys.indexes.type_desc</c>: CLUSTERED, NONCLUSTERED, CLUSTERED COLUMNSTORE
    /// or NONCLUSTERED COLUMNSTORE. It is what tells the renderer which CREATE INDEX
    /// grammar to use, so it doubles as the index kind.
    /// </summary>
    public string TypeDesc { get; set; } = string.Empty;
    public string? FilterDefinition { get; set; }
    public bool IsDisabled { get; set; }
    public List<IndexColumnModel> Columns { get; set; } = new();

    public byte FillFactor { get; set; }
    public bool IsPadded { get; set; }
    public bool IgnoreDupKey { get; set; }
    public bool AllowRowLocks { get; set; } = true;
    public bool AllowPageLocks { get; set; } = true;
    public string? DataCompression { get; set; }
}

public sealed class IndexColumnModel
{
    public string Name { get; set; } = string.Empty;
    public byte KeyOrdinal { get; set; }
    public bool IsDescending { get; set; }
    public bool IsIncluded { get; set; }
    public int IndexColumnId { get; set; }
}
