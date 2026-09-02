namespace SqlSchemaDiff.Models;

/// <summary>
/// A user-defined table type (<c>CREATE TYPE ... AS TABLE</c>), the type behind a
/// table-valued parameter.
/// <para>
/// It reuses <see cref="ColumnModel"/>, <see cref="KeyConstraintModel"/> and
/// <see cref="CheckConstraintModel"/> because the catalog stores a table type as a
/// hidden table: its columns and constraints hang off
/// <c>sys.table_types.type_table_object_id</c> exactly as a table's do off its
/// <c>object_id</c>.
/// </para>
/// <para>
/// A table type cannot be altered, so every constraint is rendered inline in the
/// CREATE and any change means DROP + CREATE.
/// </para>
/// </summary>
public sealed class TableTypeModel
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public List<ColumnModel> Columns { get; set; } = new();

    /// <summary>Primary key and unique constraints, rendered inline.</summary>
    public List<KeyConstraintModel> KeyConstraints { get; set; } = new();

    public List<CheckConstraintModel> CheckConstraints { get; set; } = new();

    public bool IsMemoryOptimized { get; set; }
}
