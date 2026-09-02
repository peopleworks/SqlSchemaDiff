namespace SqlSchemaDiff.Models;

/// <summary>
/// A DML trigger on a user table. The trigger's own schema and name live on the
/// owning <see cref="DbSchemaObject"/>; this carries the parts that are not in
/// the module text and that a diff has to act on separately.
/// <para>
/// Only DML triggers are captured (<c>sys.triggers.parent_class = 1</c>).
/// Database-level DDL triggers and server-level/logon triggers are deliberately
/// out of scope: they are not schema-bound objects, they cannot be scripted into
/// a schema-only snapshot without also carrying database-wide policy, and
/// applying them to another database is a security decision rather than a
/// schema one.
/// </para>
/// </summary>
public sealed class TriggerModel
{
    public string ParentSchema { get; set; } = string.Empty;
    public string ParentName { get; set; } = string.Empty;

    /// <summary>
    /// Disabled state is not part of the module text, so it has to be diffed on
    /// its own and re-applied with <c>DISABLE TRIGGER</c> / <c>ENABLE TRIGGER</c>.
    /// </summary>
    public bool IsDisabled { get; set; }

    public bool IsInsteadOf { get; set; }
}
