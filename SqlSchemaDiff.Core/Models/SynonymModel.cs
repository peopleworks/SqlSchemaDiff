namespace SqlSchemaDiff.Models;

/// <summary>
/// A synonym (<c>sys.synonyms</c>): a local name for an object that lives
/// somewhere else — another schema, another database, another server.
/// <para>
/// <see cref="BaseObjectName"/> is stored and emitted <b>verbatim</b>. SQL Server
/// already hands it back bracket-quoted and one to four parts long
/// (<c>[schema].[object]</c>, <c>[db].[schema].[object]</c>,
/// <c>[server].[db].[schema].[object]</c>), so quoting it again would produce
/// <c>[[db]].[[dbo]].[[T]]</c> and splitting it would have to guess which parts
/// are present. <c>CREATE SYNONYM</c> never resolves its target either — the name
/// is looked up when the synonym is used, not when it is created — so passing the
/// text straight through is both the simplest and the only correct thing to do.
/// </para>
/// <para>
/// There is no <c>ALTER SYNONYM</c>. A synonym that points somewhere else is
/// dropped and created again, which costs nothing: a synonym is a name and holds
/// no data.
/// </para>
/// </summary>
public sealed class SynonymModel
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;

    /// <summary><c>sys.synonyms.base_object_name</c>, exactly as the catalog reports it.</summary>
    public string BaseObjectName { get; set; } = string.Empty;
}
