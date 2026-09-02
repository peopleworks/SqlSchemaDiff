using System.Text.Json.Serialization;

namespace SqlSchemaDiff.Models;

public sealed class DbSchemaObject
{
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public DbObjectType Type { get; init; }

    public string Schema { get; init; } = string.Empty;

    public string Name { get; init; } = string.Empty;

    public string Definition { get; init; } = string.Empty;

    public List<string> Dependencies { get; init; } = new();

    /// <summary>
    /// Structured table metadata. Populated only for <see cref="DbObjectType.Table"/>;
    /// null for programmable objects. Enables column-level ALTER diffing.
    /// </summary>
    public TableModel? Table { get; init; }

    /// <summary>
    /// <c>sys.sql_modules.uses_ansi_nulls</c> for a programmable object: the
    /// setting the module was created with, which SQL Server re-applies every time
    /// it runs. Null means unknown (an older snapshot, or a non-module) and is
    /// treated as ON, which is the default and by far the common case.
    /// </summary>
    public bool? UsesAnsiNulls { get; init; }

    /// <summary><c>sys.sql_modules.uses_quoted_identifier</c>. See <see cref="UsesAnsiNulls"/>.</summary>
    public bool? UsesQuotedIdentifier { get; init; }

    [JsonIgnore]
    public string Identifier => $"[{Schema}].[{Name}]";

    [JsonIgnore]
    public string Key => $"{Type}:{Schema}.{Name}";
}
