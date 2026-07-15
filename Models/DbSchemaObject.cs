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

    [JsonIgnore]
    public string Identifier => $"[{Schema}].[{Name}]";

    [JsonIgnore]
    public string Key => $"{Type}:{Schema}.{Name}";
}
