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

    [JsonIgnore]
    public string Identifier => $"[{Schema}].[{Name}]";

    [JsonIgnore]
    public string Key => $"{Type}:{Schema}.{Name}";
}
