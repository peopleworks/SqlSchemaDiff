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
    /// Structured trigger metadata. Populated only for <see cref="DbObjectType.Trigger"/>.
    /// Null on snapshots written before 1.6.
    /// </summary>
    public TriggerModel? Trigger { get; init; }

    /// <summary>
    /// Structured sequence metadata. Populated only for <see cref="DbObjectType.Sequence"/>.
    /// Null on snapshots written before 1.6.
    /// </summary>
    public SequenceModel? Sequence { get; init; }

    /// <summary>
    /// Structured table-type metadata. Populated only for <see cref="DbObjectType.TableType"/>.
    /// Null on snapshots written before 1.6.
    /// </summary>
    public TableTypeModel? TableType { get; init; }

    [JsonIgnore]
    public string Identifier => $"[{Schema}].[{Name}]";

    [JsonIgnore]
    public string Key => BuildKey(Type, Schema, Name);

    /// <summary>
    /// The identity used for matching objects across snapshots and for the entries
    /// in <see cref="Dependencies"/>. Exposed so producers of dependency edges
    /// (the extractor, the sequence scanner) cannot drift from <see cref="Key"/>.
    /// </summary>
    public static string BuildKey(DbObjectType type, string schema, string name) => $"{type}:{schema}.{name}";
}
