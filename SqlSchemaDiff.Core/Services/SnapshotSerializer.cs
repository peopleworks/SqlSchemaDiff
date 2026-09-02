using System.Text.Json;
using System.Text.Json.Serialization;
using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.Services;

/// <summary>
/// Reads and writes <see cref="DatabaseSnapshot"/> as JSON with one shared set of
/// options. Before this existed, serialization was inline in the CLI — indented
/// output plus a <see cref="JsonStringEnumConverter"/> so enums round-trip as
/// names rather than ordinals — so any other consumer of the NuGet package had to
/// duplicate those options by hand or risk enum deserialization breaking silently.
/// Use this instead of building a private <see cref="JsonSerializerOptions"/>.
/// </summary>
public static class SnapshotSerializer
{
    /// <summary>
    /// The highest <see cref="DatabaseSnapshot.FormatVersion"/> this build can
    /// load. Bump alongside any change to the snapshot shape that an older reader
    /// could not safely ignore.
    /// </summary>
    public const int CurrentFormatVersion = 1;

    /// <summary>
    /// The options every read and write below uses. Property names match
    /// case-insensitively and unrecognized properties are ignored — the
    /// System.Text.Json default — so a snapshot written by a newer minor version,
    /// carrying properties this build does not know about, still loads.
    /// </summary>
    public static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public static string Serialize(DatabaseSnapshot snapshot) =>
        JsonSerializer.Serialize(snapshot, Options);

    public static DatabaseSnapshot Deserialize(string json)
    {
        try
        {
            return Validate(JsonSerializer.Deserialize<DatabaseSnapshot>(json, Options));
        }
        catch(JsonException ex)
        {
            throw new SnapshotFormatException($"Snapshot JSON could not be parsed: {ex.Message}", ex);
        }
    }

    public static async Task SaveAsync(DatabaseSnapshot snapshot, string path, CancellationToken cancellationToken)
    {
        await using var stream = File.Create(path);
        await WriteAsync(snapshot, stream, cancellationToken);
    }

    public static async Task<DatabaseSnapshot> LoadAsync(string path, CancellationToken cancellationToken)
    {
        await using var stream = File.OpenRead(path);
        return await ReadAsync(stream, cancellationToken);
    }

    public static Task WriteAsync(DatabaseSnapshot snapshot, Stream stream, CancellationToken cancellationToken) =>
        JsonSerializer.SerializeAsync(stream, snapshot, Options, cancellationToken);

    public static async Task<DatabaseSnapshot> ReadAsync(Stream stream, CancellationToken cancellationToken)
    {
        try
        {
            return Validate(await JsonSerializer.DeserializeAsync<DatabaseSnapshot>(stream, Options, cancellationToken));
        }
        catch(JsonException ex)
        {
            throw new SnapshotFormatException($"Snapshot JSON could not be parsed: {ex.Message}", ex);
        }
    }

    private static DatabaseSnapshot Validate(DatabaseSnapshot? snapshot)
    {
        if(snapshot is null)
            throw new SnapshotFormatException("Snapshot JSON did not deserialize to a DatabaseSnapshot object.");

        if(snapshot.FormatVersion > CurrentFormatVersion)
            throw new SnapshotFormatException(
                $"Snapshot format version {snapshot.FormatVersion} is newer than the highest version this build of SqlSchemaDiff.Core supports ({CurrentFormatVersion}). Upgrade the package to read it.");

        return snapshot;
    }
}
