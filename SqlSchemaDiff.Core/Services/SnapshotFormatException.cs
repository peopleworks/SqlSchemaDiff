namespace SqlSchemaDiff.Services;

/// <summary>
/// Thrown by <see cref="SnapshotSerializer"/> when JSON handed to it is not a
/// usable snapshot: the document does not deserialize to an object at all (an
/// empty file, a JSON array or scalar, or the literal <c>null</c>), or it names a
/// <see cref="Models.DatabaseSnapshot.FormatVersion"/> newer than this build of
/// SqlSchemaDiff.Core understands.
/// </summary>
public sealed class SnapshotFormatException : Exception
{
    public SnapshotFormatException(string message) : base(message)
    {
    }

    public SnapshotFormatException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
