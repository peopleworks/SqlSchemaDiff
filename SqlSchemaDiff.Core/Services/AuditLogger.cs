using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlSchemaDiff.Services;

/// <summary>
/// Appends a human-readable audit entry for each apply/deploy operation so there
/// is a durable record of what was executed, where, and whether it succeeded.
/// </summary>
public static class AuditLogger
{
    public static async Task AppendAsync(
        string logPath,
        string command,
        string connectionString,
        string? scriptPath,
        BatchExecutionResult? result,
        string outcome,
        string? error,
        DateTimeOffset timestampUtc,
        CancellationToken cancellationToken)
    {
        var (server, database) = DescribeTarget(connectionString);

        var sb = new StringBuilder();
        sb.AppendLine("----------------------------------------");
        sb.AppendLine($"Timestamp (UTC): {timestampUtc:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine($"Command        : {command}");
        sb.AppendLine($"Server         : {server}");
        sb.AppendLine($"Database       : {database}");
        if(!string.IsNullOrWhiteSpace(scriptPath))
            sb.AppendLine($"Script         : {scriptPath}");
        if(result is not null)
        {
            sb.AppendLine($"Batches        : {result.Executed}/{result.BatchCount} executed");
            sb.AppendLine($"Transactional  : {(result.Transactional ? "yes" : "no")}");
        }
        sb.AppendLine($"Outcome        : {outcome}");
        if(!string.IsNullOrWhiteSpace(error))
            sb.AppendLine($"Error          : {error}");

        var directory = Path.GetDirectoryName(Path.GetFullPath(logPath));
        if(!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        await File.AppendAllTextAsync(logPath, sb.ToString(), cancellationToken);
    }

    private static (string Server, string Database) DescribeTarget(string connectionString)
    {
        try
        {
            var builder = new SqlConnectionStringBuilder(connectionString);
            return (
                string.IsNullOrWhiteSpace(builder.DataSource) ? "(unknown)" : builder.DataSource,
                string.IsNullOrWhiteSpace(builder.InitialCatalog) ? "(unknown)" : builder.InitialCatalog);
        }
        catch
        {
            return ("(unparseable)", "(unparseable)");
        }
    }
}
