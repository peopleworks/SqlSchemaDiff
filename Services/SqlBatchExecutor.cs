using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

namespace SqlSchemaDiff.Services;

public sealed class SqlBatchExecutor
{
    public async Task<int> ExecuteAsync(
        string connectionString,
        string script,
        bool dryRun,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        var batches = SplitBatches(script);

        if(dryRun)
            return batches.Count;

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var executed = 0;
        foreach(var batch in batches)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = batch;
            command.CommandTimeout = commandTimeoutSeconds;
            await command.ExecuteNonQueryAsync(cancellationToken);
            executed++;
        }

        return executed;
    }

    private static List<string> SplitBatches(string script)
    {
        var result = new List<string>();
        var current = new StringBuilder();

        var lines = script.Replace("\r\n", "\n").Split('\n');
        foreach(var line in lines)
        {
            if(Regex.IsMatch(line, @"^\s*GO\s*(--.*)?$", RegexOptions.IgnoreCase))
            {
                AddBatchIfAny(result, current);
                continue;
            }

            current.AppendLine(line);
        }

        AddBatchIfAny(result, current);
        return result;
    }

    private static void AddBatchIfAny(List<string> batches, StringBuilder sb)
    {
        var content = sb.ToString().Trim();
        if(content.Length > 0)
            batches.Add(content);
        sb.Clear();
    }
}
