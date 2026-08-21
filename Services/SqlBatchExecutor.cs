using Microsoft.Data.SqlClient;

namespace SqlSchemaDiff.Services;

public sealed class SqlBatchExecutor
{
    /// <summary>
    /// Executes the script batch-by-batch (split on <c>GO</c>). When
    /// <paramref name="useTransaction"/> is true, all batches run inside a single
    /// transaction that is rolled back if any batch fails, leaving the target in
    /// its original state (SQL Server DDL is transactional).
    /// </summary>
    public async Task<BatchExecutionResult> ExecuteAsync(
        string connectionString,
        string script,
        bool dryRun,
        int commandTimeoutSeconds,
        bool useTransaction,
        CancellationToken cancellationToken)
    {
        var batches = SplitBatches(script);

        if(dryRun)
            return new BatchExecutionResult(batches.Count, 0, RolledBack: false, Transactional: useTransaction);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        SqlTransaction? transaction = useTransaction
            ? (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken)
            : null;

        var executed = 0;
        try
        {
            foreach(var batch in batches)
            {
                await using var command = connection.CreateCommand();
                command.CommandText = batch;
                command.CommandTimeout = commandTimeoutSeconds;
                if(transaction is not null)
                    command.Transaction = transaction;
                await command.ExecuteNonQueryAsync(cancellationToken);
                executed++;
            }

            if(transaction is not null)
                await transaction.CommitAsync(cancellationToken);

            return new BatchExecutionResult(batches.Count, executed, RolledBack: false, Transactional: useTransaction);
        }
        catch
        {
            if(transaction is not null)
                await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
        finally
        {
            if(transaction is not null)
                await transaction.DisposeAsync();
        }
    }

    /// <summary>
    /// Splits the script into batches on <c>GO</c>. See <see cref="SqlBatchSplitter"/>
    /// for why this is not a line-by-line regular expression.
    /// </summary>
    public static List<string> SplitBatches(string script) => SqlBatchSplitter.Split(script);
}

public sealed record BatchExecutionResult(int BatchCount, int Executed, bool RolledBack, bool Transactional);
