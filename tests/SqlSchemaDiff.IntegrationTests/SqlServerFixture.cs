using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;

namespace SqlSchemaDiff.IntegrationTests;

/// <summary>
/// Hands out throwaway databases on the server named by <c>SQLDIFF_TEST_CONN</c>
/// and removes every one of them when the collection finishes.
/// <para>
/// Scratch databases are named <c>SqlDiffIT_&lt;8 hex&gt;</c> so a run that is killed
/// half way through leaves an obvious, greppable trace on the server rather than
/// something that looks like real data. The drop is unconditional — it runs after
/// failing tests too, because a failed round trip is exactly the run that leaves
/// the most rubbish behind.
/// </para>
/// <para>
/// The fixture never touches the server from its constructor: xunit builds it even
/// when every test in the collection is skipped, so all the work is deferred to
/// the first <see cref="CreateDatabase"/>.
/// </para>
/// </summary>
public sealed class SqlServerFixture : IAsyncLifetime, IDisposable
{
    public const string ConnectionEnvironmentVariable = "SQLDIFF_TEST_CONN";

    /// <summary>Prefix for every database this fixture creates.</summary>
    public const string DatabasePrefix = "SqlDiffIT_";

    private const int CommandTimeoutSeconds = 180;

    private static readonly Lazy<string> FullSchema = new(() => ReadScript("full.sql"));
    private static readonly Lazy<string> AlterSchema = new(() => ReadScript("alter.sql"));
    private static readonly Lazy<string> UnsupportedSchema = new(() => ReadScript("unsupported.sql"));
    private static readonly Lazy<string> RebuildBeforeSchema = new(() => ReadScript("rebuild-before.sql"));
    private static readonly Lazy<string> RebuildAfterSchema = new(() => ReadScript("rebuild-after.sql"));

    private readonly ConcurrentQueue<string> _databases = new();
    private bool _cleanedUp;

    /// <summary>True when the environment names a server to test against.</summary>
    public static bool IsConfigured => !string.IsNullOrWhiteSpace(RawConnectionString);

    /// <summary>The purpose-built schema that exercises every extractor branch.</summary>
    public static string FullSchemaScript => FullSchema.Value;

    /// <summary>Schema drift applied on top of <see cref="FullSchemaScript"/>.</summary>
    public static string AlterSchemaScript => AlterSchema.Value;

    /// <summary>Objects the extractor reports as notices instead of modelling.</summary>
    public static string UnsupportedSchemaScript => UnsupportedSchema.Value;

    /// <summary>A populated table in the shape a rebuild starts from.</summary>
    public static string RebuildBeforeScript => RebuildBeforeSchema.Value;

    /// <summary>The same schema with the identity change only a rebuild can make.</summary>
    public static string RebuildAfterScript => RebuildAfterSchema.Value;

    private static string? RawConnectionString => Environment.GetEnvironmentVariable(ConnectionEnvironmentVariable);

    /// <summary>The configured connection string, pointed at <c>master</c>.</summary>
    public static string ServerConnectionString => Build("master");

    /// <summary>
    /// Creates a fresh, empty database and returns a connection string scoped to
    /// it. The database is registered for cleanup before it is created, so one that
    /// fails half way through still gets dropped.
    /// </summary>
    public string CreateDatabase()
    {
        var name = DatabasePrefix + Guid.NewGuid().ToString("N")[..8];
        _databases.Enqueue(name);

        // CREATE DATABASE cannot run inside a transaction, hence the bare command
        // rather than SqlBatchExecutor.
        Execute(ServerConnectionString, $"CREATE DATABASE [{name}];");
        return Build(name);
    }

    /// <summary>A fresh database with <see cref="FullSchemaScript"/> already applied.</summary>
    public async Task<string> CreateDatabaseWithFullSchemaAsync()
    {
        var connectionString = CreateDatabase();

        // No transaction: full.sql flips QUOTED_IDENTIFIER between batches, and a
        // setup script that half-applies is easier to diagnose than one that
        // silently rolls back.
        await ApplyAsync(connectionString, FullSchemaScript, useTransaction: false);
        return connectionString;
    }

    /// <summary>Runs a GO-separated script through the engine's own batch executor.</summary>
    public static async Task<BatchExecutionResult> ApplyAsync(
        string connectionString,
        string script,
        bool useTransaction = true) =>
        await new SqlBatchExecutor().ExecuteAsync(
            connectionString,
            script,
            dryRun: false,
            CommandTimeoutSeconds,
            useTransaction,
            CancellationToken.None);

    public static async Task<DatabaseSnapshot> ExtractAsync(string connectionString) =>
        (await ExtractWithNoticesAsync(connectionString)).Snapshot;

    public static async Task<(DatabaseSnapshot Snapshot, IReadOnlyList<string> Notices)> ExtractWithNoticesAsync(
        string connectionString)
    {
        var extractor = new SqlServerSchemaExtractor();
        var snapshot = await extractor.ExtractAsync(connectionString, CancellationToken.None);
        return (snapshot, extractor.Notices.ToList());
    }

    /// <summary>Reads a single scalar, for the edition probes the notice test needs.</summary>
    public static async Task<T?> ScalarAsync<T>(string connectionString, string sql)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = CommandTimeoutSeconds;
        var value = await command.ExecuteScalarAsync();
        return value is null || value is DBNull ? default : (T)value;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync()
    {
        Cleanup();
        return Task.CompletedTask;
    }

    public void Dispose() => Cleanup();

    /// <summary>
    /// Drops every database this run created. Idempotent: xunit calls both
    /// <see cref="DisposeAsync"/> and <see cref="Dispose"/> on a collection fixture
    /// that implements both.
    /// </summary>
    private void Cleanup()
    {
        if(_cleanedUp)
            return;

        _cleanedUp = true;
        if(!IsConfigured || _databases.IsEmpty)
            return;

        // A pooled connection still counts as a session, and SET SINGLE_USER waits
        // for sessions. Without this the drop blocks until the pool times out.
        SqlConnection.ClearAllPools();

        var failures = new List<string>();
        while(_databases.TryDequeue(out var name))
        {
            try
            {
                Drop(name);
            }
            catch(SqlException)
            {
                // One retry: something that only just released its connection is the
                // common case, and clearing the pools again usually settles it.
                try
                {
                    SqlConnection.ClearAllPools();
                    Drop(name);
                }
                catch(SqlException ex)
                {
                    failures.Add($"[{name}]: {ex.Message}");
                }
            }
        }

        if(failures.Count > 0)
        {
            throw new InvalidOperationException(
                "Scratch databases were left behind on the test server and have to be dropped by hand:" +
                Environment.NewLine + string.Join(Environment.NewLine, failures));
        }
    }

    private static void Drop(string name) =>
        Execute(ServerConnectionString, $"""
                                         IF DB_ID(N'{name}') IS NOT NULL
                                         BEGIN
                                             ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                                             DROP DATABASE [{name}];
                                         END
                                         """);

    private static void Execute(string connectionString, string sql)
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = CommandTimeoutSeconds;
        command.ExecuteNonQuery();
    }

    private static string Build(string databaseName)
    {
        var raw = RawConnectionString;
        if(string.IsNullOrWhiteSpace(raw))
            throw new InvalidOperationException($"{ConnectionEnvironmentVariable} is not set.");

        return new SqlConnectionStringBuilder(raw) { InitialCatalog = databaseName }.ConnectionString;
    }

    private static string ReadScript(string fileName) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Schemas", fileName));
}

/// <summary>
/// One collection for every live test, so they share a single fixture and a single
/// cleanup pass, and never run in parallel against the same server.
/// </summary>
[CollectionDefinition(Name)]
public sealed class SqlServerCollection : ICollectionFixture<SqlServerFixture>
{
    public const string Name = "SQL Server";
}
