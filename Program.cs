using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;

return await ProgramMain.RunAsync(args);

internal static class ProgramMain
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public static async Task<int> RunAsync(string[] args)
    {
        if(args.Length == 0)
        {
            PrintHelp();
            return 1;
        }

        if(IsHelp(args[0]))
        {
            PrintHelp();
            return 0;
        }

        if(IsVersion(args[0]))
        {
            Console.WriteLine(GetVersion());
            return 0;
        }

        var command = args[0].Trim().ToLowerInvariant();
        var options = CliOptions.Parse(args.Skip(1).ToArray());

        try
        {
            return command switch
            {
                "extract" => await RunExtractAsync(options),
                "diff" => await RunDiffAsync(options, mode: "diff"),
                "drift" => await RunDiffAsync(options, mode: "drift"),
                "apply" => await RunApplyAsync(options),
                "sync" => await RunSyncAsync(options, forceApply: false),
                "deploy" => await RunSyncAsync(options, forceApply: true),
                "delta-apply" => await RunSyncAsync(options, forceApply: true),
                "check-conn" => await RunCheckConnAsync(options),
                "check-connection" => await RunCheckConnAsync(options),
                _ => Fail($"Unknown command: {command}. Run 'sqldiff --help' for the command list.")
            };
        }
        catch(Exception ex)
        {
            Console.Error.WriteLine($"ERROR: {ex.Message}");
            return 1;
        }
    }

    private static async Task<int> RunExtractAsync(CliOptions options)
    {
        var connectionString = options.GetRequiredConnection(ConnectionSide.Single);
        var outSql = options.Get("--out") ?? "schema.sql";
        var outJson = options.Get("--json");

        var extractor = new SqlServerSchemaExtractor();
        var filter = options.GetObjectFilter();
        var snapshot = filter.Apply(await extractor.ExtractAsync(connectionString, CancellationToken.None));
        var script = ScriptComposer.ComposeFullScript(snapshot);

        await File.WriteAllTextAsync(outSql, script);
        if(!string.IsNullOrWhiteSpace(outJson))
        {
            var json = JsonSerializer.Serialize(snapshot, JsonOptions);
            await File.WriteAllTextAsync(outJson, json);
        }

        PrintSnapshotSummary("Extract", snapshot);
        PrintNotices(extractor);
        Console.WriteLine($"SQL written to: {Path.GetFullPath(outSql)}");
        if(!string.IsNullOrWhiteSpace(outJson))
            Console.WriteLine($"Snapshot JSON written to: {Path.GetFullPath(outJson)}");
        return 0;
    }

    private static async Task<int> RunDiffAsync(CliOptions options, string mode)
    {
        var filter = options.GetObjectFilter();
        var sourceSnapshot = filter.Apply(await ResolveSnapshotAsync(options, ConnectionSide.Source));
        var targetSnapshot = filter.Apply(await ResolveSnapshotAsync(options, ConnectionSide.Target));
        PrintFilterNotice(options, filter);

        var includeDrops = options.GetBool("--include-drops", defaultValue: mode == "drift");
        var includeTableDrops = options.GetBool("--include-table-drops", defaultValue: mode == "drift");
        var allowTableRebuild = options.GetBool("--allow-table-rebuild", defaultValue: false);
        var addOnly = options.GetBool("--add-only", defaultValue: false);

        var differ = new SchemaDiffer();
        var result = differ.Diff(
            sourceSnapshot,
            targetSnapshot,
            includeDrops: includeDrops,
            includeTableDrops: includeTableDrops,
            allowTableRebuild: allowTableRebuild,
            addOnly: addOnly);

        if(mode == "diff")
        {
            var outPath = options.Get("--out") ?? "diff.sql";
            await File.WriteAllTextAsync(outPath, result.Script);
            Console.WriteLine($"Diff SQL written to: {Path.GetFullPath(outPath)}");
        }
        else
        {
            var outPath = options.Get("--out");
            if(!string.IsNullOrWhiteSpace(outPath))
            {
                await File.WriteAllTextAsync(outPath, result.Script);
                Console.WriteLine($"Drift script written to: {Path.GetFullPath(outPath)}");
            }
        }

        Console.WriteLine(
            $"Summary: added={result.Added}, changed={result.Changed}, removed={result.Removed}, skipped={result.Skipped}");
        PrintDiffObjects(result);

        if(mode == "drift")
            return result.HasChanges ? 2 : 0;
        return 0;
    }

    private static async Task<int> RunApplyAsync(CliOptions options)
    {
        var connectionString = options.GetRequiredConnection(ConnectionSide.Single);
        var scriptPath = options.GetRequired("--script");
        var dryRun = options.GetBool("--dry-run", defaultValue: false);
        var timeoutSeconds = options.GetInt("--timeout-seconds", 120);

        if(!File.Exists(scriptPath))
            return Fail($"Script not found: {scriptPath}");

        var script = await File.ReadAllTextAsync(scriptPath);
        var execResult = await ExecuteAndLogAsync(options, "apply", connectionString, script, scriptPath, dryRun, timeoutSeconds);

        PrintExecutionResult(execResult, dryRun, "Script");
        return 0;
    }

    private static async Task<BatchExecutionResult> ExecuteAndLogAsync(
        CliOptions options,
        string command,
        string connectionString,
        string script,
        string? scriptPath,
        bool dryRun,
        int timeoutSeconds)
    {
        var useTransaction = !options.GetBool("--no-transaction", defaultValue: false);
        var logPath = options.Get("--log");
        var executor = new SqlBatchExecutor();
        var startedUtc = DateTimeOffset.UtcNow;

        try
        {
            var result = await executor.ExecuteAsync(
                connectionString, script, dryRun, timeoutSeconds, useTransaction, CancellationToken.None);

            if(!dryRun && !string.IsNullOrWhiteSpace(logPath))
                await AuditLogger.AppendAsync(
                    logPath, command, connectionString, scriptPath, result, "applied", null, startedUtc, CancellationToken.None);

            return result;
        }
        catch(Exception ex)
        {
            if(!dryRun && !string.IsNullOrWhiteSpace(logPath))
                await AuditLogger.AppendAsync(
                    logPath, command, connectionString, scriptPath, null,
                    useTransaction ? "rolled-back" : "failed", ex.Message, startedUtc, CancellationToken.None);
            throw;
        }
    }

    private static void PrintExecutionResult(BatchExecutionResult result, bool dryRun, string label)
    {
        if(dryRun)
        {
            Console.WriteLine($"Dry run OK. Batches detected: {result.BatchCount}");
            return;
        }

        var mode = result.Transactional ? " (transactional)" : string.Empty;
        Console.WriteLine($"{label} applied successfully. Batches executed: {result.Executed}{mode}");
    }

    private static async Task<int> RunSyncAsync(CliOptions options, bool forceApply)
    {
        var filter = options.GetObjectFilter();
        var source = filter.Apply(await ResolveSnapshotAsync(options, ConnectionSide.Source));
        var target = filter.Apply(await ResolveSnapshotAsync(options, ConnectionSide.Target));
        PrintFilterNotice(options, filter);

        var includeDrops = options.GetBool("--include-drops", defaultValue: false);
        var includeTableDrops = options.GetBool("--include-table-drops", defaultValue: false);
        var allowTableRebuild = options.GetBool("--allow-table-rebuild", defaultValue: false);
        var addOnly = options.GetBool("--add-only", defaultValue: false);
        var apply = forceApply || options.GetBool("--apply", defaultValue: false);
        var dryRun = options.GetBool("--dry-run", defaultValue: false);
        var timeoutSeconds = options.GetInt("--timeout-seconds", 120);
        var outPath = options.Get("--out") ?? "sync.diff.sql";

        var differ = new SchemaDiffer();
        var result = differ.Diff(source, target, includeDrops, includeTableDrops, allowTableRebuild, addOnly);
        await File.WriteAllTextAsync(outPath, result.Script);

        Console.WriteLine($"Sync SQL written to: {Path.GetFullPath(outPath)}");
        Console.WriteLine(
            $"Summary: added={result.Added}, changed={result.Changed}, removed={result.Removed}, skipped={result.Skipped}");
        PrintDiffObjects(result);

        if(!apply)
            return 0;

        var targetConn = options.GetConnection(ConnectionSide.Target);
        if(string.IsNullOrWhiteSpace(targetConn))
            return Fail("Applying changes needs --target-conn (or --target-conn-file / SQLDIFF_TARGET_CONN).");

        var execResult = await ExecuteAndLogAsync(
            options, forceApply ? "deploy" : "sync", targetConn, result.Script, null, dryRun, timeoutSeconds);

        PrintExecutionResult(execResult, dryRun, "Sync");
        return 0;
    }

    private static async Task<int> RunCheckConnAsync(CliOptions options)
    {
        var verifier = new ConnectionVerifier();
        var timeoutSeconds = options.GetInt("--timeout-seconds", 15);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));

        var singleConn = options.GetConnection(ConnectionSide.Single);
        if(!string.IsNullOrWhiteSpace(singleConn))
        {
            PrintConnResult("conn", await verifier.VerifyAsync(singleConn, cts.Token));
            return 0;
        }

        var sourceConn = options.GetConnection(ConnectionSide.Source);
        var targetConn = options.GetConnection(ConnectionSide.Target);

        if(string.IsNullOrWhiteSpace(sourceConn) && string.IsNullOrWhiteSpace(targetConn))
            throw new InvalidOperationException(
                "Specify --conn, or at least one of --source-conn / --target-conn.");

        if(!string.IsNullOrWhiteSpace(sourceConn))
            PrintConnResult("source", await verifier.VerifyAsync(sourceConn, cts.Token));

        if(!string.IsNullOrWhiteSpace(targetConn))
            PrintConnResult("target", await verifier.VerifyAsync(targetConn, cts.Token));

        return 0;
    }

    private static async Task<DatabaseSnapshot> ResolveSnapshotAsync(CliOptions options, ConnectionSide side)
    {
        var snapshotOption = side == ConnectionSide.Source ? "--source-snapshot" : "--target-snapshot";
        var label = side == ConnectionSide.Source ? "source" : "target";

        var snapshotPath = options.Get(snapshotOption);
        if(!string.IsNullOrWhiteSpace(snapshotPath))
        {
            if(!File.Exists(snapshotPath))
                throw new FileNotFoundException($"Snapshot for {label} not found: {snapshotPath}");

            var json = await File.ReadAllTextAsync(snapshotPath);
            var snapshot = JsonSerializer.Deserialize<DatabaseSnapshot>(json, JsonOptions);
            if(snapshot is null)
                throw new InvalidOperationException($"Invalid snapshot: {snapshotPath}");
            return snapshot;
        }

        var connectionString = options.GetConnection(side);
        if(string.IsNullOrWhiteSpace(connectionString))
            throw new InvalidOperationException($"Specify {snapshotOption} or --{label}-conn.");

        var extractor = new SqlServerSchemaExtractor();
        var result = await extractor.ExtractAsync(connectionString, CancellationToken.None);
        PrintNotices(extractor, label);
        return result;
    }

    /// <summary>
    /// Says out loud that the comparison was narrowed. A filtered run that looks
    /// like a full one is how someone concludes two databases match when half the
    /// objects were never looked at.
    /// </summary>
    private static void PrintFilterNotice(CliOptions options, ObjectFilter filter)
    {
        if(filter.IsEmpty)
            return;

        var parts = new List<string>();
        var include = options.Get("--include");
        var exclude = options.Get("--exclude");
        if(!string.IsNullOrWhiteSpace(include))
            parts.Add($"include={include}");
        if(!string.IsNullOrWhiteSpace(exclude))
            parts.Add($"exclude={exclude}");

        Console.WriteLine($"Filtered comparison ({string.Join(", ", parts)}); objects outside the filter were not compared.");
    }

    private static void PrintNotices(SqlServerSchemaExtractor extractor, string? label = null)
    {
        if(extractor.Notices.Count == 0)
            return;

        var prefix = label is null ? string.Empty : $"[{label}] ";
        foreach(var notice in extractor.Notices)
            Console.WriteLine($"  {prefix}NOTE: {notice}");
    }

    private static void PrintDiffObjects(DiffResult result)
    {
        PrintObjectList("Added", result.AddedObjects);
        PrintObjectList("Changed", result.ChangedObjects);
        PrintObjectList("Removed", result.RemovedObjects);
    }

    private static void PrintObjectList(string label, List<string> items)
    {
        if(items.Count == 0)
            return;
        Console.WriteLine($"  {label} ({items.Count}): {string.Join(", ", items)}");
    }

    private static void PrintSnapshotSummary(string label, DatabaseSnapshot snapshot)
    {
        var tables = snapshot.Objects.Count(x => x.Type == DbObjectType.Table);
        var views = snapshot.Objects.Count(x => x.Type == DbObjectType.View);
        var procedures = snapshot.Objects.Count(x => x.Type == DbObjectType.StoredProcedure);
        var functions = snapshot.Objects.Count(x => x.Type == DbObjectType.Function);

        Console.WriteLine($"[{label}] Database [{snapshot.DatabaseName}]");
        Console.WriteLine($"Objects: tables={tables}, views={views}, procs={procedures}, funcs={functions}");
        if(snapshot.Schemas.Count > 0)
            Console.WriteLine($"Schemas: {string.Join(", ", snapshot.Schemas)}");
        if(snapshot.Types.Count > 0)
            Console.WriteLine($"Alias types: {string.Join(", ", snapshot.Types.Select(x => $"{x.Schema}.{x.Name}"))}");
    }

    private static void PrintConnResult(string label, ConnectionProbeResult result)
    {
        Console.WriteLine($"[{label}] Connection OK");
        Console.WriteLine($"Server: {result.ServerName}");
        Console.WriteLine($"Database: {result.DatabaseName}");
        Console.WriteLine($"Login: {result.LoginName}");
        Console.WriteLine($"SQL version: {result.ProductVersion}");
        Console.WriteLine($"Edition: {result.Edition}");
        Console.WriteLine();
    }

    private static bool IsHelp(string command) =>
        command is "help" or "--help" or "-h" or "-?" or "/?";

    private static bool IsVersion(string command) =>
        command is "version" or "--version" or "-v";

    private static string GetVersion() =>
        typeof(ProgramMain).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            .Split('+')[0]
        ?? "unknown";

    private static int Fail(string message)
    {
        Console.Error.WriteLine(message);
        return 1;
    }

    private static void PrintHelp()
    {
        Console.WriteLine(
            $"""
            SQLDiff {GetVersion()} - SQL Server schema diff and sync (.NET 9)

            Usage: sqldiff <command> [options]

            Commands:
              extract     Script a database and optionally save a JSON snapshot.
                          --conn <cs> [--out schema.sql] [--json snapshot.json]

              diff        Compare source against target and write the migration SQL.
                          (--source-conn <cs> | --source-snapshot <json>)
                          (--target-conn <cs> | --target-snapshot <json>)
                          [--out diff.sql] [--include-drops] [--include-table-drops]
                          [--allow-table-rebuild] [--add-only]

              sync        Same as diff, and applies it when --apply is given.
                          ... [--apply] [--dry-run] [--timeout-seconds 120]
                              [--no-transaction] [--log apply.log]

              deploy      diff + apply in one step (always applies).
                          delta-apply is an alias.

              apply       Run an existing script against a database.
                          --conn <cs> --script <diff.sql> [--dry-run]
                          [--timeout-seconds 120] [--no-transaction] [--log apply.log]

              drift       Same as diff, but exits with code 2 when anything differs.
                          Defaults --include-drops and --include-table-drops to true.

              check-conn  Verify connectivity and print server metadata.
                          (--conn <cs> | --source-conn <cs> [--target-conn <cs>])
                          [--timeout-seconds 15]

              version     Print the version. --version and -v also work.

            Narrowing the comparison (all commands except apply and check-conn):
              --include <patterns>       Compare only what matches. Comma-separated.
              --exclude <patterns>       Skip what matches, applied after --include.
              A pattern is [type:]glob, where type is table/view/proc/func and glob
              takes * and ? and matches either schema.name or the bare name:
                --include "Sales.*"              only the Sales schema
                --exclude "proc:usp_Temp*,dbo.Audit*"
                --exclude "view:"                every view
              Filters apply to both sides, so a skipped object is never created,
              altered or dropped.

            Connection strings (keep passwords off the command line):
              --conn "<cs>"              Literal value. Use env:NAME to read a variable.
              --conn-file <path>         Read the connection string from a file.
              SQLDIFF_CONN               Used when no option is given.
              The same three forms exist for both sides: --source-conn / --source-conn-file /
              SQLDIFF_SOURCE_CONN and --target-conn / --target-conn-file / SQLDIFF_TARGET_CONN.
              Prefer Integrated Security on Windows; a password passed as an argument is
              visible to other processes and is recorded in shell history.

            Safe apply:
              apply, sync and deploy run inside one transaction by default: if any batch
              fails everything rolls back and the target is left untouched. Use
              --no-transaction to opt out, and --log <file> to append an audit record.

            Exit codes:
              0 success   1 error   2 drift detected (drift command)

            Examples:
              sqldiff check-conn --conn "Server=SQL1;Database=DbA;Integrated Security=True;TrustServerCertificate=True"
              sqldiff extract --conn env:SQLDIFF_CONN --out db.sql --json db.snapshot.json
              sqldiff diff --source-conn "..." --target-conn "..." --out changes.sql --include-drops
              sqldiff deploy --source-snapshot source.snapshot.json --target-conn-file target.conn --add-only
              sqldiff drift --source-conn "..." --target-conn "..."
            """);
    }
}

internal enum ConnectionSide
{
    Single,
    Source,
    Target
}

internal sealed class CliOptions
{
    private readonly Dictionary<string, string> _values;

    private CliOptions(Dictionary<string, string> values)
    {
        _values = values;
    }

    public static CliOptions Parse(string[] args)
    {
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for(var i = 0; i < args.Length; i++)
        {
            var token = args[i];
            if(!token.StartsWith("--", StringComparison.Ordinal))
                continue;

            var separatorIndex = token.IndexOf('=');
            if(separatorIndex > 2)
            {
                var key = token[..separatorIndex];
                var value = token[(separatorIndex + 1)..];
                values[key] = value;
                continue;
            }

            if(i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal))
            {
                values[token] = args[i + 1];
                i++;
                continue;
            }

            values[token] = "true";
        }

        return new CliOptions(values);
    }

    public string? Get(params string[] names)
    {
        foreach(var name in names)
        {
            if(_values.TryGetValue(name, out var value))
                return value;
        }

        return null;
    }

    /// <summary>Resolves one side's connection string from option, file or environment.</summary>
    public string? GetConnection(ConnectionSide side)
    {
        var (optionNames, fileOption, environmentVariable) = side switch
        {
            ConnectionSide.Source =>
                (new[] { "--source-conn", "--source-connection" }, "--source-conn-file", "SQLDIFF_SOURCE_CONN"),
            ConnectionSide.Target =>
                (new[] { "--target-conn", "--target-connection" }, "--target-conn-file", "SQLDIFF_TARGET_CONN"),
            _ =>
                (new[] { "--conn", "--connection" }, "--conn-file", "SQLDIFF_CONN")
        };

        return ConnectionStringResolver.Resolve(Get, optionNames, fileOption, environmentVariable);
    }

    public string GetRequiredConnection(ConnectionSide side)
    {
        var value = GetConnection(side);
        if(!string.IsNullOrWhiteSpace(value))
            return value;

        var hint = side switch
        {
            ConnectionSide.Source => "--source-conn, --source-conn-file or SQLDIFF_SOURCE_CONN",
            ConnectionSide.Target => "--target-conn, --target-conn-file or SQLDIFF_TARGET_CONN",
            _ => "--conn, --conn-file or SQLDIFF_CONN"
        };
        throw new InvalidOperationException($"Missing connection string. Provide one of: {hint}.");
    }

    public ObjectFilter GetObjectFilter() => ObjectFilter.Parse(Get("--include"), Get("--exclude"));

    public string GetRequired(params string[] names)
    {
        var value = Get(names);
        if(string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"Missing required option: {string.Join(" or ", names)}");
        return value;
    }

    public bool GetBool(string name, bool defaultValue)
    {
        if(!_values.TryGetValue(name, out var rawValue))
            return defaultValue;

        if(bool.TryParse(rawValue, out var parsed))
            return parsed;

        return string.Equals(rawValue, "1", StringComparison.Ordinal) ||
               string.Equals(rawValue, "yes", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(rawValue, "y", StringComparison.OrdinalIgnoreCase);
    }

    public int GetInt(string name, int defaultValue)
    {
        if(!_values.TryGetValue(name, out var rawValue))
            return defaultValue;
        return int.TryParse(rawValue, out var parsed) ? parsed : defaultValue;
    }
}
