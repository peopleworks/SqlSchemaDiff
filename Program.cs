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
                _ => Fail($"Comando no soportado: {command}")
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
        var connectionString = options.GetRequired("--conn", "--connection");
        var outSql = options.Get("--out") ?? "schema.sql";
        var outJson = options.Get("--json");

        var extractor = new SqlServerSchemaExtractor();
        var snapshot = await extractor.ExtractAsync(connectionString, CancellationToken.None);
        var script = ScriptComposer.ComposeFullScript(snapshot);

        await File.WriteAllTextAsync(outSql, script);
        if(!string.IsNullOrWhiteSpace(outJson))
        {
            var json = JsonSerializer.Serialize(snapshot, JsonOptions);
            await File.WriteAllTextAsync(outJson, json);
        }

        PrintSnapshotSummary("Extract", snapshot);
        Console.WriteLine($"SQL generado en: {Path.GetFullPath(outSql)}");
        if(!string.IsNullOrWhiteSpace(outJson))
            Console.WriteLine($"Snapshot JSON en: {Path.GetFullPath(outJson)}");
        return 0;
    }

    private static async Task<int> RunDiffAsync(CliOptions options, string mode)
    {
        var sourceSnapshot = await ResolveSnapshotAsync(
            options,
            snapshotOption: "--source-snapshot",
            connOption: "--source-conn",
            fallbackConnOption: "--source-connection",
            sideLabel: "source");

        var targetSnapshot = await ResolveSnapshotAsync(
            options,
            snapshotOption: "--target-snapshot",
            connOption: "--target-conn",
            fallbackConnOption: "--target-connection",
            sideLabel: "target");

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
            Console.WriteLine($"Diff SQL generado en: {Path.GetFullPath(outPath)}");
        }
        else
        {
            var outPath = options.Get("--out");
            if(!string.IsNullOrWhiteSpace(outPath))
            {
                await File.WriteAllTextAsync(outPath, result.Script);
                Console.WriteLine($"Script de drift generado en: {Path.GetFullPath(outPath)}");
            }
        }

        Console.WriteLine(
            $"Resumen: added={result.Added}, changed={result.Changed}, removed={result.Removed}, skipped={result.Skipped}");

        if(mode == "drift")
            return result.HasChanges ? 2 : 0;
        return 0;
    }

    private static async Task<int> RunApplyAsync(CliOptions options)
    {
        var connectionString = options.GetRequired("--conn", "--connection");
        var scriptPath = options.GetRequired("--script");
        var dryRun = options.GetBool("--dry-run", defaultValue: false);
        var timeoutSeconds = options.GetInt("--timeout-seconds", 120);

        if(!File.Exists(scriptPath))
            return Fail($"No existe el script: {scriptPath}");

        var script = await File.ReadAllTextAsync(scriptPath);
        var executor = new SqlBatchExecutor();
        var batchCount = await executor.ExecuteAsync(
            connectionString,
            script,
            dryRun,
            timeoutSeconds,
            CancellationToken.None);

        if(dryRun)
            Console.WriteLine($"Dry-run OK. Lotes detectados: {batchCount}");
        else
            Console.WriteLine($"Script aplicado correctamente. Lotes ejecutados: {batchCount}");

        return 0;
    }

    private static async Task<int> RunSyncAsync(CliOptions options, bool forceApply)
    {
        var source = await ResolveSnapshotAsync(
            options,
            snapshotOption: "--source-snapshot",
            connOption: "--source-conn",
            fallbackConnOption: "--source-connection",
            sideLabel: "source");

        var target = await ResolveSnapshotAsync(
            options,
            snapshotOption: "--target-snapshot",
            connOption: "--target-conn",
            fallbackConnOption: "--target-connection",
            sideLabel: "target");

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

        Console.WriteLine($"Sync SQL generado en: {Path.GetFullPath(outPath)}");
        Console.WriteLine(
            $"Resumen: added={result.Added}, changed={result.Changed}, removed={result.Removed}, skipped={result.Skipped}");

        if(!apply)
            return 0;

        var targetConn = options.Get("--target-conn", "--target-connection");
        if(string.IsNullOrWhiteSpace(targetConn))
        {
            return Fail("Para aplicar cambios debe indicar --target-conn (o --target-connection).");
        }

        var executor = new SqlBatchExecutor();
        var batchCount = await executor.ExecuteAsync(
            targetConn,
            result.Script,
            dryRun,
            timeoutSeconds,
            CancellationToken.None);

        if(dryRun)
            Console.WriteLine($"Dry-run OK. Lotes detectados: {batchCount}");
        else
            Console.WriteLine($"Sync aplicado correctamente. Lotes ejecutados: {batchCount}");

        return 0;
    }

    private static async Task<int> RunCheckConnAsync(CliOptions options)
    {
        var verifier = new ConnectionVerifier();
        var timeoutSeconds = options.GetInt("--timeout-seconds", 15);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));

        var singleConn = options.Get("--conn", "--connection");
        if(!string.IsNullOrWhiteSpace(singleConn))
        {
            var result = await verifier.VerifyAsync(singleConn, cts.Token);
            PrintConnResult("conn", result);
            return 0;
        }

        var sourceConn = options.Get("--source-conn", "--source-connection");
        var targetConn = options.Get("--target-conn", "--target-connection");

        if(string.IsNullOrWhiteSpace(sourceConn) && string.IsNullOrWhiteSpace(targetConn))
            throw new InvalidOperationException(
                "Debe indicar --conn o al menos --source-conn/--target-conn para validar conexion.");

        if(!string.IsNullOrWhiteSpace(sourceConn))
        {
            var source = await verifier.VerifyAsync(sourceConn, cts.Token);
            PrintConnResult("source", source);
        }

        if(!string.IsNullOrWhiteSpace(targetConn))
        {
            var target = await verifier.VerifyAsync(targetConn, cts.Token);
            PrintConnResult("target", target);
        }

        return 0;
    }

    private static async Task<DatabaseSnapshot> ResolveSnapshotAsync(
        CliOptions options,
        string snapshotOption,
        string connOption,
        string fallbackConnOption,
        string sideLabel)
    {
        var snapshotPath = options.Get(snapshotOption);
        if(!string.IsNullOrWhiteSpace(snapshotPath))
        {
            if(!File.Exists(snapshotPath))
                throw new FileNotFoundException($"No se encontro snapshot {sideLabel}: {snapshotPath}");

            var json = await File.ReadAllTextAsync(snapshotPath);
            var snapshot = JsonSerializer.Deserialize<DatabaseSnapshot>(json, JsonOptions);
            if(snapshot is null)
                throw new InvalidOperationException($"Snapshot invalido en {snapshotPath}");
            return snapshot;
        }

        var connectionString = options.Get(connOption) ?? options.Get(fallbackConnOption);
        if(string.IsNullOrWhiteSpace(connectionString))
            throw new InvalidOperationException($"Debe indicar {snapshotOption} o {connOption}");

        var extractor = new SqlServerSchemaExtractor();
        return await extractor.ExtractAsync(connectionString, CancellationToken.None);
    }

    private static void PrintSnapshotSummary(string label, DatabaseSnapshot snapshot)
    {
        var tables = snapshot.Objects.Count(x => x.Type == DbObjectType.Table);
        var views = snapshot.Objects.Count(x => x.Type == DbObjectType.View);
        var procedures = snapshot.Objects.Count(x => x.Type == DbObjectType.StoredProcedure);
        var functions = snapshot.Objects.Count(x => x.Type == DbObjectType.Function);

        Console.WriteLine($"[{label}] Database [{snapshot.DatabaseName}]");
        Console.WriteLine($"Objects: tables={tables}, views={views}, procs={procedures}, funcs={functions}");
    }

    private static void PrintConnResult(string label, ConnectionProbeResult result)
    {
        Console.WriteLine($"[{label}] Connection OK");
        Console.WriteLine($"Server: {result.ServerName}");
        Console.WriteLine($"Database: {result.DatabaseName}");
        Console.WriteLine($"Login: {result.LoginName}");
        Console.WriteLine($"SQL Version: {result.ProductVersion}");
        Console.WriteLine($"Edition: {result.Edition}");
        Console.WriteLine();
    }

    private static bool IsHelp(string command) =>
        command is "help" or "--help" or "-h" or "/?";

    private static int Fail(string message)
    {
        Console.Error.WriteLine(message);
        return 1;
    }

    private static void PrintHelp()
    {
        Console.WriteLine(
            """
            SQLDiff CLI (.NET 9) - SQL Server schema diff

            Comandos:
              extract  --conn <connectionString> [--out schema.sql] [--json snapshot.json]
              diff     (--source-conn <cs> | --source-snapshot <json>)
                       (--target-conn <cs> | --target-snapshot <json>)
                       [--out diff.sql] [--include-drops] [--include-table-drops] [--allow-table-rebuild] [--add-only]
              apply    --conn <connectionString> --script <diff.sql> [--dry-run] [--timeout-seconds 120]
              sync     (--source-conn <cs> | --source-snapshot <json>)
                       (--target-conn <cs> | --target-snapshot <json>) [--out sync.diff.sql]
                       [--include-drops] [--include-table-drops] [--allow-table-rebuild] [--add-only]
                       [--apply] [--dry-run] [--timeout-seconds 120]
              deploy   Igual a sync, pero siempre aplica (diff + apply en un solo comando).
              check-conn (--conn <cs> | --source-conn <cs> [--target-conn <cs>]) [--timeout-seconds 15]
              drift    Igual a diff, pero retorna codigo 2 si hay diferencias.
                       Por defecto en drift: include-drops=true e include-table-drops=true.

            Ejemplos:
              SqlSchemaDiff extract --conn "Server=SQL1;Database=DB1;User Id=sa;Password=***;Encrypt=True;TrustServerCertificate=True" --out db1.sql --json db1.snapshot.json
              SqlSchemaDiff diff --source-conn "Server=SQL1;Database=DB1;..." --target-conn "Server=SQL2;Database=DB1;..." --out cambios.sql --include-drops
              SqlSchemaDiff diff --source-conn "Server=SQL1;Database=DB1;..." --target-conn "Server=SQL2;Database=DB1;..." --out add_only.sql --add-only
              SqlSchemaDiff apply --conn "Server=SQL2;Database=DB1;..." --script cambios.sql
              SqlSchemaDiff sync --source-conn "Server=SQL1;Database=DB1;..." --target-conn "Server=SQL2;Database=DB1;..." --out sync.sql --apply
              SqlSchemaDiff deploy --source-snapshot "source.snapshot.json" --target-conn "Server=SQL2;Database=DB1;..." --out deploy.sql --add-only
              SqlSchemaDiff check-conn --source-conn "Server=SQL1;Database=DB1;..." --target-conn "Server=SQL2;Database=DB1;..."
              SqlSchemaDiff drift --source-conn "Server=SQL1;Database=DB1;..." --target-conn "Server=SQL2;Database=DB1;..."
            """);
    }
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

    public string GetRequired(params string[] names)
    {
        var value = Get(names);
        if(string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"Parametro requerido faltante: {string.Join(" o ", names)}");
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
