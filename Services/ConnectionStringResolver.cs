using Microsoft.Data.SqlClient;

namespace SqlSchemaDiff.Services;

/// <summary>
/// Resolves a connection string from the safest source available.
/// <para>
/// A password typed on the command line is visible to every other process on the
/// machine (<c>ps</c>, Task Manager, <c>/proc</c>), lands in shell history, and is
/// echoed by most CI runners. The file and environment forms exist so a password
/// never has to appear as an argument.
/// </para>
/// </summary>
public static class ConnectionStringResolver
{
    public const string EnvironmentPrefix = "env:";

    /// <summary>
    /// Reads the connection string for one side, in order of precedence:
    /// the option itself (with <c>env:NAME</c> indirection), then
    /// <c>&lt;option&gt;-file</c>, then the environment variable.
    /// Returns null when none is set.
    /// </summary>
    public static string? Resolve(
        Func<string[], string?> getOption,
        string[] optionNames,
        string fileOptionName,
        string environmentVariable)
    {
        var direct = getOption(optionNames);
        if(!string.IsNullOrWhiteSpace(direct))
        {
            if(!direct.StartsWith(EnvironmentPrefix, StringComparison.OrdinalIgnoreCase))
                return direct;

            var variableName = direct[EnvironmentPrefix.Length..].Trim();
            var fromVariable = Environment.GetEnvironmentVariable(variableName);
            if(string.IsNullOrWhiteSpace(fromVariable))
                throw new InvalidOperationException($"Environment variable '{variableName}' is not set or is empty.");
            return fromVariable;
        }

        var path = getOption(new[] { fileOptionName });
        if(!string.IsNullOrWhiteSpace(path))
        {
            if(!File.Exists(path))
                throw new FileNotFoundException($"Connection file not found: {path}");

            var fromFile = File.ReadAllText(path).Trim();
            if(string.IsNullOrWhiteSpace(fromFile))
                throw new InvalidOperationException($"Connection file is empty: {path}");
            return fromFile;
        }

        var fromEnvironment = Environment.GetEnvironmentVariable(environmentVariable);
        return string.IsNullOrWhiteSpace(fromEnvironment) ? null : fromEnvironment;
    }

    /// <summary>
    /// A connection string safe to print: the password is replaced with <c>***</c>.
    /// Falls back to a placeholder if the string cannot be parsed, so an unparseable
    /// value is never echoed verbatim.
    /// </summary>
    public static string Mask(string connectionString)
    {
        try
        {
            var builder = new SqlConnectionStringBuilder(connectionString);
            if(!string.IsNullOrEmpty(builder.Password))
                builder.Password = "***";
            return builder.ConnectionString;
        }
        catch
        {
            return "(unparseable connection string)";
        }
    }
}
