using Microsoft.Data.SqlClient;

namespace SqlSchemaDiff.Services;

public sealed class ConnectionVerifier
{
    public async Task<ConnectionProbeResult> VerifyAsync(string connectionString, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
                           SELECT
                               @@SERVERNAME AS ServerName,
                               DB_NAME() AS DatabaseName,
                               SUSER_SNAME() AS LoginName,
                               CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)) AS ProductVersion,
                               CAST(SERVERPROPERTY('Edition') AS nvarchar(128)) AS Edition;
                           """;

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if(!await reader.ReadAsync(cancellationToken))
            throw new InvalidOperationException("No se pudo leer metadata del servidor.");

        return new ConnectionProbeResult(
            ServerName: reader.IsDBNull(0) ? "(null)" : reader.GetString(0),
            DatabaseName: reader.IsDBNull(1) ? "(null)" : reader.GetString(1),
            LoginName: reader.IsDBNull(2) ? "(null)" : reader.GetString(2),
            ProductVersion: reader.IsDBNull(3) ? "(null)" : reader.GetString(3),
            Edition: reader.IsDBNull(4) ? "(null)" : reader.GetString(4));
    }
}

public sealed record ConnectionProbeResult(
    string ServerName,
    string DatabaseName,
    string LoginName,
    string ProductVersion,
    string Edition);
