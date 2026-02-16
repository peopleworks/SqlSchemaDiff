using System.Text;
using System.Text.RegularExpressions;
using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.Services;

public static class ScriptComposer
{
    public static string ComposeFullScript(DatabaseSnapshot snapshot)
    {
        var orderedObjects = snapshot.Objects
            .OrderBy(GetCreateOrder)
            .ThenBy(x => x.Schema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var sb = new StringBuilder();
        sb.AppendLine($"-- Snapshot database: [{snapshot.DatabaseName}]");
        sb.AppendLine($"-- Generated (UTC): {snapshot.GeneratedAtUtc:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine();

        foreach(var schemaObject in orderedObjects)
        {
            sb.AppendLine($"-- {schemaObject.Type} {schemaObject.Identifier}");
            sb.AppendLine(EnsureTrailingGo(schemaObject.Definition));
            sb.AppendLine();
        }

        return sb.ToString();
    }

    private static int GetCreateOrder(DbSchemaObject schemaObject) => schemaObject.Type switch
    {
        DbObjectType.Table => 0,
        DbObjectType.Function => 1,
        DbObjectType.View => 2,
        DbObjectType.StoredProcedure => 3,
        _ => 99
    };

    private static string EnsureTrailingGo(string definition)
    {
        var trimmed = definition.TrimEnd();
        if(Regex.IsMatch(trimmed, @"(^|\r?\n)\s*GO\s*$", RegexOptions.IgnoreCase))
            return trimmed;

        return $"{trimmed}{Environment.NewLine}GO";
    }
}
