using System.Text.RegularExpressions;

namespace SqlSchemaDiff.Services;

public static class SchemaTextNormalizer
{
    public static string Normalize(string input)
    {
        if(string.IsNullOrWhiteSpace(input))
            return string.Empty;

        var unifiedNewLines = input.Replace("\r\n", "\n");
        var collapsedWhitespace = Regex.Replace(unifiedNewLines, @"\s+", " ");
        return collapsedWhitespace.Trim().ToUpperInvariant();
    }
}
