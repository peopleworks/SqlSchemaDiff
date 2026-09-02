namespace SqlSchemaDiff.Services;

/// <summary>
/// Rewrites the stored text of a programmable object (view, procedure, function,
/// trigger) so it can be re-applied to a database where the object already exists.
/// </summary>
public static class SqlModuleRewriter
{
    /// <summary>
    /// The object kinds SQL Server accepts after <c>CREATE OR ALTER</c>. Anything
    /// else — a CREATE TABLE that reached here by accident, say — is left alone:
    /// <c>CREATE OR ALTER TABLE</c> is a syntax error, and silently producing one
    /// is worse than leaving the text untouched.
    /// </summary>
    private static readonly string[] AlterableKinds = { "VIEW", "PROCEDURE", "PROC", "FUNCTION", "TRIGGER" };

    /// <summary>
    /// Turns the leading <c>CREATE</c> into <c>CREATE OR ALTER</c>.
    /// <para>
    /// The definition in <c>sys.sql_modules</c> is the whole batch as it was
    /// submitted, so a procedure written with a header comment is stored with that
    /// comment in front of the <c>CREATE</c>. Anchoring on the first character of
    /// the text therefore misses a very common shape, and the script goes out as a
    /// plain <c>CREATE</c> that fails with "There is already an object named ...".
    /// This skips whitespace and any run of line or block comments first.
    /// </para>
    /// <para>
    /// SQL Server stores <c>CREATE OR ALTER</c> back as <c>CREATE</c> (padded with
    /// spaces), so the rewritten object still compares equal to its source.
    /// </para>
    /// </summary>
    public static string ToCreateOrAlter(string definition)
    {
        if(string.IsNullOrWhiteSpace(definition))
            return definition;

        var index = SkipLeadingTrivia(definition, 0);
        if(!Matches(definition, index, "CREATE"))
            return definition;

        var afterCreate = index + "CREATE".Length;
        if(afterCreate >= definition.Length || !char.IsWhiteSpace(definition[afterCreate]))
            return definition;

        // Already a CREATE OR ALTER: leave it alone.
        var afterKeyword = SkipLeadingTrivia(definition, afterCreate);
        if(Matches(definition, afterKeyword, "OR"))
            return definition;

        if(!AlterableKinds.Any(kind => Matches(definition, afterKeyword, kind)))
            return definition;

        return definition[..afterCreate] + " OR ALTER" + definition[afterCreate..];
    }

    /// <summary>Advances past whitespace, <c>--</c> line comments and <c>/* */</c> block comments.</summary>
    private static int SkipLeadingTrivia(string text, int index)
    {
        while(index < text.Length)
        {
            if(char.IsWhiteSpace(text[index]))
            {
                index++;
                continue;
            }

            if(index + 1 < text.Length && text[index] == '-' && text[index + 1] == '-')
            {
                var newLine = text.IndexOf('\n', index);
                index = newLine < 0 ? text.Length : newLine + 1;
                continue;
            }

            if(index + 1 < text.Length && text[index] == '/' && text[index + 1] == '*')
            {
                var close = text.IndexOf("*/", index + 2, StringComparison.Ordinal);
                index = close < 0 ? text.Length : close + 2;
                continue;
            }

            break;
        }

        return index;
    }

    private static bool Matches(string text, int index, string keyword) =>
        index + keyword.Length <= text.Length &&
        string.Compare(text, index, keyword, 0, keyword.Length, StringComparison.OrdinalIgnoreCase) == 0;
}
