using System.Text;

namespace SqlSchemaDiff.Services;

/// <summary>
/// Splits a T-SQL script into batches on the <c>GO</c> separator.
/// <para>
/// <c>GO</c> is not T-SQL — it is a convention the client tools implement, so this
/// has to decide for itself where a batch ends. It only counts as a separator when
/// it stands alone on a line <b>and</b> that line is ordinary script text: a
/// <c>GO</c> inside a block comment, a string literal or a quoted identifier is
/// just characters. Matching lines with a regular expression instead splits a
/// header comment down the middle and leaves the halves unparseable
/// ("Missing end comment mark '*/'").
/// </para>
/// </summary>
public static class SqlBatchSplitter
{
    public static List<string> Split(string script)
    {
        var batches = new List<string>();
        if(string.IsNullOrEmpty(script))
            return batches;

        var text = script.Replace("\r\n", "\n");
        var current = new StringBuilder();

        var blockCommentDepth = 0;   // T-SQL block comments nest
        var inLineComment = false;
        var inString = false;        // '...'
        var inQuotedIdentifier = false;  // "..."
        var inBracketIdentifier = false; // [...]
        var atLineStart = true;

        var index = 0;
        while(index < text.Length)
        {
            var isOrdinaryText = blockCommentDepth == 0 && !inLineComment &&
                                 !inString && !inQuotedIdentifier && !inBracketIdentifier;

            if(atLineStart && isOrdinaryText && TryReadSeparator(text, index, out var lineEnd, out var repeatCount))
            {
                AddBatch(batches, current, repeatCount);
                index = lineEnd;
                atLineStart = true;
                continue;
            }

            var c = text[index];
            var next = index + 1 < text.Length ? text[index + 1] : '\0';

            if(c == '\n')
            {
                inLineComment = false;
                atLineStart = true;
                current.Append(c);
                index++;
                continue;
            }

            atLineStart = false;

            if(inLineComment)
            {
                current.Append(c);
                index++;
                continue;
            }

            if(blockCommentDepth > 0)
            {
                if(c == '/' && next == '*') { blockCommentDepth++; current.Append("/*"); index += 2; continue; }
                if(c == '*' && next == '/') { blockCommentDepth--; current.Append("*/"); index += 2; continue; }
                current.Append(c);
                index++;
                continue;
            }

            if(inString)
            {
                // '' is an escaped quote, not the end of the literal.
                if(c == '\'' && next == '\'') { current.Append("''"); index += 2; continue; }
                if(c == '\'') inString = false;
                current.Append(c);
                index++;
                continue;
            }

            if(inQuotedIdentifier)
            {
                if(c == '"' && next == '"') { current.Append("\"\""); index += 2; continue; }
                if(c == '"') inQuotedIdentifier = false;
                current.Append(c);
                index++;
                continue;
            }

            if(inBracketIdentifier)
            {
                if(c == ']' && next == ']') { current.Append("]]"); index += 2; continue; }
                if(c == ']') inBracketIdentifier = false;
                current.Append(c);
                index++;
                continue;
            }

            // Ordinary text: look for the start of anything that swallows a GO.
            if(c == '-' && next == '-') { inLineComment = true; current.Append("--"); index += 2; continue; }
            if(c == '/' && next == '*') { blockCommentDepth = 1; current.Append("/*"); index += 2; continue; }
            if(c == '\'') { inString = true; current.Append(c); index++; continue; }
            if(c == '"') { inQuotedIdentifier = true; current.Append(c); index++; continue; }
            if(c == '[') { inBracketIdentifier = true; current.Append(c); index++; continue; }

            current.Append(c);
            index++;
        }

        AddBatch(batches, current, 1);
        return batches;
    }

    /// <summary>
    /// Recognises a separator line at <paramref name="index"/>: optional whitespace,
    /// <c>GO</c>, an optional repeat count, an optional trailing line comment, and
    /// nothing else before the newline.
    /// </summary>
    private static bool TryReadSeparator(string text, int index, out int lineEnd, out int repeatCount)
    {
        repeatCount = 1;
        var newLine = text.IndexOf('\n', index);
        lineEnd = newLine < 0 ? text.Length : newLine + 1;

        var line = (newLine < 0 ? text[index..] : text[index..newLine]).Trim();
        if(line.Length < 2)
            return false;

        if(!(char.ToUpperInvariant(line[0]) == 'G' && char.ToUpperInvariant(line[1]) == 'O'))
            return false;

        var rest = line[2..].Trim();
        if(rest.Length == 0)
            return true;

        // A trailing comment is fine; anything else means this is not a separator
        // (it could be a column called GOAL, or `GOTO label`).
        var commentAt = rest.IndexOf("--", StringComparison.Ordinal);
        if(commentAt == 0)
            return true;

        var countText = (commentAt < 0 ? rest : rest[..commentAt]).Trim();
        if(countText.Length == 0)
            return true;

        // `GO 5` asks for the batch to run five times.
        if(!int.TryParse(countText, out var parsed) || parsed < 1)
            return false;

        repeatCount = parsed;
        return true;
    }

    private static void AddBatch(List<string> batches, StringBuilder sb, int repeatCount)
    {
        var content = sb.ToString().Trim();
        sb.Clear();
        if(content.Length == 0)
            return;

        for(var i = 0; i < repeatCount; i++)
            batches.Add(content);
    }
}
