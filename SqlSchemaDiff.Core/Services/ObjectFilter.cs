using System.Text.RegularExpressions;
using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.Services;

/// <summary>
/// Restricts a comparison to part of a database.
/// <para>
/// A filter is applied to <b>both</b> snapshots before they are compared, so an
/// excluded object is invisible to the diff entirely: it is never created, never
/// altered, and never dropped. That is the important property — filtering only the
/// source would leave the object looking target-only, and a run with
/// <c>--include-drops</c> would then delete the very thing you asked to skip.
/// </para>
/// <para>
/// Each pattern is <c>[type:]glob</c>. The glob supports <c>*</c> and <c>?</c>, is
/// case-insensitive, and matches either the qualified <c>schema.name</c> or the bare
/// name, so <c>Sales.*</c>, <c>usp_Temp*</c> and <c>proc:*</c> all work. Separate
/// several with commas.
/// </para>
/// </summary>
public sealed class ObjectFilter
{
    private readonly List<Pattern> _include;
    private readonly List<Pattern> _exclude;

    private ObjectFilter(List<Pattern> include, List<Pattern> exclude)
    {
        _include = include;
        _exclude = exclude;
    }

    /// <summary>A filter that keeps everything.</summary>
    public static ObjectFilter None { get; } = new(new List<Pattern>(), new List<Pattern>());

    public bool IsEmpty => _include.Count == 0 && _exclude.Count == 0;

    public static ObjectFilter Parse(string? include, string? exclude)
    {
        var includePatterns = ParsePatterns(include);
        var excludePatterns = ParsePatterns(exclude);
        return includePatterns.Count == 0 && excludePatterns.Count == 0
            ? None
            : new ObjectFilter(includePatterns, excludePatterns);
    }

    public bool ShouldInclude(DbSchemaObject schemaObject)
    {
        if(_include.Count > 0 && !_include.Any(p => p.Matches(schemaObject)))
            return false;

        return !_exclude.Any(p => p.Matches(schemaObject));
    }

    /// <summary>Returns a snapshot containing only the objects this filter keeps.</summary>
    public DatabaseSnapshot Apply(DatabaseSnapshot snapshot)
    {
        if(IsEmpty)
            return snapshot;

        return new DatabaseSnapshot
        {
            DatabaseName = snapshot.DatabaseName,
            GeneratedAtUtc = snapshot.GeneratedAtUtc,
            Schemas = snapshot.Schemas,
            SchemaOwners = snapshot.SchemaOwners,
            Types = snapshot.Types,
            Objects = snapshot.Objects.Where(ShouldInclude).ToList()
        };
    }

    private static List<Pattern> ParsePatterns(string? raw)
    {
        var patterns = new List<Pattern>();
        if(string.IsNullOrWhiteSpace(raw))
            return patterns;

        foreach(var part in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            patterns.Add(Pattern.Parse(part));

        return patterns;
    }

    private sealed class Pattern
    {
        private readonly DbObjectType? _type;
        private readonly Regex? _glob;

        private Pattern(DbObjectType? type, Regex? glob)
        {
            _type = type;
            _glob = glob;
        }

        public static Pattern Parse(string text)
        {
            DbObjectType? type = null;
            var body = text;

            var separator = text.IndexOf(':');
            if(separator > 0)
            {
                var candidate = ToType(text[..separator]);
                if(candidate is not null)
                {
                    type = candidate;
                    body = text[(separator + 1)..].Trim();
                }
            }

            // "proc:" on its own means every procedure.
            var glob = body.Length == 0 || body == "*" ? null : ToRegex(body);
            return new Pattern(type, glob);
        }

        public bool Matches(DbSchemaObject schemaObject)
        {
            if(_type is not null && schemaObject.Type != _type)
                return false;

            if(_glob is null)
                return true;

            return _glob.IsMatch($"{schemaObject.Schema}.{schemaObject.Name}") ||
                   _glob.IsMatch(schemaObject.Name);
        }

        // "type:" means a user-defined TABLE type, which is the only kind of type
        // that is an object in its own right here. Alias types travel in
        // DatabaseSnapshot.Types as prerequisites, never as filterable objects, so
        // there is nothing for the prefix to collide with.
        private static DbObjectType? ToType(string text) => text.Trim().ToLowerInvariant() switch
        {
            "table" or "tables" => DbObjectType.Table,
            "view" or "views" => DbObjectType.View,
            "proc" or "procs" or "procedure" or "procedures" => DbObjectType.StoredProcedure,
            "func" or "funcs" or "function" or "functions" => DbObjectType.Function,
            "trigger" or "triggers" => DbObjectType.Trigger,
            "sequence" or "sequences" or "seq" or "seqs" => DbObjectType.Sequence,
            "tabletype" or "tabletypes" or "type" or "types" or "tvp" => DbObjectType.TableType,
            _ => null
        };

        private static Regex ToRegex(string glob)
        {
            var escaped = Regex.Escape(glob).Replace("\\*", ".*").Replace("\\?", ".");
            return new Regex($"^{escaped}$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        }
    }
}
