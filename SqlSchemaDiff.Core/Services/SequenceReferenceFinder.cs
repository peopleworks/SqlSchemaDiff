using System.Text.RegularExpressions;
using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.Services;

/// <summary>
/// Finds the sequences a column default draws from.
/// <para>
/// A column whose default is <c>NEXT VALUE FOR dbo.OrderNumbers</c> cannot be
/// created before that sequence exists, but nothing in <c>sys.foreign_keys</c>
/// says so — the edge only exists inside the default's expression text. Reading it
/// out of the text is also what lets the edge survive a JSON round trip: a
/// snapshot loaded from disk has no catalog to ask.
/// </para>
/// </summary>
public static class SequenceReferenceFinder
{
    // SQL Server stores the default as (NEXT VALUE FOR [dbo].[OrderNumbers]) when
    // it was written qualified, and as (NEXT VALUE FOR [PlainSeq]) when it was not,
    // so both the bracketed and the bare form have to parse. Up to three parts are
    // accepted (db.schema.name) and the last two are used.
    private const string Part = @"(?:\[[^\]]+\]|[A-Za-z_@#][\w@#$]*)";

    private static readonly Regex NextValueFor = new(
        $@"NEXT\s+VALUE\s+FOR\s+(?<a>{Part})\s*(?:\.\s*(?<b>{Part})\s*(?:\.\s*(?<c>{Part}))?)?",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <summary>
    /// The sequence references in an expression, as (schema, name) pairs. The schema
    /// is null when the reference was written unqualified.
    /// </summary>
    public static List<(string? Schema, string Name)> Find(string? expression)
    {
        var references = new List<(string? Schema, string Name)>();
        if(string.IsNullOrWhiteSpace(expression))
            return references;

        foreach(Match match in NextValueFor.Matches(expression))
        {
            var parts = new[] { match.Groups["a"], match.Groups["b"], match.Groups["c"] }
                .Where(g => g.Success)
                .Select(g => Unquote(g.Value))
                .ToList();

            references.Add(parts.Count switch
            {
                1 => (null, parts[0]),
                _ => (parts[^2], parts[^1])
            });
        }

        return references;
    }

    /// <summary>
    /// Resolves the references in <paramref name="expression"/> against the
    /// sequences that actually exist and returns their
    /// <see cref="DbSchemaObject.Key"/> values.
    /// <para>
    /// An unqualified reference is resolved the way SQL Server resolved it when the
    /// default was created: prefer a sequence in <paramref name="defaultSchema"/>
    /// (the referencing table's own schema), then <c>dbo</c>, then a unique match by
    /// name. A reference that matches nothing is dropped rather than guessed at — a
    /// dependency on an object that is not in the snapshot would only be ignored by
    /// the topological sort anyway.
    /// </para>
    /// </summary>
    public static List<string> FindDependencyKeys(
        string? expression,
        IReadOnlyCollection<SequenceModel> sequences,
        string defaultSchema)
    {
        var keys = new List<string>();
        if(sequences.Count == 0)
            return keys;

        foreach(var (schema, name) in Find(expression))
        {
            var match = Resolve(sequences, schema, name, defaultSchema);
            if(match is null)
                continue;

            var key = DbSchemaObject.BuildKey(DbObjectType.Sequence, match.Schema, match.Name);
            if(!keys.Contains(key, StringComparer.OrdinalIgnoreCase))
                keys.Add(key);
        }

        return keys;
    }

    private static SequenceModel? Resolve(
        IEnumerable<SequenceModel> sequences,
        string? schema,
        string name,
        string defaultSchema)
    {
        var byName = sequences
            .Where(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase))
            .ToList();

        if(byName.Count == 0)
            return null;

        if(schema is not null)
            return byName.FirstOrDefault(x => string.Equals(x.Schema, schema, StringComparison.OrdinalIgnoreCase));

        return byName.FirstOrDefault(x => string.Equals(x.Schema, defaultSchema, StringComparison.OrdinalIgnoreCase))
               ?? byName.FirstOrDefault(x => string.Equals(x.Schema, "dbo", StringComparison.OrdinalIgnoreCase))
               ?? (byName.Count == 1 ? byName[0] : null);
    }

    private static string Unquote(string part) =>
        part.Length >= 2 && part[0] == '[' && part[^1] == ']'
            ? part[1..^1].Replace("]]", "]")
            : part;
}
