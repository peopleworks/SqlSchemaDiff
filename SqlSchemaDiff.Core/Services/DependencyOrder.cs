using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.Services;

/// <summary>
/// Deterministic dependency ordering, shared by the differ (which orders the
/// statements of a migration) and the composer (which orders the objects of a
/// full script).
/// <para>
/// The sort is Kahn's algorithm over the edges declared in
/// <see cref="DbSchemaObject.Dependencies"/>, with two rules that make it usable
/// on real catalogs: a dependency on something outside the set is ignored (the
/// object already exists on the target, or was filtered out), and a cycle is
/// broken instead of throwing — the members it could not place are appended in
/// their tie-break order and reported, so the caller can warn about them.
/// </para>
/// <para>
/// Ties are broken by rank, then by the caller's sort key, then by the identity
/// key, so the same input always produces byte-identical output regardless of
/// the order the objects arrived in.
/// </para>
/// </summary>
public static class DependencyOrder
{
    /// <summary>
    /// Orders <paramref name="objects"/> so that every object follows the objects
    /// it depends on. <paramref name="rankSelector"/> ranks an object for tie-break
    /// purposes (e.g. functions before views); remaining ties fall back to
    /// schema.name and then to <see cref="DbSchemaObject.Key"/>.
    /// </summary>
    public static DependencyOrderResult<DbSchemaObject> Sort(
        IEnumerable<DbSchemaObject> objects,
        Func<DbSchemaObject, int> rankSelector) =>
        Sort(
            objects,
            x => x.Key,
            x => x.Dependencies,
            rankSelector,
            x => $"{x.Schema}.{x.Name}");

    /// <summary>
    /// The general form: any item that can produce an identity key, a list of the
    /// keys it depends on, and a rank. Duplicate keys keep their first occurrence,
    /// self-references are ignored.
    /// </summary>
    public static DependencyOrderResult<T> Sort<T>(
        IEnumerable<T> items,
        Func<T, string> keySelector,
        Func<T, IEnumerable<string>?> dependenciesSelector,
        Func<T, int> rankSelector,
        Func<T, string>? sortKeySelector = null)
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(dependenciesSelector);
        ArgumentNullException.ThrowIfNull(rankSelector);

        var nodes = new List<T>();
        var keys = new List<string>();
        var indexByKey = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach(var item in items)
        {
            var key = keySelector(item) ?? string.Empty;
            if(indexByKey.ContainsKey(key))
                continue;

            indexByKey[key] = nodes.Count;
            keys.Add(key);
            nodes.Add(item);
        }

        if(nodes.Count == 0)
            return DependencyOrderResult<T>.Empty;

        var ranks = nodes.Select(rankSelector).ToArray();
        var sortKeys = sortKeySelector is null
            ? keys.ToArray()
            : nodes.Select(x => sortKeySelector(x) ?? string.Empty).ToArray();

        var dependents = new List<int>[nodes.Count];
        var inDegree = new int[nodes.Count];
        for(var i = 0; i < nodes.Count; i++)
            dependents[i] = new List<int>();

        for(var i = 0; i < nodes.Count; i++)
        {
            var declared = dependenciesSelector(nodes[i]);
            if(declared is null)
                continue;

            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach(var dependency in declared)
            {
                if(string.IsNullOrWhiteSpace(dependency) || !seen.Add(dependency))
                    continue;

                // A dependency the caller did not hand us is not ours to order:
                // it already exists on the target, or the filter excluded it.
                if(!indexByKey.TryGetValue(dependency, out var dependencyIndex) || dependencyIndex == i)
                    continue;

                dependents[dependencyIndex].Add(i);
                inDegree[i]++;
            }
        }

        int Compare(int left, int right)
        {
            var byRank = ranks[left].CompareTo(ranks[right]);
            if(byRank != 0)
                return byRank;

            var bySortKey = string.Compare(sortKeys[left], sortKeys[right], StringComparison.OrdinalIgnoreCase);
            return bySortKey != 0
                ? bySortKey
                : string.Compare(keys[left], keys[right], StringComparison.OrdinalIgnoreCase);
        }

        // Always taking the smallest ready node — rather than the first one that
        // happened to become ready — is what makes the output independent of the
        // input order.
        var ready = new PriorityQueue<int, int>(Comparer<int>.Create(Compare));
        for(var i = 0; i < nodes.Count; i++)
        {
            if(inDegree[i] == 0)
                ready.Enqueue(i, i);
        }

        var ordered = new List<T>(nodes.Count);
        var placed = new bool[nodes.Count];
        while(ready.TryDequeue(out var index, out _))
        {
            ordered.Add(nodes[index]);
            placed[index] = true;

            foreach(var dependent in dependents[index])
            {
                if(--inDegree[dependent] == 0)
                    ready.Enqueue(dependent, dependent);
            }
        }

        if(ordered.Count == nodes.Count)
            return new DependencyOrderResult<T>(ordered, Array.Empty<T>());

        // What is left is a cycle plus everything downstream of it. Nothing can
        // order it correctly, so order it predictably and let the caller say so.
        var remaining = Enumerable.Range(0, nodes.Count)
            .Where(i => !placed[i])
            .OrderBy(i => i, Comparer<int>.Create(Compare))
            .Select(i => nodes[i])
            .ToList();

        ordered.AddRange(remaining);
        return new DependencyOrderResult<T>(ordered, remaining);
    }
}

/// <summary>
/// The outcome of a <see cref="DependencyOrder"/> sort: every item, in the order
/// it should be created, plus the items the sort could not place.
/// </summary>
public sealed class DependencyOrderResult<T>
{
    internal static readonly DependencyOrderResult<T> Empty = new(Array.Empty<T>(), Array.Empty<T>());

    internal DependencyOrderResult(IReadOnlyList<T> ordered, IReadOnlyList<T> cycleMembers)
    {
        Ordered = ordered;
        CycleMembers = cycleMembers;
    }

    /// <summary>Every input item, dependencies first. Cycle members come last.</summary>
    public IReadOnlyList<T> Ordered { get; }

    /// <summary>
    /// The tail of <see cref="Ordered"/> that a topological sort could not place:
    /// the cycle itself and anything that depends on it. Empty when the graph is
    /// acyclic.
    /// </summary>
    public IReadOnlyList<T> CycleMembers { get; }

    public bool HasCycle => CycleMembers.Count > 0;
}
