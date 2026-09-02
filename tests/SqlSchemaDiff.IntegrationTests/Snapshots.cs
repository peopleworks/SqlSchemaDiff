using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.IntegrationTests;

/// <summary>
/// Lookups over a <see cref="DatabaseSnapshot"/> that fail with the whole list of
/// candidates instead of a NullReferenceException. When a live test breaks, the
/// question is always "what did the extractor actually see?", and this answers it
/// in the assertion message.
/// </summary>
internal static class Snapshots
{
    public static DbSchemaObject Object(this DatabaseSnapshot snapshot, DbObjectType type, string schema, string name)
    {
        var match = snapshot.Objects.FirstOrDefault(x =>
            x.Type == type &&
            string.Equals(x.Schema, schema, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase));

        Assert.True(match is not null,
            $"{type} [{schema}].[{name}] is missing from the snapshot. Present: {Describe(snapshot)}");
        return match!;
    }

    public static TableModel Table(this DatabaseSnapshot snapshot, string schema, string name)
    {
        var table = snapshot.Object(DbObjectType.Table, schema, name).Table;
        Assert.True(table is not null, $"[{schema}].[{name}] was captured without a structured table model.");
        return table!;
    }

    public static ColumnModel Column(this TableModel table, string name)
    {
        var column = table.Columns.FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase));
        Assert.True(column is not null,
            $"[{table.Schema}].[{table.Name}] has no column [{name}]. Columns: {Join(table.Columns.Select(x => x.Name))}");
        return column!;
    }

    public static IndexModel Index(this TableModel table, string name)
    {
        var index = table.Indexes.FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase));
        Assert.True(index is not null,
            $"[{table.Schema}].[{table.Name}] has no index [{name}]. Indexes: {Join(table.Indexes.Select(x => x.Name))}");
        return index!;
    }

    public static KeyConstraintModel Key(this TableModel table, string name)
    {
        var key = table.KeyConstraints.FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase));
        Assert.True(key is not null,
            $"[{table.Schema}].[{table.Name}] has no key constraint [{name}]. Keys: {Join(table.KeyConstraints.Select(x => x.Name))}");
        return key!;
    }

    public static ForeignKeyModel ForeignKey(this TableModel table, string name)
    {
        var foreignKey = table.ForeignKeys.FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase));
        Assert.True(foreignKey is not null,
            $"[{table.Schema}].[{table.Name}] has no foreign key [{name}]. Keys: {Join(table.ForeignKeys.Select(x => x.Name))}");
        return foreignKey!;
    }

    public static CheckConstraintModel? FindCheck(this TableModel table, string name) =>
        table.CheckConstraints.FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase));

    public static CheckConstraintModel Check(this TableModel table, string name)
    {
        var check = table.FindCheck(name);
        Assert.True(check is not null,
            $"[{table.Schema}].[{table.Name}] has no check constraint [{name}]. Checks: {Join(table.CheckConstraints.Select(x => x.Name))}");
        return check!;
    }

    public static int Count(this DatabaseSnapshot snapshot, DbObjectType type) =>
        snapshot.Objects.Count(x => x.Type == type);

    public static string Describe(this DatabaseSnapshot snapshot) =>
        Join(snapshot.Objects.Select(x => $"{x.Type} {x.Identifier}").OrderBy(x => x, StringComparer.Ordinal));

    /// <summary>
    /// Asserts a diff is a no-op, and prints the script when it is not. A diff that
    /// "should be empty" but is not is only debuggable with the statements in hand.
    /// </summary>
    public static void AssertNoChanges(string what, DiffResult diff) =>
        Assert.True(
            !diff.HasChanges && diff.Skipped == 0,
            $"""
             {what}: expected no changes, got +{diff.Added} ~{diff.Changed} -{diff.Removed} (skipped {diff.Skipped}).
               added:   {Join(diff.AddedObjects)}
               changed: {Join(diff.ChangedObjects)}
               removed: {Join(diff.RemovedObjects)}
             {diff.Script}
             """);

    private static string Join(IEnumerable<string> values)
    {
        var list = values.ToList();
        return list.Count == 0 ? "(none)" : string.Join(", ", list);
    }
}
