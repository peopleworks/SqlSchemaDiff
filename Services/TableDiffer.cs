using System.Text;
using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.Services;

/// <summary>
/// Produces incremental ALTER TABLE statements to make a target table match a
/// source table, comparing column-by-column instead of rebuilding the table.
/// This preserves existing data. Risky operations are annotated with warnings
/// and destructive drops are gated behind <c>includeDrops</c>.
/// </summary>
public sealed class TableDiffer
{
    public sealed class TableAlterResult
    {
        public string Script { get; init; } = string.Empty;
        public int ChangeCount { get; init; }
        public int WarningCount { get; init; }
        public bool HasChanges => ChangeCount > 0;
    }

    public TableAlterResult Diff(TableModel source, TableModel target, bool includeDrops)
    {
        var pre = new List<string>();       // drops of constraints/indexes that change
        var columnAdds = new List<string>();
        var columnAlters = new List<string>();
        var columnDrops = new List<string>();
        var post = new List<string>();      // adds of constraints/indexes
        var warnings = new List<string>();
        var changeCount = 0;

        var sourceCols = ToDict(source.Columns, x => x.Name);
        var targetCols = ToDict(target.Columns, x => x.Name);

        // ---- Columns ----
        foreach(var col in source.Columns)
        {
            if(!targetCols.TryGetValue(col.Name, out var targetCol))
            {
                columnAdds.Add($"ALTER TABLE {SqlRender.TableIdentifier(source)} ADD {SqlRender.BuildColumnDefinition(col)};");
                changeCount++;

                if(!col.IsNullable && string.IsNullOrWhiteSpace(col.DefaultDefinition) && !col.IsIdentity && !col.IsComputed)
                {
                    warnings.Add($"-- WARNING: new column [{col.Name}] is NOT NULL without a default; " +
                                 "ADD will fail if the table already has rows. Provide a default or backfill first.");
                }
                continue;
            }

            if(ColumnsEqual(col, targetCol))
                continue;

            changeCount++;
            AppendColumnAlter(source, col, targetCol, columnAlters, warnings);
        }

        // ---- Columns removed on source ----
        foreach(var col in target.Columns)
        {
            if(sourceCols.ContainsKey(col.Name))
                continue;

            if(!includeDrops)
            {
                warnings.Add($"-- WARNING: column [{col.Name}] exists only on target and was not dropped. Use --include-drops to remove it.");
                continue;
            }

            if(!string.IsNullOrWhiteSpace(col.DefaultName))
                columnDrops.Add($"ALTER TABLE {SqlRender.TableIdentifier(source)} DROP CONSTRAINT {SqlRender.Quote(col.DefaultName)};");
            columnDrops.Add($"ALTER TABLE {SqlRender.TableIdentifier(source)} DROP COLUMN {SqlRender.Quote(col.Name)};");
            warnings.Add($"-- WARNING: dropping column [{col.Name}] on target permanently deletes its data.");
            changeCount++;
        }

        // ---- Key constraints (PK / UQ) ----
        DiffNamed(
            source.KeyConstraints, target.KeyConstraints, x => x.Name,
            (s, t) => KeyConstraintsEqual(s, t),
            add: s => post.Add(SqlRender.BuildKeyConstraintAdd(source, s)),
            drop: t => pre.Add($"ALTER TABLE {SqlRender.TableIdentifier(source)} DROP CONSTRAINT {SqlRender.Quote(t.Name)};"),
            includeDrops, ref changeCount, warnings, "key constraint");

        // ---- Check constraints ----
        DiffNamed(
            source.CheckConstraints, target.CheckConstraints, x => x.Name,
            (s, t) => NormalizedEqual(s.Definition, t.Definition) && s.IsNotTrusted == t.IsNotTrusted,
            add: s => post.Add(SqlRender.BuildCheckConstraintAdd(source, s)),
            drop: t => pre.Add($"ALTER TABLE {SqlRender.TableIdentifier(source)} DROP CONSTRAINT {SqlRender.Quote(t.Name)};"),
            includeDrops, ref changeCount, warnings, "check constraint");

        // ---- Foreign keys ----
        DiffNamed(
            source.ForeignKeys, target.ForeignKeys, x => x.Name,
            (s, t) => ForeignKeysEqual(s, t),
            add: s => post.Add(SqlRender.BuildForeignKeyAdd(source, s)),
            drop: t => pre.Add($"ALTER TABLE {SqlRender.TableIdentifier(source)} DROP CONSTRAINT {SqlRender.Quote(t.Name)};"),
            includeDrops, ref changeCount, warnings, "foreign key");

        // ---- Indexes ----
        DiffNamed(
            source.Indexes, target.Indexes, x => x.Name,
            (s, t) => IndexesEqual(s, t),
            add: s => post.Add(SqlRender.BuildIndexCreate(source, s)),
            drop: t => pre.Add($"DROP INDEX {SqlRender.Quote(t.Name)} ON {SqlRender.TableIdentifier(source)};"),
            includeDrops, ref changeCount, warnings, "index");

        var script = Compose(source, pre, columnAdds, columnAlters, columnDrops, post, warnings);
        return new TableAlterResult
        {
            Script = script,
            ChangeCount = changeCount,
            WarningCount = warnings.Count
        };
    }

    private static void AppendColumnAlter(
        TableModel source, ColumnModel src, ColumnModel tgt,
        List<string> columnAlters, List<string> warnings)
    {
        var tableId = SqlRender.TableIdentifier(source);

        // Identity cannot be changed with ALTER COLUMN.
        if(src.IsIdentity != tgt.IsIdentity ||
           !string.Equals(src.IdentitySeed, tgt.IdentitySeed, StringComparison.OrdinalIgnoreCase) ||
           !string.Equals(src.IdentityIncrement, tgt.IdentityIncrement, StringComparison.OrdinalIgnoreCase))
        {
            warnings.Add($"-- WARNING: column [{src.Name}] identity property differs and cannot be changed with ALTER COLUMN. Manual table rebuild required.");
        }

        // Computed columns must be dropped and re-added.
        if(src.IsComputed || tgt.IsComputed)
        {
            if(tgt.IsComputed && !string.IsNullOrWhiteSpace(tgt.DefaultName))
                columnAlters.Add($"ALTER TABLE {tableId} DROP CONSTRAINT {SqlRender.Quote(tgt.DefaultName)};");
            columnAlters.Add($"ALTER TABLE {tableId} DROP COLUMN {SqlRender.Quote(src.Name)};");
            columnAlters.Add($"ALTER TABLE {tableId} ADD {SqlRender.BuildColumnDefinition(src)};");
            return;
        }

        // Type / nullability / collation change.
        var srcType = SqlRender.BuildType(src);
        var tgtType = SqlRender.BuildType(tgt);
        var typeChanged = !string.Equals(srcType, tgtType, StringComparison.OrdinalIgnoreCase);
        var collationChanged = !string.Equals(src.CollationName, tgt.CollationName, StringComparison.OrdinalIgnoreCase);
        var nullabilityChanged = src.IsNullable != tgt.IsNullable;

        if(typeChanged || collationChanged || nullabilityChanged)
        {
            var sb = new StringBuilder();
            sb.Append($"ALTER TABLE {tableId} ALTER COLUMN {SqlRender.Quote(src.Name)} {srcType}");
            if(!string.IsNullOrWhiteSpace(src.CollationName))
                sb.Append($" COLLATE {src.CollationName}");
            sb.Append(src.IsNullable ? " NULL" : " NOT NULL");
            sb.Append(';');
            columnAlters.Add(sb.ToString());

            if(nullabilityChanged && !src.IsNullable)
                warnings.Add($"-- WARNING: column [{src.Name}] becomes NOT NULL; ALTER fails if it contains NULLs. Backfill first.");
            if(typeChanged && IsNarrowing(src, tgt))
                warnings.Add($"-- WARNING: column [{src.Name}] type narrows ({tgtType} -> {srcType}); review for data truncation.");
        }

        // Default constraint change (separate from ALTER COLUMN).
        if(!NormalizedEqual(src.DefaultDefinition ?? "", tgt.DefaultDefinition ?? ""))
        {
            if(!string.IsNullOrWhiteSpace(tgt.DefaultName))
                columnAlters.Add($"ALTER TABLE {tableId} DROP CONSTRAINT {SqlRender.Quote(tgt.DefaultName)};");
            if(!string.IsNullOrWhiteSpace(src.DefaultDefinition))
            {
                var constraintName = string.IsNullOrWhiteSpace(src.DefaultName)
                    ? string.Empty
                    : $"CONSTRAINT {SqlRender.Quote(src.DefaultName)} ";
                columnAlters.Add($"ALTER TABLE {tableId} ADD {constraintName}DEFAULT {src.DefaultDefinition} FOR {SqlRender.Quote(src.Name)};");
            }
        }
    }

    private static void DiffNamed<T>(
        List<T> source, List<T> target, Func<T, string> key,
        Func<T, T, bool> equal, Action<T> add, Action<T> drop,
        bool includeDrops, ref int changeCount, List<string> warnings, string label)
    {
        var targetByKey = ToDict(target, key);
        var sourceByKey = ToDict(source, key);

        foreach(var s in source)
        {
            if(!targetByKey.TryGetValue(key(s), out var t))
            {
                add(s);
                changeCount++;
                continue;
            }

            if(equal(s, t))
                continue;

            // Changed: drop then re-add.
            drop(t);
            add(s);
            changeCount++;
        }

        foreach(var t in target)
        {
            if(sourceByKey.ContainsKey(key(t)))
                continue;

            if(!includeDrops)
            {
                warnings.Add($"-- WARNING: {label} [{key(t)}] exists only on target and was not dropped. Use --include-drops to remove it.");
                continue;
            }

            drop(t);
            changeCount++;
        }
    }

    private static string Compose(
        TableModel table,
        List<string> pre, List<string> columnAdds, List<string> columnAlters,
        List<string> columnDrops, List<string> post, List<string> warnings)
    {
        var ordered = new List<string>();
        ordered.AddRange(pre);
        ordered.AddRange(columnAdds);
        ordered.AddRange(columnAlters);
        ordered.AddRange(columnDrops);
        ordered.AddRange(post);

        if(ordered.Count == 0 && warnings.Count == 0)
            return string.Empty;

        var sb = new StringBuilder();
        sb.AppendLine($"-- ALTER {SqlRender.TableIdentifier(table)} (column-level sync)");
        foreach(var warning in warnings)
            sb.AppendLine(warning);
        foreach(var statement in ordered)
        {
            sb.AppendLine(statement);
            sb.AppendLine("GO");
        }
        sb.AppendLine();
        return sb.ToString();
    }

    private static bool ColumnsEqual(ColumnModel a, ColumnModel b) =>
        string.Equals(SqlRender.BuildType(a), SqlRender.BuildType(b), StringComparison.OrdinalIgnoreCase) &&
        a.IsNullable == b.IsNullable &&
        string.Equals(a.CollationName, b.CollationName, StringComparison.OrdinalIgnoreCase) &&
        a.IsIdentity == b.IsIdentity &&
        string.Equals(a.IdentitySeed, b.IdentitySeed, StringComparison.OrdinalIgnoreCase) &&
        string.Equals(a.IdentityIncrement, b.IdentityIncrement, StringComparison.OrdinalIgnoreCase) &&
        a.IsComputed == b.IsComputed &&
        NormalizedEqual(a.ComputedDefinition ?? "", b.ComputedDefinition ?? "") &&
        a.IsPersisted == b.IsPersisted &&
        a.IsRowGuid == b.IsRowGuid &&
        NormalizedEqual(a.DefaultDefinition ?? "", b.DefaultDefinition ?? "");

    private static bool KeyConstraintsEqual(KeyConstraintModel a, KeyConstraintModel b) =>
        a.TypeCode == b.TypeCode &&
        a.IndexTypeDesc.Contains("CLUSTERED", StringComparison.OrdinalIgnoreCase) ==
            b.IndexTypeDesc.Contains("CLUSTERED", StringComparison.OrdinalIgnoreCase) &&
        ColumnListEqual(a.Columns, b.Columns);

    private static bool ForeignKeysEqual(ForeignKeyModel a, ForeignKeyModel b) =>
        string.Equals(a.ReferencedSchema, b.ReferencedSchema, StringComparison.OrdinalIgnoreCase) &&
        string.Equals(a.ReferencedTable, b.ReferencedTable, StringComparison.OrdinalIgnoreCase) &&
        a.DeleteActionDesc == b.DeleteActionDesc &&
        a.UpdateActionDesc == b.UpdateActionDesc &&
        a.IsNotForReplication == b.IsNotForReplication &&
        a.Columns.Select(x => $"{x.ParentColumn}>{x.ReferencedColumn}")
            .SequenceEqual(b.Columns.Select(x => $"{x.ParentColumn}>{x.ReferencedColumn}"), StringComparer.OrdinalIgnoreCase);

    private static bool IndexesEqual(IndexModel a, IndexModel b) =>
        a.IsUnique == b.IsUnique &&
        string.Equals(a.TypeDesc, b.TypeDesc, StringComparison.OrdinalIgnoreCase) &&
        NormalizedEqual(a.FilterDefinition ?? "", b.FilterDefinition ?? "") &&
        ColumnListEqual(a.Columns, b.Columns);

    private static bool ColumnListEqual(List<IndexColumnModel> a, List<IndexColumnModel> b) =>
        a.Count == b.Count &&
        a.Zip(b).All(pair =>
            string.Equals(pair.First.Name, pair.Second.Name, StringComparison.OrdinalIgnoreCase) &&
            pair.First.IsDescending == pair.Second.IsDescending &&
            pair.First.IsIncluded == pair.Second.IsIncluded);

    private static bool IsNarrowing(ColumnModel src, ColumnModel tgt)
    {
        if(src.IsUserDefinedType || tgt.IsUserDefinedType)
            return false;
        if(!string.Equals(src.TypeName, tgt.TypeName, StringComparison.OrdinalIgnoreCase))
            return false; // different base type: not a simple narrowing, already flagged as type change

        // -1 means MAX; treat as widest.
        if(tgt.MaxLength == -1 && src.MaxLength != -1)
            return true;
        if(src.MaxLength != -1 && tgt.MaxLength != -1 && src.MaxLength < tgt.MaxLength)
            return true;
        if(src.Precision < tgt.Precision || src.Scale < tgt.Scale)
            return true;
        return false;
    }

    private static bool NormalizedEqual(string a, string b) =>
        string.Equals(SchemaTextNormalizer.Normalize(a), SchemaTextNormalizer.Normalize(b), StringComparison.Ordinal);

    private static Dictionary<string, T> ToDict<T>(IEnumerable<T> items, Func<T, string> key)
    {
        var dict = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
        foreach(var item in items)
            dict[key(item)] = item;
        return dict;
    }
}
