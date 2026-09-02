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
        var pre = new List<string>();       // drops of constraints/indexes
        var columnAdds = new List<string>();
        var columnAlters = new List<string>();
        var columnDrops = new List<string>();
        var post = new List<string>();      // adds of constraints/indexes
        var warnings = new List<string>();
        var changeCount = 0;

        var sourceCols = ToDict(source.Columns, x => x.Name);
        var targetCols = ToDict(target.Columns, x => x.Name);

        // Columns whose storage is being rewritten. SQL Server refuses ALTER COLUMN
        // while an index, key or foreign key references the column, so anything that
        // touches one of these has to be dropped first and put back afterwards —
        // even when the object itself is not changing.
        var rewrittenColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

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

                WarnIfSparseNotNull(col, warnings);
                continue;
            }

            if(ColumnsEqual(col, targetCol))
                continue;

            changeCount++;
            if(AppendColumnAlter(source, col, targetCol, columnAlters, warnings))
                rewrittenColumns.Add(col.Name);
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
                columnDrops.Add(SqlRender.BuildConstraintDrop(source, col.DefaultName));
            columnDrops.Add($"ALTER TABLE {SqlRender.TableIdentifier(source)} DROP COLUMN {SqlRender.Quote(col.Name)};");
            warnings.Add($"-- WARNING: dropping column [{col.Name}] on target permanently deletes its data.");
            changeCount++;
        }

        // ---- Key constraints (PK / UQ) ----
        DiffNamed(
            source.KeyConstraints, target.KeyConstraints, KeyConstraintMatchKey,
            KeyConstraintsEqual,
            add: s => post.Add(SqlRender.BuildKeyConstraintAdd(source, s)),
            drop: t => pre.Add(SqlRender.BuildConstraintDrop(source, t.Name)),
            touches: x => x.Columns.Select(c => c.Name),
            rewrittenColumns, includeDrops, ref changeCount, warnings, "key constraint",
            alterInPlace: (s, t) => TryAlterKeyConstraintInPlace(source, s, t, rewrittenColumns, post));

        // ---- Check constraints ----
        DiffNamed(
            source.CheckConstraints, target.CheckConstraints, CheckConstraintMatchKey,
            (s, t) => NormalizedEqual(s.Definition, t.Definition) && s.IsNotTrusted == t.IsNotTrusted,
            add: s => post.Add(SqlRender.BuildCheckConstraintAdd(source, s)),
            drop: t => pre.Add(SqlRender.BuildConstraintDrop(source, t.Name)),
            touches: _ => Array.Empty<string>(),
            rewrittenColumns, includeDrops, ref changeCount, warnings, "check constraint");

        // ---- Foreign keys ----
        DiffNamed(
            source.ForeignKeys, target.ForeignKeys, ForeignKeyMatchKey,
            ForeignKeysEqual,
            add: s => post.Add(SqlRender.BuildForeignKeyAdd(source, s)),
            drop: t => pre.Add(SqlRender.BuildConstraintDrop(source, t.Name)),
            touches: x => x.Columns.Select(c => c.ParentColumn),
            rewrittenColumns, includeDrops, ref changeCount, warnings, "foreign key");

        // ---- Indexes ----
        DiffNamed(
            source.Indexes, target.Indexes, x => x.Name,
            IndexesEqual,
            add: s => post.Add(SqlRender.BuildIndexCreate(source, s)),
            drop: t => pre.Add(SqlRender.BuildIndexDrop(source, t)),
            touches: x => x.Columns.Select(c => c.Name),
            rewrittenColumns, includeDrops, ref changeCount, warnings, "index",
            alterInPlace: (s, t) => TryAlterIndexInPlace(source, s, t, rewrittenColumns, post));

        // ---- Heap storage ----
        // A heap has no index to carry its compression, so the setting lives on the
        // table and only ALTER TABLE ... REBUILD can change it. This keys off the
        // source alone: if the source has a clustered index the compression rides on
        // that index and was handled above, and if the target had one that is being
        // dropped, the heap it leaves behind still has to be rebuilt. The statement
        // lands after the index work for exactly that reason.
        if(SqlRender.IsHeap(source) &&
           !SqlRender.CompressionEqual(source.DataCompression, target.DataCompression))
        {
            post.Add(SqlRender.BuildTableRebuild(source, source.DataCompression));
            changeCount++;
        }

        var script = Compose(source, pre, columnAdds, columnAlters, columnDrops, post, warnings);
        return new TableAlterResult
        {
            Script = script,
            ChangeCount = changeCount,
            WarningCount = warnings.Count
        };
    }

    /// <summary>
    /// Emits the statements that turn <paramref name="tgt"/> into <paramref name="src"/>.
    /// Returns true when the column's storage is rewritten, which means dependent
    /// indexes and keys have to be dropped around the change.
    /// </summary>
    private static bool AppendColumnAlter(
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
                columnAlters.Add(SqlRender.BuildConstraintDrop(source, tgt.DefaultName));
            columnAlters.Add($"ALTER TABLE {tableId} DROP COLUMN {SqlRender.Quote(src.Name)};");
            columnAlters.Add($"ALTER TABLE {tableId} ADD {SqlRender.BuildColumnDefinition(src)};");
            return true;
        }

        // Type / nullability / collation change.
        var srcType = SqlRender.BuildType(src);
        var tgtType = SqlRender.BuildType(tgt);
        var typeChanged = !string.Equals(srcType, tgtType, StringComparison.OrdinalIgnoreCase);
        var collationChanged = !string.Equals(src.CollationName, tgt.CollationName, StringComparison.OrdinalIgnoreCase);
        var nullabilityChanged = src.IsNullable != tgt.IsNullable;
        var sparseChanged = src.IsSparse != tgt.IsSparse;
        var rewritten = false;

        WarnIfSparseNotNull(src, warnings);

        // An ALTER COLUMN that omits SPARSE clears the flag, so a column that is
        // meant to stay sparse has to restate it on every rewrite.
        if(typeChanged || collationChanged || nullabilityChanged || (sparseChanged && src.IsSparse))
        {
            var sb = new StringBuilder();
            sb.Append($"ALTER TABLE {tableId} ALTER COLUMN {SqlRender.Quote(src.Name)} {srcType}");
            if(!string.IsNullOrWhiteSpace(src.CollationName) && !src.IsUserDefinedType)
                sb.Append($" COLLATE {src.CollationName}");
            if(src.IsSparse)
                sb.Append(" SPARSE");
            sb.Append(src.IsNullable ? " NULL" : " NOT NULL");
            sb.Append(';');
            columnAlters.Add(sb.ToString());
            rewritten = true;

            if(nullabilityChanged && !src.IsNullable)
                warnings.Add($"-- WARNING: column [{src.Name}] becomes NOT NULL; ALTER fails if it contains NULLs. Backfill first.");
            if(typeChanged && IsNarrowing(src, tgt))
                warnings.Add($"-- WARNING: column [{src.Name}] type narrows ({tgtType} -> {srcType}); review for data truncation.");
        }

        // Turning sparse off gets its own statement: DROP SPARSE leaves the rest of
        // the column definition alone. It is emitted after a type rewrite too - the
        // rewrite already clears the flag, but DROP SPARSE on a column that is not
        // sparse is a no-op, and saying it makes the script's intent explicit.
        if(sparseChanged && !src.IsSparse)
        {
            columnAlters.Add($"ALTER TABLE {tableId} ALTER COLUMN {SqlRender.Quote(src.Name)} DROP SPARSE;");
            rewritten = true;
        }

        // Default constraint change (separate from ALTER COLUMN).
        if(!NormalizedEqual(src.DefaultDefinition ?? "", tgt.DefaultDefinition ?? ""))
        {
            if(!string.IsNullOrWhiteSpace(tgt.DefaultName))
                columnAlters.Add(SqlRender.BuildConstraintDrop(source, tgt.DefaultName));
            if(!string.IsNullOrWhiteSpace(src.DefaultDefinition))
            {
                var constraintName = SqlRender.BuildConstraintNameClause(src.DefaultName, src.DefaultIsSystemNamed).TrimStart();
                var prefix = constraintName.Length == 0 ? string.Empty : constraintName + " ";
                columnAlters.Add($"ALTER TABLE {tableId} ADD {prefix}DEFAULT {src.DefaultDefinition} FOR {SqlRender.Quote(src.Name)};");
            }
        }

        return rewritten;
    }

    /// <param name="alterInPlace">
    /// Given the source and target versions of an object that differ, emits the
    /// statements that reconcile them without a drop and re-create, and returns true
    /// when it did. Used for index storage options, which ALTER INDEX can change on
    /// an index that is already there.
    /// </param>
    private static void DiffNamed<T>(
        List<T> source, List<T> target, Func<T, string> matchKey,
        Func<T, T, bool> equal, Action<T> add, Action<T> drop,
        Func<T, IEnumerable<string>> touches,
        HashSet<string> rewrittenColumns,
        bool includeDrops, ref int changeCount, List<string> warnings, string label,
        Func<T, T, bool>? alterInPlace = null)
    {
        var targetByKey = ToDict(target, matchKey);
        var sourceByKey = ToDict(source, matchKey);

        foreach(var s in source)
        {
            if(!targetByKey.TryGetValue(matchKey(s), out var t))
            {
                add(s);
                changeCount++;
                continue;
            }

            if(equal(s, t))
            {
                // Unchanged, but sitting on a column whose type is being rewritten:
                // take it down and put it back so the ALTER COLUMN can go through.
                if(DependsOnRewrittenColumn(t, touches, rewrittenColumns))
                {
                    drop(t);
                    add(s);
                }
                continue;
            }

            // Changed: reconcile in place when that is possible, otherwise drop
            // then re-add.
            if(alterInPlace is null || !alterInPlace(s, t))
            {
                drop(t);
                add(s);
            }
            changeCount++;
        }

        foreach(var t in target)
        {
            if(sourceByKey.ContainsKey(matchKey(t)))
                continue;

            if(!includeDrops)
            {
                warnings.Add($"-- WARNING: {label} [{DisplayName(t)}] exists only on target and was not dropped. Use --include-drops to remove it.");

                // It still has to move out of the way of an ALTER COLUMN, so it is
                // dropped and restored exactly as the target had it.
                if(DependsOnRewrittenColumn(t, touches, rewrittenColumns))
                {
                    drop(t);
                    add(t);
                }
                continue;
            }

            drop(t);
            changeCount++;
        }
    }

    private static bool DependsOnRewrittenColumn<T>(T item, Func<T, IEnumerable<string>> touches, HashSet<string> rewrittenColumns) =>
        rewrittenColumns.Count > 0 && touches(item).Any(rewrittenColumns.Contains);

    // ------------------------------------------------- in-place index alters

    /// <summary>
    /// Reconciles a PRIMARY KEY or UNIQUE constraint whose index differs only in its
    /// storage options. Dropping a key and putting it back also drops and re-checks
    /// every foreign key pointing at it, so avoiding that is worth the special case.
    /// </summary>
    private static bool TryAlterKeyConstraintInPlace(
        TableModel table, KeyConstraintModel src, KeyConstraintModel tgt,
        HashSet<string> rewrittenColumns, List<string> statements)
    {
        if(!KeyConstraintShapeEqual(src, tgt) ||
           DependsOnRewrittenColumn(tgt, x => x.Columns.Select(c => c.Name), rewrittenColumns))
            return false;

        return AppendIndexOptionAlters(table, tgt.Name, src, tgt, isColumnstore: false, statements);
    }

    /// <summary>
    /// Reconciles an index that differs only in its storage options. A fill factor or
    /// compression change is a rebuild either way; going through ALTER INDEX saves
    /// the re-sort a drop and re-create would add on top of it.
    /// </summary>
    private static bool TryAlterIndexInPlace(
        TableModel table, IndexModel src, IndexModel tgt,
        HashSet<string> rewrittenColumns, List<string> statements)
    {
        if(!IndexShapeEqual(src, tgt) ||
           DependsOnRewrittenColumn(tgt, x => x.Columns.Select(c => c.Name), rewrittenColumns))
            return false;

        return AppendIndexOptionAlters(table, tgt.Name, src, tgt, SqlRender.IsColumnstore(tgt.TypeDesc), statements);
    }

    /// <summary>
    /// The target's name is the one that exists on the target server, so it is the
    /// one ALTER INDEX has to use - which matters for a system-named constraint,
    /// where the two sides never share a name.
    /// </summary>
    private static bool AppendIndexOptionAlters(
        TableModel table, string indexName,
        IIndexStorageOptions src, IIndexStorageOptions tgt, bool isColumnstore, List<string> statements)
    {
        var alters = SqlRender.BuildIndexOptionsAlter(table, indexName, src, tgt, isColumnstore);
        if(alters.Count == 0)
            return false;

        statements.AddRange(alters);
        return true;
    }

    private static void WarnIfSparseNotNull(ColumnModel column, List<string> warnings)
    {
        if(column.IsSparse && !column.IsNullable)
        {
            warnings.Add($"-- WARNING: column [{column.Name}] is SPARSE but NOT NULL; " +
                         "SQL Server only allows SPARSE on a nullable column and will reject the statement.");
        }
    }

    private static string DisplayName<T>(T item) => item switch
    {
        KeyConstraintModel k => k.Name,
        CheckConstraintModel c => c.Name,
        ForeignKeyModel f => f.Name,
        IndexModel i => i.Name,
        _ => item?.ToString() ?? string.Empty
    };

    // ------------------------------------------------------------ match keys

    // A constraint SQL Server named itself carries a per-database random suffix
    // (PK__Orders__3214EC07CF883821). Matching those by name would make every
    // database look like it had a different constraint, so they are matched by the
    // shape that actually defines them.

    private static string KeyConstraintMatchKey(KeyConstraintModel k) =>
        k.IsSystemNamed
            ? $"~{k.TypeCode}:{string.Join(",", k.Columns.Select(c => $"{c.Name}:{c.IsDescending}"))}"
            : k.Name;

    private static string CheckConstraintMatchKey(CheckConstraintModel c) =>
        c.IsSystemNamed ? $"~CK:{SchemaTextNormalizer.Normalize(c.Definition)}" : c.Name;

    private static string ForeignKeyMatchKey(ForeignKeyModel f) =>
        f.IsSystemNamed
            ? $"~FK:{f.ReferencedSchema}.{f.ReferencedTable}:" +
              string.Join(",", f.Columns.Select(c => $"{c.ParentColumn}>{c.ReferencedColumn}"))
            : f.Name;

    // ----------------------------------------------------------- composition

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

    // ------------------------------------------------------------ comparison

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
        a.IsSparse == b.IsSparse &&
        NormalizedEqual(a.DefaultDefinition ?? "", b.DefaultDefinition ?? "");

    private static bool KeyConstraintsEqual(KeyConstraintModel a, KeyConstraintModel b) =>
        KeyConstraintShapeEqual(a, b) && IndexOptionsEqual(a, b);

    /// <summary>
    /// Everything about a key constraint that only a drop and re-create can change.
    /// Split out from <see cref="KeyConstraintsEqual"/> so the differ can tell a
    /// storage-option change, which ALTER INDEX handles, from a real one.
    /// </summary>
    private static bool KeyConstraintShapeEqual(KeyConstraintModel a, KeyConstraintModel b) =>
        a.TypeCode == b.TypeCode &&
        SqlRender.IsClustered(a.IndexTypeDesc) == SqlRender.IsClustered(b.IndexTypeDesc) &&
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
        IndexShapeEqual(a, b) && IndexOptionsEqual(a, b);

    /// <summary>Everything about an index that only a drop and re-create can change.</summary>
    private static bool IndexShapeEqual(IndexModel a, IndexModel b) =>
        a.IsUnique == b.IsUnique &&
        string.Equals(a.TypeDesc, b.TypeDesc, StringComparison.OrdinalIgnoreCase) &&
        NormalizedEqual(a.FilterDefinition ?? "", b.FilterDefinition ?? "") &&
        ColumnListEqual(a.Columns, b.Columns);

    /// <summary>
    /// Compares the <c>WITH (...)</c> options of two indexes. Each property defaults
    /// to what SQL Server itself would use, so a snapshot written before these were
    /// captured compares equal to a freshly extracted index left at the defaults.
    /// </summary>
    private static bool IndexOptionsEqual(IIndexStorageOptions a, IIndexStorageOptions b) =>
        a.FillFactor == b.FillFactor &&
        a.IsPadded == b.IsPadded &&
        a.IgnoreDupKey == b.IgnoreDupKey &&
        a.AllowRowLocks == b.AllowRowLocks &&
        a.AllowPageLocks == b.AllowPageLocks &&
        SqlRender.CompressionEqual(a.DataCompression, b.DataCompression);

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
