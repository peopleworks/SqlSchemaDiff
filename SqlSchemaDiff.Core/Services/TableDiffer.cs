using System.Text;
using System.Text.RegularExpressions;
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

        /// <summary>
        /// Why this table cannot be reconciled where it stands, one line per reason,
        /// and empty when it can. <see cref="Script"/> still carries everything the
        /// differ could express; the caller decides between running that and
        /// rebuilding the table around its rows - see <see cref="TableRebuilder"/>.
        /// </summary>
        public IReadOnlyList<string> RebuildReasons { get; init; } = Array.Empty<string>();

        /// <summary>True when <see cref="RebuildReasons"/> has anything in it.</summary>
        public bool RequiresRebuild => RebuildReasons.Count > 0;
    }

    public TableAlterResult Diff(TableModel source, TableModel target, bool includeDrops) =>
        Diff(source, target, includeDrops, options: null);

    /// <param name="options">
    /// Work another part of the same script has already taken on, so this table does
    /// not emit it a second time. Null - all any caller outside
    /// <see cref="SchemaDiffer"/> needs - means the table stands on its own.
    /// </param>
    public TableAlterResult Diff(TableModel source, TableModel target, bool includeDrops, TableDiffOptions? options)
    {
        var pre = new List<string>();       // drops of constraints/indexes
        var columnAdds = new List<string>();
        var columnAlters = new List<string>();
        var columnDrops = new List<string>();
        var post = new List<string>();      // adds of constraints/indexes
        var warnings = new List<string>();
        var rebuildReasons = new List<string>();
        var changeCount = 0;

        var sourceCols = ToDict(source.Columns, x => x.Name);
        var targetCols = ToDict(target.Columns, x => x.Name);

        // Every column name either side knows about. A check constraint's expression
        // is searched for these, which is how the differ works out which checks stand
        // in the way of an ALTER COLUMN.
        var columnNames = new HashSet<string>(sourceCols.Keys, StringComparer.OrdinalIgnoreCase);
        columnNames.UnionWith(targetCols.Keys);

        // Foreign keys another table's rebuild has already dropped and put back. They
        // stay out of the comparison altogether: adding one again would fail, and so
        // would dropping the target's copy, which is no longer there.
        var handledForeignKeys = new HashSet<string>(
            options?.ForeignKeysHandledElsewhere ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);

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
            if(AppendColumnAlter(source, col, targetCol, columnAlters, warnings, rebuildReasons))
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
            CheckConstraintsEqual,
            add: s => AddCheckConstraint(source, s, post, warnings),
            drop: t => pre.Add(SqlRender.BuildConstraintDrop(source, t.Name)),
            touches: x => ReferencedColumnNames(x.Definition, columnNames),
            rewrittenColumns, includeDrops, ref changeCount, warnings, "check constraint",
            alterInPlace: (s, t) => TryAlterCheckConstraintInPlace(source, s, t, rewrittenColumns, columnNames, post));

        // ---- Foreign keys ----
        DiffNamed(
            Retain(source.ForeignKeys, handledForeignKeys),
            Retain(target.ForeignKeys, handledForeignKeys),
            ForeignKeyMatchKey,
            ForeignKeysEqual,
            add: s => AddForeignKey(source, s, post, warnings),
            drop: t => pre.Add(SqlRender.BuildConstraintDrop(source, t.Name)),
            touches: x => x.Columns.Select(c => c.ParentColumn),
            rewrittenColumns, includeDrops, ref changeCount, warnings, "foreign key",
            alterInPlace: (s, t) => TryAlterForeignKeyInPlace(source, s, t, rewrittenColumns, post));

        // ---- Indexes ----
        DiffNamed(
            source.Indexes, target.Indexes, x => x.Name,
            IndexesEqual,
            add: s => AddIndex(source, s, post),
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
            WarningCount = warnings.Count,
            RebuildReasons = rebuildReasons
        };
    }

    /// <summary>
    /// Emits the statements that turn <paramref name="tgt"/> into <paramref name="src"/>.
    /// Returns true when the column's storage is rewritten, which means dependent
    /// indexes and keys have to be dropped around the change.
    /// </summary>
    private static bool AppendColumnAlter(
        TableModel source, ColumnModel src, ColumnModel tgt,
        List<string> columnAlters, List<string> warnings, List<string> rebuildReasons)
    {
        var tableId = SqlRender.TableIdentifier(source);

        // Identity cannot be changed with ALTER COLUMN. Nothing this method emits can
        // express it, so it is recorded as a rebuild reason as well as a warning, and
        // the caller chooses between an ALTER that leaves the column behind and a
        // rebuild that carries the rows across.
        if(src.IsIdentity != tgt.IsIdentity ||
           !string.Equals(src.IdentitySeed, tgt.IdentitySeed, StringComparison.OrdinalIgnoreCase) ||
           !string.Equals(src.IdentityIncrement, tgt.IdentityIncrement, StringComparison.OrdinalIgnoreCase))
        {
            warnings.Add($"-- WARNING: column [{src.Name}] identity property differs and cannot be changed with ALTER COLUMN. Manual table rebuild required.");
            rebuildReasons.Add($"column [{src.Name}] changes its identity property, which ALTER COLUMN cannot do");
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
    /// Reconciles an index that differs only in its storage options or its disabled
    /// state. A fill factor or compression change is a rebuild either way; going
    /// through ALTER INDEX saves the re-sort a drop and re-create would add on top of
    /// it, and a disabled index has no rows to re-sort at all.
    /// </summary>
    /// <remarks>
    /// An index that is disabled on both sides and differs in its options is the one
    /// case this declines: SET and REBUILD both need the index online, and bringing
    /// it up only to put it back down is slower than the drop and re-create the
    /// caller falls back to.
    /// </remarks>
    private static bool TryAlterIndexInPlace(
        TableModel table, IndexModel src, IndexModel tgt,
        HashSet<string> rewrittenColumns, List<string> statements)
    {
        if(!IndexShapeEqual(src, tgt) ||
           DependsOnRewrittenColumn(tgt, x => x.Columns.Select(c => c.Name), rewrittenColumns))
            return false;

        if(src.IsDisabled && tgt.IsDisabled)
            return false;

        var alters = new List<string>();

        // SQL Server has no ALTER INDEX ... ENABLE, so an index coming back online is
        // rebuilt first and only then given its options; one going offline keeps them
        // until the last statement takes it down.
        if(!src.IsDisabled && tgt.IsDisabled)
            alters.Add(SqlRender.BuildIndexRebuild(table, tgt));

        alters.AddRange(SqlRender.BuildIndexOptionsAlter(
            table, tgt.Name, src, tgt, SqlRender.IsColumnstore(tgt.TypeDesc)));

        if(src.IsDisabled && !tgt.IsDisabled)
            alters.Add(SqlRender.BuildIndexDisable(table, tgt));

        if(alters.Count == 0)
            return false;

        statements.AddRange(alters);
        return true;
    }

    /// <summary>
    /// Reconciles a check constraint whose expression is unchanged and whose enabled
    /// or trusted state is not.
    /// </summary>
    private static bool TryAlterCheckConstraintInPlace(
        TableModel table, CheckConstraintModel src, CheckConstraintModel tgt,
        HashSet<string> rewrittenColumns, HashSet<string> columnNames, List<string> statements)
    {
        if(!CheckConstraintShapeEqual(src, tgt) ||
           DependsOnRewrittenColumn(tgt, x => ReferencedColumnNames(x.Definition, columnNames), rewrittenColumns))
            return false;

        return AppendConstraintStateAlters(
            table, tgt.Name, src.IsDisabled, src.IsNotTrusted, tgt.IsDisabled, tgt.IsNotTrusted, statements);
    }

    /// <summary>
    /// Reconciles a foreign key that points at the same columns as before and differs
    /// only in whether it is switched on and whether its rows were ever checked.
    /// Dropping a foreign key and putting it back re-validates every row in the table
    /// - on a large one that is minutes of work for a bit that a single ALTER sets.
    /// </summary>
    private static bool TryAlterForeignKeyInPlace(
        TableModel table, ForeignKeyModel src, ForeignKeyModel tgt,
        HashSet<string> rewrittenColumns, List<string> statements)
    {
        if(!ForeignKeyShapeEqual(src, tgt) ||
           DependsOnRewrittenColumn(tgt, x => x.Columns.Select(c => c.ParentColumn), rewrittenColumns))
            return false;

        return AppendConstraintStateAlters(
            table, tgt.Name, src.IsDisabled, src.IsNotTrusted, tgt.IsDisabled, tgt.IsNotTrusted, statements);
    }

    /// <summary>
    /// Moves a constraint's enabled and trusted state from the target's to the
    /// source's without dropping it, and reports whether it had anything to do.
    /// <para>
    /// SQL Server offers three reachable states, not four. <c>NOCHECK CONSTRAINT</c>
    /// switches a constraint off and clears its trusted bit in one go;
    /// <c>WITH NOCHECK CHECK CONSTRAINT</c> switches it back on and leaves it
    /// untrusted, so it polices new rows and says nothing about the old ones; and
    /// <c>WITH CHECK CHECK CONSTRAINT</c> switches it on after validating every
    /// existing row, which is the only thing that makes it trusted again. There is no
    /// disabled-but-trusted.
    /// </para>
    /// <para>
    /// So going from a trusted target to an untrusted source costs two statements:
    /// NOCHECK to clear the bit, then WITH NOCHECK CHECK to switch the constraint back
    /// on. That is a downgrade of a guarantee, and deliberately so. An enabled but
    /// untrusted constraint on the source means its rows were never validated there;
    /// the target is being made to match the source, and leaving the target trusted
    /// would be a disagreement every later diff reports again and no run ever settles.
    /// </para>
    /// </summary>
    /// <param name="name">
    /// The target's name for the constraint - the one that exists on the server the
    /// script runs against, which for a server-named constraint is not the source's.
    /// </param>
    private static bool AppendConstraintStateAlters(
        TableModel table, string name,
        bool sourceDisabled, bool sourceNotTrusted,
        bool targetDisabled, bool targetNotTrusted,
        List<string> statements)
    {
        if(sourceDisabled == targetDisabled && sourceNotTrusted == targetNotTrusted)
            return false;

        if(sourceDisabled)
        {
            statements.Add(SqlRender.BuildConstraintNoCheck(table, name));
            return true;
        }

        if(!targetDisabled && sourceNotTrusted && !targetNotTrusted)
            statements.Add(SqlRender.BuildConstraintNoCheck(table, name));

        statements.Add(sourceNotTrusted
            ? SqlRender.BuildConstraintCheckNoValidate(table, name)
            : SqlRender.BuildConstraintCheck(table, name));
        return true;
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

    /// <summary>
    /// Adds a check constraint and restores the disabled state its model carries.
    /// <c>ADD CONSTRAINT</c> always leaves a constraint enabled, exactly as CREATE
    /// TABLE does, so the NOCHECK afterwards is what
    /// <see cref="SqlRender.BuildTableCreateScript"/> emits too.
    /// </summary>
    private static void AddCheckConstraint(
        TableModel table, CheckConstraintModel check, List<string> statements, List<string> warnings)
    {
        statements.Add(SqlRender.BuildCheckConstraintAdd(table, check));
        AppendDisableAfterAdd(table, check.Name, check.IsSystemNamed, check.IsDisabled,
            "check constraint", statements, warnings);
    }

    /// <summary>Adds a foreign key and restores its disabled state. See <see cref="AddCheckConstraint"/>.</summary>
    private static void AddForeignKey(
        TableModel table, ForeignKeyModel foreignKey, List<string> statements, List<string> warnings)
    {
        statements.Add(SqlRender.BuildForeignKeyAdd(table, foreignKey));
        AppendDisableAfterAdd(table, foreignKey.Name, foreignKey.IsSystemNamed, foreignKey.IsDisabled,
            "foreign key", statements, warnings);
    }

    /// <summary>Creates an index and puts it straight back to sleep if that is its state.</summary>
    private static void AddIndex(TableModel table, IndexModel index, List<string> statements)
    {
        statements.Add(SqlRender.BuildIndexCreate(table, index));
        if(index.IsDisabled)
            statements.Add(SqlRender.BuildIndexDisable(table, index));
    }

    /// <summary>
    /// The <c>NOCHECK</c> that follows an <c>ADD CONSTRAINT</c> for a constraint that
    /// is meant to be switched off - or a warning, when the constraint has no name of
    /// its own to switch off by. A server-named constraint gets a fresh random name on
    /// the target, which the script cannot know, and guessing one would disable
    /// whatever else happened to answer to it.
    /// </summary>
    private static void AppendDisableAfterAdd(
        TableModel table, string name, bool isSystemNamed, bool isDisabled, string label,
        List<string> statements, List<string> warnings)
    {
        if(!isDisabled)
            return;

        if(isSystemNamed || string.IsNullOrWhiteSpace(name))
        {
            warnings.Add($"-- WARNING: {label} [{name}] is disabled on source but its name is server-generated, " +
                         "so the one created here cannot be named and stays enabled.");
            return;
        }

        statements.Add(SqlRender.BuildConstraintNoCheck(table, name));
    }

    /// <summary>
    /// The list with everything another part of the script has already dealt with
    /// taken out. The identity used is the same match key the comparison uses, so a
    /// server-named key is recognised by its shape on both sides.
    /// </summary>
    private static List<ForeignKeyModel> Retain(List<ForeignKeyModel> keys, HashSet<string> handled) =>
        handled.Count == 0 ? keys : keys.Where(x => !handled.Contains(ForeignKeyMatchKey(x))).ToList();

    private static readonly Regex DefinitionIdentifiers = new(
        @"'(?:[^']|'')*'|\[(?<bracketed>(?:[^\]]|\]\])*)\]|(?<bare>[A-Za-z_][A-Za-z0-9_@#$]*)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The table's own columns named inside a constraint's expression, bracketed or
    /// bare.
    /// <para>
    /// SQL Server refuses <c>ALTER COLUMN</c> while a check constraint mentions the
    /// column ("The object 'CK_x' is dependent on column 'y'", error 5074), and unlike
    /// an index or a key there is no catalog view listing which columns a check
    /// touches - the answer is only in its expression. So the expression is read:
    /// every identifier in it that matches a column name counts, and string literals
    /// are skipped so a value that happens to spell a column name does not. Matching
    /// a function that shares a column's name costs one needless drop and re-create;
    /// missing a real reference costs a script that fails.
    /// </para>
    /// </summary>
    private static IEnumerable<string> ReferencedColumnNames(string? definition, HashSet<string> columnNames)
    {
        if(string.IsNullOrWhiteSpace(definition) || columnNames.Count == 0)
            yield break;

        foreach(Match match in DefinitionIdentifiers.Matches(definition))
        {
            var bracketed = match.Groups["bracketed"];
            var bare = match.Groups["bare"];

            string candidate;
            if(bracketed.Success)
                candidate = bracketed.Value.Replace("]]", "]");
            else if(bare.Success)
                candidate = bare.Value;
            else
                continue; // a string literal

            if(columnNames.TryGetValue(candidate, out var actual))
                yield return actual;
        }
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

    /// <summary>
    /// The identity a foreign key is matched by across two snapshots. Public because
    /// <see cref="TableRebuilder"/> hands the same keys back through
    /// <see cref="TableDiffOptions.ForeignKeysHandledElsewhere"/>, and the two have to
    /// agree on what "the same key" means.
    /// </summary>
    public static string ForeignKeyMatchKey(ForeignKeyModel f) =>
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

    /// <summary>
    /// Two foreign keys are the same when they point the same way <b>and</b> are in
    /// the same state. Leaving the flags out of this is how a target that quietly
    /// re-enabled a disabled key, or validated an untrusted one, used to diff clean:
    /// the two databases enforced different rules and the tool said they matched.
    /// </summary>
    private static bool ForeignKeysEqual(ForeignKeyModel a, ForeignKeyModel b) =>
        ForeignKeyShapeEqual(a, b) &&
        a.IsDisabled == b.IsDisabled &&
        a.IsNotTrusted == b.IsNotTrusted;

    /// <summary>
    /// Everything about a foreign key that only a drop and re-create can change. Split
    /// out from <see cref="ForeignKeysEqual"/> so a state-only change can go through
    /// <c>NOCHECK</c>/<c>CHECK</c> instead.
    /// </summary>
    private static bool ForeignKeyShapeEqual(ForeignKeyModel a, ForeignKeyModel b) =>
        string.Equals(a.ReferencedSchema, b.ReferencedSchema, StringComparison.OrdinalIgnoreCase) &&
        string.Equals(a.ReferencedTable, b.ReferencedTable, StringComparison.OrdinalIgnoreCase) &&
        a.DeleteActionDesc == b.DeleteActionDesc &&
        a.UpdateActionDesc == b.UpdateActionDesc &&
        a.IsNotForReplication == b.IsNotForReplication &&
        a.Columns.Select(x => $"{x.ParentColumn}>{x.ReferencedColumn}")
            .SequenceEqual(b.Columns.Select(x => $"{x.ParentColumn}>{x.ReferencedColumn}"), StringComparer.OrdinalIgnoreCase);

    /// <summary>See <see cref="ForeignKeysEqual"/>: the state is part of the comparison.</summary>
    private static bool CheckConstraintsEqual(CheckConstraintModel a, CheckConstraintModel b) =>
        CheckConstraintShapeEqual(a, b) &&
        a.IsDisabled == b.IsDisabled &&
        a.IsNotTrusted == b.IsNotTrusted;

    /// <summary>The expression, which is the only part of a check a drop and re-create can change.</summary>
    private static bool CheckConstraintShapeEqual(CheckConstraintModel a, CheckConstraintModel b) =>
        NormalizedEqual(a.Definition, b.Definition);

    private static bool IndexesEqual(IndexModel a, IndexModel b) =>
        IndexShapeEqual(a, b) && IndexOptionsEqual(a, b) && a.IsDisabled == b.IsDisabled;

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

/// <summary>
/// What one table's diff should leave to the rest of the script. A table is normally
/// diffed on its own, but a rebuild elsewhere in the same run reaches across table
/// boundaries - it has to drop the foreign keys pointing at the table it replaces and
/// put them back - and this is how the tables on the other end of those keys are told
/// not to do it a second time.
/// </summary>
public sealed class TableDiffOptions
{
    /// <summary>
    /// Foreign keys on this table that something else has already dropped and
    /// re-created, as <see cref="TableDiffer.ForeignKeyMatchKey"/> values. Both sides
    /// of the comparison drop out: there is nothing left to add, and nothing left to
    /// drop either.
    /// </summary>
    public IReadOnlyCollection<string> ForeignKeysHandledElsewhere { get; init; } = Array.Empty<string>();
}
