# PeopleWorks.SqlSchemaDiff.Core

The SQL Server schema engine behind [SQLDiff](https://github.com/peopleworks/SqlSchemaDiff),
packaged as a library so that every tool that needs it shares **one** implementation.

It extracts a database's structure, compares two structures, and generates the T-SQL that
carries one to the shape of the other — emitting incremental `ALTER TABLE` statements that
**preserve the rows already in the table** rather than rebuilding it.

## Why this package exists

The engine used to be copied into each consumer. The copies drifted, and the drift was not
cosmetic: one copy still compared tables as normalised text and could only offer a
`DROP`/`CREATE` rebuild, while the other had moved on to column-level `ALTER`. The tool
calling the stale copy silently lost the data-preserving behaviour that is the entire point.

One package, one behaviour, one place to fix a bug.

## Install

```bash
dotnet add package PeopleWorks.SqlSchemaDiff.Core
```

## Using it

```csharp
using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;

// 1. read both sides
var extractor = new SqlServerSchemaExtractor();
var source = await extractor.ExtractAsync(sourceConnectionString, ct);
var target = await extractor.ExtractAsync(targetConnectionString, ct);

// 2. optionally narrow the comparison; filters must apply to BOTH sides,
//    or a skipped object looks target-only and a later drop would remove it
var filter = ObjectFilter.Parse(include: "table:", exclude: "dbo.Audit*");
source = filter.Apply(source);
target = filter.Apply(target);

// 3. compare — nothing is executed, you get a script back
var diff = new SchemaDiffer().Diff(
    source, target,
    includeDrops: false,          // target-only objects are reported, not dropped
    includeTableDrops: false,     // dropping whole tables needs this as well
    allowTableRebuild: false,     // refuse a rebuild rather than lose rows
    addOnly: false);              // true = only add what is missing

Console.WriteLine(diff.Script);
```

`SqlBatchExecutor` applies a script in a single transaction — if any batch fails the whole
change rolls back — and `AuditLogger` records what ran.

## What is in the box

| Type | Does |
|---|---|
| `SqlServerSchemaExtractor` | Reads tables, columns, indexes, keys, constraints, views, procedures, functions and alias types into a `DatabaseSnapshot` |
| `SchemaDiffer` | Compares two snapshots and composes the migration script, ordering objects by dependency |
| `TableDiffer` | The data-preserving part: column-level `ADD` / `ALTER COLUMN` / `DROP COLUMN` |
| `ObjectFilter` | `[type:]glob` include/exclude patterns, applied to both sides |
| `SqlBatchExecutor` · `SqlBatchSplitter` | Splits on `GO` and applies in one transaction |
| `SchemaTextNormalizer` · `SqlModuleRewriter` · `SqlRender` | Text normalisation and DDL rendering |
| `ConnectionStringResolver` · `ConnectionVerifier` | Connection strings from a value, a file or `env:NAME`; connectivity checks |

## Safety defaults

Nothing is dropped unless asked. An object that exists only on the target is reported as a
`-- WARNING:` comment and left in place; `includeDrops` enables removing them, and dropping
whole tables needs `includeTableDrops` on top of that. A table that cannot be reconciled with
`ALTER` alone is refused unless `allowTableRebuild` is set.

The namespaces are `SqlSchemaDiff.*`, matching the package: `SqlSchemaDiff.Models` and
`SqlSchemaDiff.Services`.

MIT licensed. Built by [PeopleWorks](https://github.com/peopleworks).
