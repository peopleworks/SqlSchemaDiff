# Changelog

All notable changes to SQLDiff are recorded here.
This project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.6.0] - Unreleased

### Changed

- **The packages carry the family name.** `SqlSchemaDiff.Core` is now
  **`PeopleWorks.SqlSchemaDiff.Core`** and `SqlSchemaDiff.Cli` is
  **`PeopleWorks.SqlSchemaDiff.Cli`**, in line with `PeopleWorks.SyncJob` and
  `PeopleWorks.DBFSync`. The assembly (`SqlSchemaDiff.Core.dll`), the namespaces
  (`SqlSchemaDiff.Models`, `SqlSchemaDiff.Services`) and the command (`sqldiff`) do not
  change, so a consumer only edits its `PackageReference`. The 1.5.0 ids stay on nuget.org,
  deprecated with a pointer to the new ones.

  ```bash
  dotnet tool install --global PeopleWorks.SqlSchemaDiff.Cli
  dotnet add package PeopleWorks.SqlSchemaDiff.Core
  ```

## [1.5.0] - 2026-08-29

### Added

- **`SqlSchemaDiff.Core`, the engine as a package.** Schema extraction, comparison and
  `ALTER` generation now live in a library of their own, published alongside the tool so
  that other programs share one implementation instead of copying it.

  ```bash
  dotnet add package SqlSchemaDiff.Core
  ```

  This is not housekeeping. The engine had been copied into `MSSQLMCPServer`, and the
  copies drifted: that one still compares a table by normalising its `CREATE` text, so
  when a table differs it can only warn or `DROP` and `CREATE` it, while this repo had
  moved on to `TableDiffer` and column-level `ALTER TABLE` that preserves the rows.
  Measured before the split — `SqlServerSchemaExtractor` 668 lines here against 772
  there with roughly 900 differing lines, `SchemaDiffer` 410 against 301. **The copy an
  AI agent calls through MCP was the one that had lost the data-preserving behaviour the
  tool exists for.**

### Changed

- The CLI package is now **`SqlSchemaDiff.Cli`**, matching the `Core` / `Cli` / `Mcp`
  convention already used by the other PeopleWorks packages on NuGet. The command it
  installs is still `sqldiff`, and the tool package still bundles everything it needs.

  ```bash
  dotnet tool install --global SqlSchemaDiff.Cli
  ```

- `Models/` and `Services/` moved to `SqlSchemaDiff.Core/`. Nothing was rewritten: the
  namespaces stay `SqlSchemaDiff.Models` and `SqlSchemaDiff.Services`, git recorded the
  19 files as renames, and the 90 tests pass unchanged. The CLI keeps only `Program.cs`.

- Release and CI pack both packages; a release pushes both.

## [1.4.0] - 2026-08-21

### Added

- **`--include` and `--exclude` to narrow a comparison.** A pattern is `[type:]glob`,
  where the type is `table`, `view`, `proc` or `func`, and the glob takes `*` and `?`
  and matches either `schema.name` or the bare name; separate several with commas.
  Filters apply to **both** snapshots, so a skipped object is never created, altered
  **or dropped** — filtering only the source would leave it looking target-only, and a
  run with `--include-drops` would then delete the very thing the filter was meant to
  protect. A filtered run announces itself, so a narrowed comparison cannot be mistaken
  for a clean one.
- A [Related projects](README.md#related-projects) section pairing SQLDiff with
  [SyncJob](https://github.com/peopleworks/syncjob), which synchronises the data where
  SQLDiff synchronises the structure.
- A README section answering the recurring complaints about SSMS Schema Compare,
  including the one gap SQLDiff shares: ignore rules are per object, not per property.

### Fixed

- **`GO` inside a block comment, a string literal or a bracketed identifier split the
  batch.** Batches were found with a per-line regular expression, so a header comment
  carrying a change history — a `GO` on its own line inside `/* ... */` — was cut in
  half and both pieces failed with *"Missing end comment mark '*/'"*. `GO` is a client
  convention rather than T-SQL, so the splitter now scans the script properly: nested
  block comments, line comments, `'...'` with doubled-quote escapes, `"..."` and `[...]`
  are all understood, and `GO` separates only when it stands alone on an ordinary line.
  `GO 5` now repeats the batch instead of being read as script text.

### Performance

- Measured on a database matching the shape people report as slow elsewhere — 50 tables,
  100 indexes, 20 views, 100 procedures, 10 functions — a full comparison completes in
  **about one second**.

## [1.3.0] - 2026-08-21

An audit pass ahead of the first public release. Every fix below was reproduced
against SQL Server before it was written, and each one is pinned by a test in
`tests/SqlSchemaDiff.Tests/RegressionTests.cs` that names the error it prevents.

### Fixed

- **Deploying into an empty database failed on the first statement.** Schemas were
  never scripted, so a table in any schema other than `dbo` died with *"The
  specified schema name 'app' either does not exist"*. Generated scripts now open
  with guarded `CREATE SCHEMA` statements for every schema they need.
- **User-defined alias types were referenced but never created**, so a table with a
  `dbo.PhoneNumber` column could not be deployed to a database that did not already
  have the type. Alias types are now captured in the snapshot and emitted as guarded
  `CREATE TYPE` prerequisites.
- **A column of an alias type was scripted with a `COLLATE` clause**, which SQL Server
  rejects outright: *"COLLATE clause cannot be used on user-defined data types."*
- **Procedures, views and functions whose definition begins with a comment were
  never rewritten to `CREATE OR ALTER`**, so re-applying them failed with *"There is
  already an object named 'X' in the database."* A header comment above `CREATE` is
  a very common shape, and the old rewrite only matched `CREATE` at the very start
  of the text. The rewrite now skips leading whitespace and comments.
- **Constraints that SQL Server named itself never converged.** An unnamed primary
  key gets a per-database random suffix (`PK__Orders__3214EC07CF883821`), so the
  same constraint looked different on every database: the diff reported permanent
  drift and the generated `ADD CONSTRAINT` failed with *"Table 'Orders' already has
  a primary key defined on it."* System-named constraints are now matched by shape
  and created without a name, letting the target generate its own.
- **Changing a primary key between clustered and nonclustered went undetected**,
  because the comparison used `IndexTypeDesc.Contains("CLUSTERED")` — which is also
  true for `NONCLUSTERED`.
- **`ALTER COLUMN` failed whenever an index touched the column**: *"The index
  'IX_Item_Sku' is dependent on column 'Sku'."* Indexes, keys and foreign keys that
  reference a column being retyped are now dropped before the `ALTER` and recreated
  after it — including ones that are not otherwise changing, which are restored
  exactly as the target had them.
- **Generated scripts did not set `QUOTED_IDENTIFIER`.** The .NET client sets it, but
  a script opened in sqlcmd or SSMS does not inherit it, so filtered indexes,
  indexed views and persisted computed columns failed to create. Every generated
  script now starts with `SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;`.
- **Temporal history tables were scripted as ordinary tables**, producing a `CREATE`
  that cannot be applied. They are skipped now, with a note; a system-versioned
  table reports that its `SYSTEM_VERSIONING` clause is not scripted.
- **Unsupported index types were dropped from the snapshot silently.** Columnstore,
  XML, spatial and hash indexes are still not scripted, but they are now reported
  instead of disappearing.

### Added

- **Connection strings that stay off the command line.** `--conn-file <path>`,
  `--conn env:VARIABLE`, and the `SQLDIFF_CONN` / `SQLDIFF_SOURCE_CONN` /
  `SQLDIFF_TARGET_CONN` environment variables. A password in an argument is
  readable by any other process on the machine and lands in shell history.
- **`sqldiff --version`**, and `version` as a command.
- **Packaging as a .NET global tool** (`dotnet tool install --global SqlSchemaDiff`),
  which installs the `sqldiff` command.
- **GitHub Actions**: build and test on Linux and Windows, an end-to-end job that
  deploys a schema into an empty SQL Server container and requires drift to come
  back clean, and a release job that publishes single-file executables and the
  NuGet package on a tag.
- `LICENSE` (MIT), `CONTRIBUTING.md`, `SECURITY.md`, `.editorconfig`.

### Changed

- **Extraction reads the whole database in a fixed number of set-based queries**
  instead of a handful per table plus one per index. Measured on a 200-table,
  600-index database: **1,430 queries before, 11 after**. The saving is proportional
  to network latency, so it matters most against a remote server.
- **All CLI output is in English**, matching the documentation. Behaviour, option
  names and exit codes are unchanged.
- The assembly and executable are now named `sqldiff` rather than `SqlSchemaDiff`.
- Warnings are treated as errors in the build.

## [1.2.0] - 2026-07-15

### Added

- **Transactional apply** (default): all batches run in one transaction and roll back
  together on any failure, leaving the target untouched. Opt out with `--no-transaction`.
- **Audit log** via `--log <file>`: records timestamp, server, database, script,
  batches executed and outcome (applied / rolled-back / failed) for every run.
- **Unit test project** (`tests/SqlSchemaDiff.Tests`, xUnit) covering the
  column-level differ, schema differ, batch splitter, SQL rendering and type mapping.
- `diff` / `drift` / `sync` print the identifiers of added, changed and removed
  objects, not just counts.

### Fixed

- `ERROR: Unsupported SQL object type code: U` when a database contains views,
  procedures, functions or foreign keys. `sys.objects.type` is `char(2)`, so
  single-character codes arrive space-padded (`"U "`); the code is trimmed before
  mapping.
- Views, procedures and functions never converging. A comment preceding a
  `CREATE VIEW/PROCEDURE/FUNCTION` shared its batch, so SQL Server stored the comment
  as part of the object definition in `sys.sql_modules`. Each `CREATE` is now
  isolated in its own batch, and the next apply self-heals already-polluted objects.
- Column-order-only differences reported as drift. Tables with a structured model are
  compared structurally (columns, constraints and indexes matched by name) rather
  than by rendered text, so the same columns in a different physical order no longer
  show as changed forever.

## [1.1.0] - 2026-07-15

### Added

- **Column-level table sync**: a changed table now generates incremental
  `ALTER TABLE ADD/ALTER COLUMN`, constraint and index changes that preserve data,
  instead of being skipped or rebuilt.
- Snapshots carry structured table metadata, which is what makes the diff possible.
- Safety warnings for risky operations (`NOT NULL` without a default, type narrowing,
  identity changes) and `--include-drops` gating for destructive column drops.
- A shared `SqlRender` module so extract and diff emit identical column syntax.

## [1.0.0] - 2026-02-16

Initial release.

- Schema extraction for tables, views, procedures and functions.
- Dependency-aware diff script generation.
- One-step deployment (`deploy`) for diff + apply.
- Add-only mode (`--add-only`) to create only what is missing.
- Connection verification (`check-conn`).
- Drift detection (`drift`) with exit code 2.

[1.4.0]: https://github.com/peopleworks/SqlSchemaDiff/releases/tag/v1.4.0
[1.3.0]: https://github.com/peopleworks/SqlSchemaDiff/releases/tag/v1.3.0
[1.2.0]: https://github.com/peopleworks/SqlSchemaDiff/releases/tag/v1.2.0
[1.1.0]: https://github.com/peopleworks/SqlSchemaDiff/releases/tag/v1.1.0
[1.0.0]: https://github.com/peopleworks/SqlSchemaDiff/releases/tag/v1.0.0
