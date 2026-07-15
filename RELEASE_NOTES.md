# Release Notes

## v1.2.0 (unreleased)

### Highlights
- **Transactional apply** (default): all batches run in one transaction and roll back
  together on any failure, leaving the target untouched. Opt out with `--no-transaction`.
- **Audit log** via `--log <file>`: records timestamp, server, database, script,
  batches executed and outcome (applied / rolled-back / failed) for every run.
- **Unit test project** (`tests/SqlSchemaDiff.Tests`, xUnit): 36 tests covering the
  column-level differ, schema differ, batch splitter, SQL rendering and type mapping.

### Fixes
- Fixed `ERROR: Unsupported SQL object type code: U` when a database contains views,
  procedures, functions or foreign keys. `sys.objects.type` is `char(2)`, so single-char
  codes arrive space-padded (`"U "`); the type code is now trimmed before mapping.
  Regression-tested against padded codes.

## v1.1.0

### Highlights
- **Column-level table sync**: changed tables now generate incremental
  `ALTER TABLE ADD/ALTER COLUMN`, constraint and index changes that preserve data,
  instead of being skipped or fully rebuilt.
- Snapshots now carry structured table metadata (`Table` model) enabling the diff.
- Safety warnings for risky operations (NOT NULL without default, type narrowing,
  identity changes) and `--include-drops` gating for destructive column drops.
- Shared `SqlRender` module so extract and diff emit identical column syntax.

Verified end-to-end against SQL Server 2025 (add columns, alter type/nullability,
create unique index) with zero data loss and drift returning to 0.

## v1.0.0

Initial public release of **SQLDiff**.

### Highlights
- SQL Server schema extraction for tables, views, procedures, and functions.
- Dependency-aware diff script generation.
- One-step deployment command (`deploy`) for `diff + apply`.
- Add-only mode (`--add-only`) to create only missing objects.
- Connection verification command (`check-conn`).
- Drift detection mode (`drift`) with exit code `2`.
- GitHub-ready documentation with Mermaid diagrams and troubleshooting.

### Credits
Created by **PeopleWorks** using **Codex**.
