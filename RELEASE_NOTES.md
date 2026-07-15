# Release Notes

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
