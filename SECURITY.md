# Security policy

## Reporting a vulnerability

Open a [private security advisory](https://github.com/peopleworks/SqlSchemaDiff/security/advisories/new),
or email **peopleworks@gmail.com** with `SQLDIFF SECURITY` in the subject.
Please do not open a public issue for a vulnerability.

Include the version (`sqldiff --version`), what you ran, and what happened.
You will get a first response within a week.

## Handling credentials

SQLDiff connects to SQL Server with a connection string you supply. **A password
passed as a command-line argument is not private**: other processes on the machine
can read the full command line, shells record it in history, and CI runners
usually echo the command. SQLDiff therefore accepts the connection string three
other ways, and any of them is preferable:

```bash
sqldiff extract --conn-file ./prod.conn      # a file you control the permissions of
sqldiff extract --conn env:MY_CONN           # read from a named variable
SQLDIFF_CONN="..." sqldiff extract           # read from the default variable
```

On Windows, `Integrated Security=True` avoids storing a password at all.

## What SQLDiff writes

- **Generated `.sql` scripts** contain schema only — no table data. They do contain
  object definitions, which may themselves be sensitive.
- **Snapshot `.json` files** contain the same schema metadata. They never contain a
  connection string or a password.
- **The audit log** (`--log`) records the server and database name taken from the
  connection string, never the password.

## What SQLDiff executes

`apply`, `sync` and `deploy` run DDL against the target database. Everything runs in
a single transaction by default, so a failure rolls the whole change back. Review
the generated script before applying it to anything you care about — `diff` writes
the script without touching the target, and `--dry-run` parses it into batches
without executing them.
