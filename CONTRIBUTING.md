# Contributing to SQLDiff

Thanks for taking a look. Issues and pull requests are both welcome.

## Getting set up

```bash
git clone https://github.com/peopleworks/SqlSchemaDiff.git
cd SqlSchemaDiff
dotnet build SqlSchemaDiff.csproj -c Release
dotnet test tests/SqlSchemaDiff.Tests/SqlSchemaDiff.Tests.csproj
```

You need the [.NET 9 SDK](https://dotnet.microsoft.com/download) or newer. The unit
tests need no database — they run against in-memory models. A SQL Server instance
(2016 or newer, any edition including Developer and Express) is only needed to try
the CLI end to end.

## Reporting a bug

The useful bug report for a schema tool is a **reproduction in SQL**: the `CREATE`
statements for the source and target, the command you ran, and the script or error
you got. Something like:

```sql
-- source
CREATE TABLE dbo.T (Id int NOT NULL, Sku varchar(60) NOT NULL);
-- target
CREATE TABLE dbo.T (Id int NOT NULL, Sku varchar(20) NOT NULL);
```

That turns straight into a test, which is how most of `tests/RegressionTests.cs` was
written. Please also include `sqldiff --version` and your SQL Server version.

## Pull requests

- **Add a test.** Every fix in `RegressionTests.cs` names the exact SQL Server error
  it prevents, in a comment. Follow that pattern: it is what stops a fix from
  silently regressing.
- **Keep the build clean.** `TreatWarningsAsErrors` is on.
- **Match the surrounding style.** `.editorconfig` covers the mechanical part; the
  house style writes `if(condition)` without a space, uses file-scoped namespaces,
  and comments explain *why* rather than restating the code.
- **One concern per pull request.** A rendering fix and a new object type are two
  pull requests.

## Adding support for a new object type

Sequences, triggers, synonyms and table types are all open (see the roadmap in the
README). The shape of the work is the same each time:

1. Add the model under `Models/`.
2. Read it in `Services/SqlServerSchemaExtractor.cs` — **one set-based query for the
   whole database**, not one query per object. Extraction cost has to stay flat.
3. Render it in `Services/SqlRender.cs`.
4. Compare it in `Services/SchemaDiffer.cs` or `Services/TableDiffer.cs`.
5. Cover the create, the change and the drop with tests.

Two things to watch, because they have both caused real bugs here:

- **System-named constraints.** Anything SQL Server names itself gets a random
  per-database suffix. Match those by shape, never by name, and do not re-emit the
  generated name.
- **Batch boundaries.** `sys.sql_modules` stores the whole batch as the definition,
  so a comment sharing a batch with a `CREATE` becomes part of the object and makes
  it differ forever. Keep each `CREATE` in its own batch.

## Code of conduct

Be decent to each other. Harassment or personal attacks are not welcome, and I will
close threads that go that way.
