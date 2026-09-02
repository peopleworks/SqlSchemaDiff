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
the CLI end to end, and to run the live tests below.

## Running the live tests

`tests/SqlSchemaDiff.IntegrationTests` drives the engine against a real server:
it builds a database from `Schemas/full.sql`, extracts it, deploys the generated
script into an empty database and requires the two to compare equal. This is the
only coverage `SqlServerSchemaExtractor` has, because every one of its queries
reads a catalog view that cannot be faked.

Point `SQLDIFF_TEST_CONN` at a **server**, not a database — the tests create their
own:

```bash
# LocalDB, which ships with SQL Server Express and needs no setup
SQLDIFF_TEST_CONN='Server=(localdb)\MSSQLLocalDB;Integrated Security=True;TrustServerCertificate=True' \
  dotnet test tests/SqlSchemaDiff.IntegrationTests
```

```powershell
# PowerShell
$env:SQLDIFF_TEST_CONN = 'Server=(localdb)\MSSQLLocalDB;Integrated Security=True;TrustServerCertificate=True'
dotnet test tests/SqlSchemaDiff.IntegrationTests
```

Scratch databases are named `SqlDiffIT_<8 hex>` and dropped when the run finishes,
including after a failure. If a run is killed hard enough to skip the cleanup, they
are safe to delete by hand — nothing else uses that prefix.

**Without the variable set, every live test reports itself as skipped and the run
passes.** That is deliberate: contributors without a SQL Server still get a green
`dotnet test`, and CI runs the same tests for real against a container.

One test is skipped even with a connection, and names the engine limitation that
blocks it in its `Skip` reason. If you fix the limitation, delete the `Skip` — the
body is already written.

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
6. Add it to `tests/SqlSchemaDiff.IntegrationTests/Schemas/full.sql`, so the live
   round trip proves a real SQL Server accepts what you render.

Two things to watch, because they have both caused real bugs here:

- **System-named constraints.** Anything SQL Server names itself gets a random
  per-database suffix. Match those by shape, never by name, and do not re-emit the
  generated name.
- **Batch boundaries.** `sys.sql_modules` stores the whole batch as the definition,
  so a comment sharing a batch with a `CREATE` becomes part of the object and makes
  it differ forever. Keep each `CREATE` in its own batch.

## Code of conduct

Be decent to each other. Harassment or personal attacks are not welcome, and I will
close threads that go that way. The full terms, and how to report an incident, are
in [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
