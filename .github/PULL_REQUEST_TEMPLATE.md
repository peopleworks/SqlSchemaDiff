## What this changes

<!-- One paragraph. What was wrong or missing, and what this does about it. -->

Closes #

## Why

<!--
  The reasoning that is not obvious from the diff: the SQL Server error it prevents,
  the behavior it preserves, the alternative you rejected.
-->

## Checklist

- [ ] **Add a test.** Every fix in `RegressionTests.cs` names, in a comment, the exact
      SQL Server error it prevents. Follow that pattern — it is what stops a fix from
      silently regressing.
- [ ] **Cover the create, the change and the drop** if this touches an object type.
- [ ] **Keep the build clean.** `TreatWarningsAsErrors` is on.
- [ ] **Match the surrounding style.** `.editorconfig` covers the mechanical part; the
      house style writes `if(condition)` without a space, uses file-scoped namespaces,
      and comments explain *why* rather than restating the code.
- [ ] **One concern per pull request.** A rendering fix and a new object type are two
      pull requests.
- [ ] **No connection strings, passwords, or unshareable production definitions** in the
      code, the tests, the docs, or the commit message.

## If this touches a new object type

<!--
  Extraction has to stay flat: one set-based query for the whole database, never one
  query per object. Two things that have caused real bugs here:

  - System-named constraints get a random per-database suffix. Match them by shape,
    never by name, and do not re-emit the generated name.
  - sys.sql_modules stores the whole batch as the definition, so a comment sharing a
    batch with a CREATE becomes part of the object and makes it differ forever.
-->

- [ ] Extraction is one set-based query for the whole database, not one per object.
- [ ] System-named constraints are matched by shape, not by name.
- [ ] Each `CREATE` is emitted in its own batch.
