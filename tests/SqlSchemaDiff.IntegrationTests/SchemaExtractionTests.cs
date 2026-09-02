using Microsoft.Data.SqlClient;
using SqlSchemaDiff.Models;

namespace SqlSchemaDiff.IntegrationTests;

/// <summary>
/// What the extractor reads back out of a real catalog. Everything here is a
/// property some renderer branch depends on, so a regression shows up as a named
/// assertion rather than as a mysterious round-trip failure three tests later.
/// </summary>
[Collection(SqlServerCollection.Name)]
public sealed class SchemaExtractionTests
{
    private readonly SqlServerFixture _sqlServer;

    public SchemaExtractionTests(SqlServerFixture sqlServer) => _sqlServer = sqlServer;

    [LiveFact]
    public async Task ExtractCapturesEveryObject()
    {
        var connectionString = await _sqlServer.CreateDatabaseWithFullSchemaAsync();
        var (snapshot, notices) = await SqlServerFixture.ExtractWithNoticesAsync(connectionString);

        // ---- counts -------------------------------------------------------
        Assert.Equal(4, snapshot.Count(DbObjectType.Table));
        Assert.Equal(3, snapshot.Count(DbObjectType.View));
        Assert.Equal(3, snapshot.Count(DbObjectType.Function));
        Assert.Equal(2, snapshot.Count(DbObjectType.StoredProcedure));
        Assert.Equal(12, snapshot.Objects.Count);

        // ---- prerequisites ------------------------------------------------
        Assert.Equal(new[] { "ops", "sales" }, snapshot.Schemas);

        var accountCode = Assert.Single(snapshot.Types, x => x.Name == "AccountCode");
        Assert.Equal("varchar", accountCode.BaseTypeName);
        Assert.Equal(20, accountCode.MaxLength);
        Assert.False(accountCode.IsNullable);
        Assert.False(string.IsNullOrWhiteSpace(accountCode.CollationName));

        var amount = Assert.Single(snapshot.Types, x => x.Name == "Amount");
        Assert.Equal("decimal", amount.BaseTypeName);
        Assert.Equal(19, amount.Precision);
        Assert.Equal(4, amount.Scale);
        Assert.True(amount.IsNullable);
        Assert.Null(amount.CollationName);

        // ---- columns ------------------------------------------------------
        var customer = snapshot.Table("sales", "Customer");

        var customerId = customer.Column("CustomerId");
        Assert.True(customerId.IsIdentity);
        Assert.Equal("1000", customerId.IdentitySeed);
        Assert.Equal("5", customerId.IdentityIncrement);

        var externalId = customer.Column("ExternalId");
        Assert.True(externalId.IsRowGuid);
        Assert.Equal("uniqueidentifier", externalId.TypeName);

        var fullName = customer.Column("FullName");
        Assert.True(fullName.IsComputed);
        Assert.False(fullName.IsPersisted);
        Assert.Contains("FirstName", fullName.ComputedDefinition);

        Assert.Equal("Latin1_General_BIN2", customer.Column("SortKey").CollationName);
        Assert.Equal(-1, customer.Column("Notes").MaxLength);   // varchar(max)
        Assert.Equal(-1, customer.Column("Photo").MaxLength);   // varbinary(max)
        Assert.Equal("money", customer.Column("Balance").TypeName);
        Assert.Equal("bit", customer.Column("IsActive").TypeName);
        Assert.Equal(3, customer.Column("CreatedAt").Scale);    // datetime2(3)
        Assert.Equal(4, customer.Column("Rating").Precision);   // numeric(4,1)
        Assert.Equal(1, customer.Column("Rating").Scale);
        Assert.True(customer.Column("Rating").IsNullable);
        Assert.False(customer.Column("FirstName").IsNullable);

        // A default the script named, and one SQL Server named for itself. The
        // second must be flagged, because re-emitting a generated name on another
        // database bakes in drift that can never be reconciled.
        var creditLimit = customer.Column("CreditLimit");
        Assert.Equal("DF_Customer_CreditLimit", creditLimit.DefaultName);
        Assert.False(creditLimit.DefaultIsSystemNamed);

        var balance = customer.Column("Balance");
        Assert.True(balance.DefaultIsSystemNamed);
        Assert.StartsWith("DF__", balance.DefaultName);
        Assert.Equal("((0))", balance.DefaultDefinition);

        var invoice = snapshot.Table("sales", "Invoice");
        var lineTotal = invoice.Column("LineTotal");
        Assert.True(lineTotal.IsComputed);
        Assert.True(lineTotal.IsPersisted);

        var unitPrice = invoice.Column("UnitPrice");
        Assert.True(unitPrice.IsUserDefinedType);
        Assert.Equal("sales", unitPrice.TypeSchema);
        Assert.Equal("Amount", unitPrice.TypeName);

        // ---- keys ---------------------------------------------------------
        Assert.Equal("CLUSTERED", snapshot.Table("sales", "Terms").Key("PK_Terms").IndexTypeDesc);
        Assert.Equal("NONCLUSTERED", snapshot.Table("sales", "Terms").Key("UQ_Terms_Code").IndexTypeDesc);
        Assert.Equal("NONCLUSTERED", invoice.Key("PK_Invoice").IndexTypeDesc);

        var uniqueClustered = invoice.Key("UQ_Invoice_Customer_Sku");
        Assert.Equal("UQ", uniqueClustered.TypeCode);
        Assert.Equal("CLUSTERED", uniqueClustered.IndexTypeDesc);
        Assert.Equal(new[] { "CustomerId", "Sku" }, uniqueClustered.Columns.Select(x => x.Name));

        // ---- foreign keys -------------------------------------------------
        var fkCustomer = invoice.ForeignKey("FK_Invoice_Customer");
        Assert.Equal("CASCADE", fkCustomer.DeleteActionDesc);
        Assert.Equal("NO_ACTION", fkCustomer.UpdateActionDesc);
        Assert.False(fkCustomer.IsNotTrusted);
        Assert.False(fkCustomer.IsDisabled);

        // The one that points at a table whose name sorts later.
        var fkTerms = invoice.ForeignKey("FK_Invoice_Terms");
        Assert.Equal("SET_NULL", fkTerms.UpdateActionDesc);
        Assert.Equal("Terms", fkTerms.ReferencedTable);
        Assert.True(string.CompareOrdinal("Invoice", fkTerms.ReferencedTable) < 0,
            "the fixture is supposed to reference a table that sorts after the referencing one");
        Assert.Contains("Table:sales.Terms",
            snapshot.Object(DbObjectType.Table, "sales", "Invoice").Dependencies);

        var audit = snapshot.Table("ops", "AuditEntry");
        var untrusted = audit.ForeignKey("FK_AuditEntry_Invoice");
        Assert.True(untrusted.IsNotTrusted);
        Assert.False(untrusted.IsDisabled);

        var disabled = audit.ForeignKey("FK_AuditEntry_Customer");
        Assert.True(disabled.IsDisabled);

        // ---- check constraints --------------------------------------------
        var creditCheck = customer.Check("CK_Customer_CreditLimit");
        Assert.False(creditCheck.IsDisabled);
        Assert.False(creditCheck.IsSystemNamed);
        Assert.Contains("CreditLimit", creditCheck.Definition);

        Assert.True(audit.Check("CK_AuditEntry_EventKind").IsDisabled);

        // ---- indexes ------------------------------------------------------
        var filtered = customer.Index("UX_Customer_Email");
        Assert.True(filtered.IsUnique);
        Assert.Equal("NONCLUSTERED", filtered.TypeDesc);
        Assert.Contains("Email", filtered.FilterDefinition);
        Assert.Equal(new[] { "Email" }, filtered.Columns.Where(x => !x.IsIncluded).Select(x => x.Name));
        Assert.Equal(
            new[] { "FirstName", "LastName" },
            filtered.Columns.Where(x => x.IsIncluded).Select(x => x.Name).OrderBy(x => x, StringComparer.Ordinal));

        var descending = invoice.Index("IX_Invoice_IssuedAt");
        var keyColumns = descending.Columns.Where(x => !x.IsIncluded).OrderBy(x => x.KeyOrdinal).ToList();
        Assert.Equal(new[] { "IssuedAt", "Sku" }, keyColumns.Select(x => x.Name));
        Assert.True(keyColumns[0].IsDescending);
        Assert.False(keyColumns[1].IsDescending);

        // ---- programmable objects -----------------------------------------
        Assert.Contains("SCHEMABINDING", snapshot.Object(DbObjectType.View, "sales", "vInvoiceTotals").Definition);
        Assert.Contains("View:sales.vInvoiceTotals",
            snapshot.Object(DbObjectType.View, "sales", "vInvoiceTotalsByCustomer").Dependencies);
        Assert.Contains("Function:sales.fnLineTotal",
            snapshot.Object(DbObjectType.View, "sales", "vInvoiceComputed").Dependencies);
        Assert.Contains("RETURNS @result TABLE",
            snapshot.Object(DbObjectType.Function, "ops", "fnRecentAudit").Definition);
        Assert.StartsWith("CREATE PROCEDURE",
            snapshot.Object(DbObjectType.StoredProcedure, "ops", "uspTouchAudit").Definition);

        // The procedure created with QUOTED_IDENTIFIER OFF has to come back like any
        // other, with the option recorded on the module rather than in its text.
        var quotedIdentifierOff = await SqlServerFixture.ScalarAsync<int>(
            connectionString,
            "SELECT CONVERT(int, uses_quoted_identifier) FROM sys.sql_modules WHERE object_id = OBJECT_ID(N'ops.uspTouchAudit');");
        Assert.Equal(0, quotedIdentifierOff);

        // ---- notices ------------------------------------------------------
        Assert.True(notices.Count == 0, $"unexpected extraction notices: {string.Join(" | ", notices)}");
    }

    /// <summary>
    /// Constructs the engine does not model yet must degrade to a notice. Silently
    /// dropping them would produce a snapshot that looks complete and deploys an
    /// incomplete database.
    /// </summary>
    [LiveFact]
    public async Task ExtractorNoticesForUnsupported()
    {
        var connectionString = _sqlServer.CreateDatabase();
        var script = SqlServerFixture.UnsupportedSchemaScript;

        var columnstoreCreated = await TryApplyAsync(connectionString, Section(script, "COLUMNSTORE", "TEMPORAL"));
        var temporalCreated = await TryApplyAsync(connectionString, Section(script, "TEMPORAL", null));

        Assert.True(columnstoreCreated || temporalCreated,
            "neither a columnstore index nor a system-versioned table could be created on this edition; " +
            "the test cannot say anything about extractor notices");

        var (snapshot, notices) = await SqlServerFixture.ExtractWithNoticesAsync(connectionString);
        var joined = string.Join(Environment.NewLine, notices);

        if(columnstoreCreated)
        {
            // The table itself is captured; only the index it cannot render is skipped.
            var fact = snapshot.Table("dbo", "Fact");
            Assert.DoesNotContain(fact.Indexes, x => x.Name == "CSX_Fact_Amount");
            Assert.Contains("CSX_Fact_Amount", joined);
            Assert.Contains("COLUMNSTORE", joined, StringComparison.OrdinalIgnoreCase);
        }

        if(temporalCreated)
        {
            // The current table is captured with a notice that SYSTEM_VERSIONING is
            // not scripted; the history table SQL Server owns is skipped outright.
            Assert.Contains("Employee", joined);
            Assert.Contains("EmployeeHistory", joined);
            Assert.Contains("system-versioned", joined, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(snapshot.Objects, x => x.Name == "EmployeeHistory");
            snapshot.Table("dbo", "Employee");
        }
    }

    private static async Task<bool> TryApplyAsync(string connectionString, string script)
    {
        try
        {
            await SqlServerFixture.ApplyAsync(connectionString, script, useTransaction: false);
            return true;
        }
        catch(SqlException)
        {
            // Some editions refuse columnstore or temporal tables. Not a failure of
            // the extractor, so the matching assertions are simply skipped.
            return false;
        }
    }

    /// <summary>
    /// Cuts the block between two <c>@@MARKER@@</c> comments out of the script, so
    /// each feature can be attempted on its own.
    /// </summary>
    private static string Section(string script, string from, string? to)
    {
        var start = script.IndexOf($"-- @@{from}@@", StringComparison.Ordinal);
        Assert.True(start >= 0, $"marker @@{from}@@ is missing from unsupported.sql");

        if(to is null)
            return script[start..];

        var end = script.IndexOf($"-- @@{to}@@", start, StringComparison.Ordinal);
        Assert.True(end > start, $"marker @@{to}@@ is missing from unsupported.sql");
        return script[start..end];
    }
}
