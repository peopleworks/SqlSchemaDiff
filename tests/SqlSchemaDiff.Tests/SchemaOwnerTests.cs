using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

public class SchemaOwnerRenderTests
{
    [Fact]
    public void AKnownOwnerBecomesAnAuthorizationClause()
    {
        var statement = SqlRender.BuildSchemaCreate("app", "app_owner");

        Assert.Contains("IF SCHEMA_ID(N'app') IS NULL", statement);
        Assert.Contains("IF DATABASE_PRINCIPAL_ID(N'app_owner') IS NOT NULL", statement);
        Assert.Contains("EXEC(N'CREATE SCHEMA [app] AUTHORIZATION [app_owner]')", statement);
        Assert.Contains("ELSE", statement);
    }

    // dbo is the default owner, and naming a principal the target may not have turns
    // a harmless preamble into a hard failure.
    [Theory]
    [InlineData("dbo")]
    [InlineData("DBO")]
    [InlineData(null)]
    [InlineData("")]
    public void NoAuthorizationIsEmittedForDboOrAnUnknownOwner(string? owner)
    {
        Assert.Equal(SqlRender.BuildSchemaCreate("app"), SqlRender.BuildSchemaCreate("app", owner));
        Assert.DoesNotContain("AUTHORIZATION", SqlRender.BuildSchemaCreate("app", owner));
    }

    [Fact]
    public void QuotesInsideTheStatementAreEscapedForTheExecWrapper()
    {
        var statement = SqlRender.BuildSchemaCreate("o'brien", "o'neil");

        Assert.Contains("IF SCHEMA_ID(N'o''brien') IS NULL", statement);
        Assert.Contains("DATABASE_PRINCIPAL_ID(N'o''neil')", statement);
        Assert.Contains("EXEC(N'CREATE SCHEMA [o''brien] AUTHORIZATION [o''neil]')", statement);
    }
}

public class SchemaOwnerDifferTests
{
    private readonly SchemaDiffer _differ = new();

    private static DatabaseSnapshot WithSchema(string name, string schema, string owner, params DbSchemaObject[] objects)
    {
        var snapshot = new DatabaseSnapshot
        {
            DatabaseName = name,
            Schemas = { schema },
            SchemaOwners = new Dictionary<string, string> { [schema] = owner },
            Objects = objects.ToList()
        };
        return snapshot;
    }

    private static DbSchemaObject TableIn(string schema, string name)
    {
        var table = Table(name, Col("Id", nullable: false));
        table.Schema = schema;
        return TableObject(table);
    }

    [Fact]
    public void ThePrerequisiteCreateCarriesTheOwnerFromTheSource()
    {
        var source = WithSchema("Src", "app", "app_owner", TableIn("app", "Customer"));

        var result = _differ.Diff(source, Snapshot("Tgt"), false, false, false, false);

        Assert.Contains("CREATE SCHEMA [app] AUTHORIZATION [app_owner]", result.Script);
        Assert.True(
            result.Script.IndexOf("CREATE SCHEMA", StringComparison.Ordinal) <
            result.Script.IndexOf("[app].[Customer]", StringComparison.Ordinal),
            "the schema must be created before the table that lives in it");
    }

    // Changing a schema's owner is ALTER AUTHORIZATION: a permissions change, and the
    // principal may not even exist on the target. A diff reports it, nothing more.
    [Fact]
    public void ADifferentOwnerIsReportedAndNotActedOn()
    {
        var table = TableIn("app", "Customer");
        var source = WithSchema("Src", "app", "dbo", table);
        var target = WithSchema("Tgt", "app", "app_owner", table);

        var result = _differ.Diff(source, target, false, false, false, false);

        Assert.Contains("-- WARNING: schema [app] is owned by [dbo] on source and [app_owner] on target", result.Script);
        Assert.DoesNotContain("ALTER AUTHORIZATION", result.Script);
        Assert.False(result.HasChanges);
        Assert.Equal(0, result.Changed);
    }

    [Fact]
    public void MatchingOwnersSaySoWithSilence()
    {
        var table = TableIn("app", "Customer");
        var source = WithSchema("Src", "app", "app_owner", table);
        var target = WithSchema("Tgt", "app", "APP_OWNER", table);

        Assert.DoesNotContain("owned by", _differ.Diff(source, target, false, false, false, false).Script);
    }

    // A pre-1.6 snapshot has no owners at all, and that is not a difference.
    [Fact]
    public void AMissingOwnerMapIsNotADifference()
    {
        var table = TableIn("app", "Customer");
        var source = WithSchema("Src", "app", "app_owner", table);
        var target = new DatabaseSnapshot { DatabaseName = "Tgt", Schemas = { "app" }, Objects = { table } };

        Assert.DoesNotContain("owned by", _differ.Diff(source, target, false, false, false, false).Script);
    }

    [Fact]
    public void AFilterKeepsTheOwnersSoPrerequisitesStillNameThem()
    {
        var source = WithSchema("Src", "app", "app_owner", TableIn("app", "Customer"), TableIn("dbo", "Audit"));

        var filtered = ObjectFilter.Parse("app.*", null).Apply(source);

        Assert.Single(filtered.Objects);
        Assert.NotNull(filtered.SchemaOwners);
        Assert.Equal("app_owner", filtered.SchemaOwners!["app"]);
    }
}
