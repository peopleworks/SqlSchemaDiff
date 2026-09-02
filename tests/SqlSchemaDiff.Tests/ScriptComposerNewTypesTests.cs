using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;

namespace SqlSchemaDiff.Tests;

/// <summary>
/// The composer's handling of what 1.6 added to the snapshot: schema owners,
/// disabled triggers and sequence positions.
/// </summary>
public sealed class ScriptComposerNewTypesTests
{
    [Fact]
    public void SchemaOwnerIsNamedBehindAPrincipalGuard()
    {
        var snapshot = new DatabaseSnapshot
        {
            DatabaseName = "Db",
            Schemas = { "app" },
            SchemaOwners = new Dictionary<string, string> { ["app"] = "app_owner" }
        };

        var sql = Phase(ScriptComposer.ComposePhases(snapshot), "schemas").Batches.Single().Sql;

        Assert.Contains("DATABASE_PRINCIPAL_ID(N'app_owner') IS NOT NULL", sql);
        Assert.Contains("CREATE SCHEMA [app] AUTHORIZATION [app_owner]", sql);
        Assert.Contains("ELSE", sql);
    }

    [Fact]
    public void SchemaOwnedByDboGetsNoAuthorization()
    {
        var snapshot = new DatabaseSnapshot
        {
            DatabaseName = "Db",
            Schemas = { "app" },
            SchemaOwners = new Dictionary<string, string> { ["app"] = "dbo" }
        };

        var sql = Phase(ScriptComposer.ComposePhases(snapshot), "schemas").Batches.Single().Sql;

        Assert.DoesNotContain("AUTHORIZATION", sql);
        Assert.DoesNotContain("DATABASE_PRINCIPAL_ID", sql);
    }

    [Fact]
    public void DisabledTriggerIsCreatedThenDisabled()
    {
        var snapshot = new DatabaseSnapshot
        {
            DatabaseName = "Db",
            Objects =
            {
                new DbSchemaObject
                {
                    Type = DbObjectType.Trigger,
                    Schema = "dbo",
                    Name = "trBlock",
                    Definition = "CREATE TRIGGER dbo.trBlock ON dbo.T INSTEAD OF DELETE AS BEGIN RAISERROR('no', 16, 1); END",
                    Dependencies = { "Table:dbo.T" },
                    Trigger = new TriggerModel { ParentSchema = "dbo", ParentName = "T", IsDisabled = true, IsInsteadOf = true }
                },
                new DbSchemaObject
                {
                    Type = DbObjectType.Trigger,
                    Schema = "dbo",
                    Name = "trLive",
                    Definition = "CREATE TRIGGER dbo.trLive ON dbo.T AFTER INSERT AS BEGIN SET NOCOUNT ON; END",
                    Dependencies = { "Table:dbo.T" },
                    Trigger = new TriggerModel { ParentSchema = "dbo", ParentName = "T" }
                }
            }
        };

        var batches = Phase(ScriptComposer.ComposePhases(snapshot), "triggers").Batches.ToList();

        Assert.Equal(3, batches.Count);
        var create = batches.Single(x => x.Describe.Contains("[trBlock]") && !x.Describe.StartsWith("Disable"));
        var disable = batches.Single(x => x.Describe.StartsWith("Disable"));
        Assert.True(batches.IndexOf(create) < batches.IndexOf(disable));
        Assert.Contains("DISABLE TRIGGER [dbo].[trBlock] ON [dbo].[T]", disable.Sql);
        Assert.DoesNotContain(batches, x => x.Describe.Contains("[trLive]") && x.Describe.StartsWith("Disable"));
    }

    [Fact]
    public void SequencesRestartOnlyWhenAsked()
    {
        var snapshot = new DatabaseSnapshot
        {
            DatabaseName = "Db",
            Objects =
            {
                new DbSchemaObject
                {
                    Type = DbObjectType.Sequence,
                    Schema = "sales",
                    Name = "InvoiceNumber",
                    Definition = "CREATE SEQUENCE [sales].[InvoiceNumber] AS int START WITH 1000 INCREMENT BY 1;",
                    Sequence = new SequenceModel
                    {
                        Schema = "sales",
                        Name = "InvoiceNumber",
                        TypeName = "int",
                        StartValue = "1000",
                        Increment = "1",
                        CurrentValue = "1041"
                    }
                }
            }
        };

        var silent = ScriptComposer.ComposePhases(snapshot);
        Assert.Empty(Phase(silent, "finalize").Batches);
        Assert.Contains("CREATE SEQUENCE", Phase(silent, "sequences").Batches.Single().Sql);

        var restarted = ScriptComposer.ComposePhases(snapshot, new ComposeOptions { RestartSequences = true });
        var finalize = Phase(restarted, "finalize").Batches.Single();
        Assert.Contains("ALTER SEQUENCE [sales].[InvoiceNumber] RESTART WITH", finalize.Sql);
    }

    private static ScriptPhase Phase(IReadOnlyList<ScriptPhase> phases, string name) =>
        phases.Single(x => x.Name == name);
}
