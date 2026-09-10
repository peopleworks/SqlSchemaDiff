using Microsoft.Data.SqlClient;
using SqlSchemaDiff.Services;

namespace SqlSchemaDiff.Tests;

/// <summary>
/// The parts of the per-table extract that decide something before a server is
/// involved: what the arguments have to be, and the one predicate that turns a
/// whole-database catalog query into a single-object one. Everything else about
/// WP 1.8b needs a real catalog and lives in the live tests.
/// </summary>
public sealed class TableExtractTests
{
    /// <summary>
    /// A server that is not there, reached quickly. A test that gets past argument
    /// validation fails with a <see cref="SqlException"/> in about a second instead of
    /// hanging, which is what makes "it threw before it connected" a real assertion.
    /// </summary>
    private const string Unreachable =
        "Server=localhost,1;Database=nope;Connect Timeout=1;Encrypt=false;TrustServerCertificate=true";

    // ------------------------------------------------------------- arguments

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task ExtractTableAsync_RejectsAnEmptySchema_BeforeItConnects(string? schema)
    {
        var extractor = new SqlServerSchemaExtractor();

        await Assert.ThrowsAnyAsync<ArgumentException>(
            () => extractor.ExtractTableAsync(Unreachable, schema!, "Orders", CancellationToken.None));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task ExtractTableAsync_RejectsAnEmptyName_BeforeItConnects(string? name)
    {
        var extractor = new SqlServerSchemaExtractor();

        await Assert.ThrowsAnyAsync<ArgumentException>(
            () => extractor.ExtractTableAsync(Unreachable, "dbo", name!, CancellationToken.None));
    }

    [Fact]
    public async Task ExtractTableAsync_RejectsANullConnection()
    {
        var extractor = new SqlServerSchemaExtractor();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => extractor.ExtractTableAsync(null!, null, "dbo", "Orders", CancellationToken.None));
    }

    [Fact]
    public async Task ExtractAsync_RejectsANullConnection()
    {
        var extractor = new SqlServerSchemaExtractor();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => extractor.ExtractAsync(null!, null, CancellationToken.None));
    }

    // ----------------------------------------------------------------- scope

    /// <summary>
    /// The whole-database read has to keep running exactly the query it always ran:
    /// no predicate, and no parameter for one.
    /// </summary>
    [Fact]
    public void DatabaseScope_AddsNothingToTheQuery()
    {
        var scope = SqlServerSchemaExtractor.CatalogScope.Database;

        Assert.Equal(string.Empty, scope.Filter("c.object_id"));

        using var command = new SqlCommand();
        scope.Bind(command);
        Assert.Empty(command.Parameters);
    }

    /// <summary>
    /// And the per-table read has to narrow on the column the caller names, not on
    /// some fixed one: the owning object is <c>c.object_id</c> in one query and
    /// <c>kc.parent_object_id</c> in the next.
    /// </summary>
    [Fact]
    public void ObjectScope_NarrowsOnTheColumnItIsGiven()
    {
        var scope = SqlServerSchemaExtractor.CatalogScope.Object(1381579960);

        Assert.Equal("AND c.object_id = @object_id", scope.Filter("c.object_id"));
        Assert.Equal("AND kc.parent_object_id = @object_id", scope.Filter("kc.parent_object_id"));
    }

    /// <summary>
    /// The id travels as a parameter, never as text spliced into the SQL.
    /// </summary>
    [Fact]
    public void ObjectScope_BindsTheObjectIdAsAParameter()
    {
        var scope = SqlServerSchemaExtractor.CatalogScope.Object(1381579960);

        using var command = new SqlCommand();
        scope.Bind(command);

        var parameter = Assert.Single(command.Parameters.Cast<SqlParameter>());
        Assert.Equal("@object_id", parameter.ParameterName);
        Assert.Equal(1381579960, parameter.Value);
    }
}
