namespace SqlSchemaDiff.IntegrationTests;

/// <summary>
/// A fact that needs a live SQL Server, reached through the connection string in
/// <c>SQLDIFF_TEST_CONN</c>.
/// <para>
/// When the variable is not set the test is skipped rather than failed, so
/// <c>dotnet test</c> on a laptop with no SQL Server still passes and still proves
/// the project compiles. CI sets the variable, so the same tests actually run
/// there.
/// </para>
/// <para>
/// Passing <c>Skip</c> explicitly wins: <c>[LiveFact(Skip = "...")]</c> is how a
/// test that is written but blocked on an engine fix stays in the tree, named and
/// visible in the run output, instead of being commented out.
/// </para>
/// </summary>
public sealed class LiveFactAttribute : FactAttribute
{
    public LiveFactAttribute()
    {
        if(!SqlServerFixture.IsConfigured)
            Skip = $"{SqlServerFixture.ConnectionEnvironmentVariable} is not set";
    }
}
