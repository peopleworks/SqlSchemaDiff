using SqlSchemaDiff.Services;

namespace SqlSchemaDiff.Tests;

/// <summary>
/// <c>GO</c> is a client convention, not T-SQL, so the splitter has to decide for
/// itself where a batch ends. These pin the cases where "a line that looks like GO"
/// is not a separator.
/// </summary>
public class BatchSplitterTests
{
    [Fact]
    public void SplitsOnAStandaloneGo()
    {
        var batches = SqlBatchSplitter.Split("SELECT 1;\nGO\nSELECT 2;\nGO\n");

        Assert.Equal(2, batches.Count);
        Assert.Equal("SELECT 1;", batches[0]);
        Assert.Equal("SELECT 2;", batches[1]);
    }

    // Reproduced as: ERROR: Missing end comment mark '*/'
    // A change-history header with GO on its own line was cut in half.
    [Fact]
    public void DoesNotSplitOnAGoInsideABlockComment()
    {
        const string script = """
                              /*
                              Change history:
                              GO
                              2026-08-21 - first version
                              */
                              CREATE TABLE dbo.Probe(Id int NOT NULL);
                              GO
                              """;

        var batches = SqlBatchSplitter.Split(script);

        Assert.Single(batches);
        Assert.Contains("Change history:", batches[0]);
        Assert.Contains("CREATE TABLE dbo.Probe", batches[0]);
    }

    [Fact]
    public void HandlesNestedBlockComments()
    {
        const string script = "/* outer /* inner\nGO\n*/ still outer */\nSELECT 1;\nGO\nSELECT 2;";

        var batches = SqlBatchSplitter.Split(script);

        Assert.Equal(2, batches.Count);
        Assert.Contains("SELECT 1;", batches[0]);
        Assert.Equal("SELECT 2;", batches[1]);
    }

    [Fact]
    public void DoesNotSplitOnAGoInsideAStringLiteral()
    {
        const string script = "INSERT INTO dbo.T VALUES('first\nGO\nsecond');\nGO\nSELECT 1;";

        var batches = SqlBatchSplitter.Split(script);

        Assert.Equal(2, batches.Count);
        Assert.Contains("first\nGO\nsecond", batches[0]);
    }

    [Fact]
    public void DoesNotSplitOnAGoInsideABracketedIdentifier()
    {
        const string script = "SELECT [weird\nGO\ncolumn] FROM dbo.T;\nGO\nSELECT 1;";

        var batches = SqlBatchSplitter.Split(script);

        Assert.Equal(2, batches.Count);
    }

    [Fact]
    public void TreatsADoubledQuoteAsAnEscapeNotAnEnding()
    {
        const string script = "SELECT 'it''s fine\nGO\nstill inside';\nGO\nSELECT 2;";

        var batches = SqlBatchSplitter.Split(script);

        Assert.Equal(2, batches.Count);
        Assert.Equal("SELECT 2;", batches[1]);
    }

    [Fact]
    public void AcceptsATrailingCommentOnTheSeparator()
    {
        var batches = SqlBatchSplitter.Split("SELECT 1;\nGO -- end of batch\nSELECT 2;");

        Assert.Equal(2, batches.Count);
    }

    [Fact]
    public void RepeatsTheBatchWhenTheSeparatorCarriesACount()
    {
        var batches = SqlBatchSplitter.Split("INSERT INTO dbo.T DEFAULT VALUES;\nGO 3\n");

        Assert.Equal(3, batches.Count);
        Assert.All(batches, b => Assert.Equal("INSERT INTO dbo.T DEFAULT VALUES;", b));
    }

    [Theory]
    [InlineData("SELECT GOAL FROM dbo.T;")]
    [InlineData("GOTO done;")]
    [InlineData("SELECT 1; GO")]      // not alone on its line
    public void DoesNotSplitOnSomethingThatMerelyStartsWithGo(string script)
    {
        var batches = SqlBatchSplitter.Split(script);

        Assert.Single(batches);
    }

    [Fact]
    public void IgnoresAGoInsideALineComment()
    {
        var batches = SqlBatchSplitter.Split("SELECT 1; -- then\nGO\nSELECT 2;");

        Assert.Equal(2, batches.Count);
    }

    [Fact]
    public void DropsEmptyBatches()
    {
        var batches = SqlBatchSplitter.Split("\nGO\n\nGO\nSELECT 1;\nGO\n");

        Assert.Single(batches);
        Assert.Equal("SELECT 1;", batches[0]);
    }

    [Fact]
    public void HandlesAnEmptyScript()
    {
        Assert.Empty(SqlBatchSplitter.Split(string.Empty));
        Assert.Empty(SqlBatchSplitter.Split("   \n  \n"));
    }
}
