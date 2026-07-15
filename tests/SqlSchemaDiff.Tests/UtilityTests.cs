using SqlSchemaDiff.Services;
using static SqlSchemaDiff.Tests.TestModels;

namespace SqlSchemaDiff.Tests;

public class SchemaTextNormalizerTests
{
    [Fact]
    public void CollapsesWhitespaceAndUppercases()
    {
        var normalized = SchemaTextNormalizer.Normalize("create   table\n\t Foo");
        Assert.Equal("CREATE TABLE FOO", normalized);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void EmptyForNullOrWhitespace(string? input)
    {
        Assert.Equal(string.Empty, SchemaTextNormalizer.Normalize(input!));
    }
}

public class SqlBatchExecutorTests
{
    [Fact]
    public void SplitsBatchesOnGo()
    {
        var script = "SELECT 1;\nGO\nSELECT 2;\nGO\n";
        var batches = SqlBatchExecutor.SplitBatches(script);
        Assert.Equal(2, batches.Count);
    }

    [Fact]
    public void GoWithTrailingCommentStillSplits()
    {
        var script = "SELECT 1;\nGO -- batch separator\nSELECT 2;\n";
        var batches = SqlBatchExecutor.SplitBatches(script);
        Assert.Equal(2, batches.Count);
    }

    [Fact]
    public void EmptyBatchesAreIgnored()
    {
        var script = "GO\nGO\nSELECT 1;\nGO\n";
        var batches = SqlBatchExecutor.SplitBatches(script);
        Assert.Single(batches);
    }
}

public class SqlRenderTests
{
    [Fact]
    public void QuoteEscapesBrackets()
    {
        Assert.Equal("[a]]b]", SqlRender.Quote("a]b"));
    }

    [Fact]
    public void NVarcharTypeUsesCharacterLength()
    {
        // MaxLength is stored in bytes; nvarchar halves it for display.
        var column = NVarchar("Name", 100);
        Assert.Equal("nvarchar(100)", SqlRender.BuildType(column));
    }

    [Fact]
    public void NVarcharMaxRendersAsMax()
    {
        var column = NVarchar("Blob", 1);
        column.MaxLength = -1;
        Assert.Equal("nvarchar(MAX)", SqlRender.BuildType(column));
    }

    [Fact]
    public void DecimalTypeUsesPrecisionAndScale()
    {
        var column = Col("Amount", "decimal", precision: 18, scale: 2);
        Assert.Equal("decimal(18,2)", SqlRender.BuildType(column));
    }
}
