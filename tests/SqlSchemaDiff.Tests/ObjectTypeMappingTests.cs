using SqlSchemaDiff.Models;
using SqlSchemaDiff.Services;

namespace SqlSchemaDiff.Tests;

public class ObjectTypeMappingTests
{
    // sys.objects.type is char(2), so single-char codes arrive space-padded ("U ").
    // Regression guard for: "Unsupported SQL object type code: U".
    [Theory]
    [InlineData("U", DbObjectType.Table)]
    [InlineData("U ", DbObjectType.Table)]
    [InlineData("V ", DbObjectType.View)]
    [InlineData("P ", DbObjectType.StoredProcedure)]
    [InlineData("FN", DbObjectType.Function)]
    [InlineData("IF", DbObjectType.Function)]
    [InlineData("TF", DbObjectType.Function)]
    [InlineData("FS", DbObjectType.Function)]
    [InlineData("FT", DbObjectType.Function)]
    public void MapsPaddedTypeCodes(string code, DbObjectType expected)
    {
        Assert.Equal(expected, SqlServerSchemaExtractor.ToDbObjectType(code));
    }

    [Fact]
    public void UnknownTypeCode_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => SqlServerSchemaExtractor.ToDbObjectType("XX"));
    }
}
