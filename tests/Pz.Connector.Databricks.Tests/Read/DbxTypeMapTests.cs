using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxTypeMapTests
{
    [Theory]
    [InlineData("BIGINT", "LONG", typeof(Int64Type))]
    [InlineData("INT", "INT", typeof(Int32Type))]
    [InlineData("SMALLINT", "SHORT", typeof(Int16Type))]
    [InlineData("TINYINT", "BYTE", typeof(Int8Type))]
    [InlineData("BOOLEAN", "BOOLEAN", typeof(BooleanType))]
    [InlineData("FLOAT", "FLOAT", typeof(FloatType))]
    [InlineData("DOUBLE", "DOUBLE", typeof(DoubleType))]
    [InlineData("STRING", "STRING", typeof(StringType))]
    [InlineData("BINARY", "BINARY", typeof(BinaryType))]
    [InlineData("DATE", "DATE", typeof(Date32Type))]
    [InlineData("ARRAY<INT>", "ARRAY", typeof(StringType))]
    [InlineData("MAP<STRING,INT>", "MAP", typeof(StringType))]
    [InlineData("STRUCT<a: INT>", "STRUCT", typeof(StringType))]
    [InlineData("VARIANT", "USER_DEFINED_TYPE", typeof(StringType))]
    [InlineData("INTERVAL DAY", "INTERVAL", typeof(StringType))]
    public void ToArrow_maps_scalar_and_complex_types(string typeText, string typeName, Type arrow)
    {
        Assert.IsType(arrow, DbxTypeMap.ToArrow(typeText, typeName, "c"));
    }

    [Fact]
    public void ToArrow_maps_decimal_with_precision_and_scale()
    {
        var t = Assert.IsType<Decimal128Type>(DbxTypeMap.ToArrow("DECIMAL(38,9)", "DECIMAL", "c"));
        Assert.Equal(38, t.Precision);
        Assert.Equal(9, t.Scale);
        var d = Assert.IsType<Decimal128Type>(DbxTypeMap.ToArrow("decimal(10, 2)", "DECIMAL", "c"));
        Assert.Equal(10, d.Precision);
        Assert.Equal(2, d.Scale);
    }

    [Fact]
    public void ToArrow_maps_timestamps_by_zone()
    {
        var ts = Assert.IsType<TimestampType>(DbxTypeMap.ToArrow("TIMESTAMP", "TIMESTAMP", "c"));
        Assert.Equal(TimeUnit.Microsecond, ts.Unit);
        Assert.Equal("UTC", ts.Timezone);
        var ntz = Assert.IsType<TimestampType>(DbxTypeMap.ToArrow("TIMESTAMP_NTZ", "TIMESTAMP", "c"));
        Assert.Equal(TimeUnit.Microsecond, ntz.Unit);
        Assert.True(string.IsNullOrEmpty(ntz.Timezone));
    }

    [Fact]
    public void ToArrow_refuses_an_unknown_type_naming_the_column()
    {
        var ex = Assert.Throws<PzConnectorException>(() => DbxTypeMap.ToArrow("GEOMETRY", "USER_DEFINED_TYPE", "shape"));
        Assert.StartsWith("databricks: PZDB0205: ", ex.Message);
        Assert.Contains("'shape'", ex.Message);
        Assert.Contains("GEOMETRY", ex.Message);
    }

    [Fact]
    public void ToArrowSchema_keeps_column_order_and_nullability()
    {
        var schema = DbxTypeMap.ToArrowSchema(new DbxResultSchema(2,
        [
            new DbxColumnInfo("id", "BIGINT", "LONG", 0),
            new DbxColumnInfo("name", "STRING", "STRING", 1),
        ]));
        Assert.Equal(["id", "name"], schema.FieldsList.Select(f => f.Name));
        Assert.All(schema.FieldsList, f => Assert.True(f.IsNullable));
    }

    [Theory]
    [InlineData(typeof(Int64Type), "BIGINT")]
    [InlineData(typeof(Int32Type), "INT")]
    [InlineData(typeof(Date32Type), "DATE")]
    [InlineData(typeof(StringType), "STRING")]
    [InlineData(typeof(DoubleType), null)]
    [InlineData(typeof(BooleanType), null)]
    public void ParameterType_names_the_cursor_types_it_supports(Type arrow, string? expected)
    {
        // Apache.Arrow 23.0.0 exposes each concrete IArrowType's "Default" as a static field, not a
        // property -- reflect over both so this helper tracks either shape.
        var type = (IArrowType)(arrow.GetProperty("Default")?.GetValue(null)
            ?? arrow.GetField("Default")!.GetValue(null))!;
        Assert.Equal(expected, DbxTypeMap.ParameterType(type));
    }

    [Fact]
    public void ParameterType_for_decimal_and_timestamps()
    {
        Assert.Equal("DECIMAL(12,3)", DbxTypeMap.ParameterType(new Decimal128Type(12, 3)));
        Assert.Equal("TIMESTAMP", DbxTypeMap.ParameterType(new TimestampType(TimeUnit.Microsecond, "UTC")));
        Assert.Equal("TIMESTAMP_NTZ", DbxTypeMap.ParameterType(new TimestampType(TimeUnit.Microsecond, (string?)null)));
    }
}
