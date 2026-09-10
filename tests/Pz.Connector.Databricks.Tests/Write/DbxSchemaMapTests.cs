using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet.Schema;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxSchemaMapTests
{
    private static DbxColumnPlan PlanOne(IArrowType type) =>
        DbxSchemaMap.Plan(new Schema([new Apache.Arrow.Field("c", type, true)], null), "out")[0];

    // Apache.Arrow 23.0.0 exposes each concrete IArrowType's "Default" as a static field, not a
    // property -- Time32Type/Time64Type are the exception (a property) -- so reflect over both.
    private static IArrowType Default(Type arrow) =>
        (IArrowType)(arrow.GetProperty("Default")?.GetValue(null) ?? arrow.GetField("Default")!.GetValue(null))!;

    [Theory]
    [InlineData(typeof(Int8Type), typeof(int), "INT")]
    [InlineData(typeof(Int16Type), typeof(int), "INT")]
    [InlineData(typeof(Int32Type), typeof(int), "INT")]
    [InlineData(typeof(UInt8Type), typeof(int), "INT")]
    [InlineData(typeof(UInt16Type), typeof(int), "INT")]
    [InlineData(typeof(Int64Type), typeof(long), "BIGINT")]
    [InlineData(typeof(UInt32Type), typeof(long), "BIGINT")]
    [InlineData(typeof(FloatType), typeof(float), "FLOAT")]
    [InlineData(typeof(DoubleType), typeof(double), "DOUBLE")]
    [InlineData(typeof(StringType), typeof(string), "STRING")]
    [InlineData(typeof(LargeStringType), typeof(string), "STRING")]
    [InlineData(typeof(BinaryType), typeof(byte[]), "BINARY")]
    [InlineData(typeof(LargeBinaryType), typeof(byte[]), "BINARY")]
    [InlineData(typeof(BooleanType), typeof(bool), "BOOLEAN")]
    public void Plain_types_keep_their_parquet_logical_type(Type arrow, Type clr, string databricks)
    {
        var plan = PlanOne(Default(arrow));
        Assert.Equal(clr, plan.SpoolField.ClrType);
        Assert.Equal(databricks, plan.DatabricksType);
        Assert.Null(plan.CastTo);
        Assert.Equal("`c`", DbxSchemaMap.SelectExpression(plan));
    }

    [Fact]
    public void Dates_and_timestamps_map_by_zone()
    {
        var date = PlanOne(Date32Type.Default);
        Assert.Equal(DateTimeFormat.Date, ((DateTimeDataField)date.SpoolField).DateTimeFormat);
        Assert.Equal("DATE", date.DatabricksType);

        var utc = PlanOne(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var utcField = (DateTimeDataField)utc.SpoolField;
        Assert.True(utcField.IsAdjustedToUTC);
        Assert.Equal(DateTimeTimeUnit.Micros, utcField.Unit);
        Assert.Equal("TIMESTAMP", utc.DatabricksType);
        Assert.Null(utc.CastTo);

        var naive = PlanOne(new TimestampType(TimeUnit.Microsecond, (string?)null));
        Assert.Equal("TIMESTAMP_NTZ", naive.DatabricksType);

        // NaiveTimestampAsString is a const bool: the branch it does not select is genuinely
        // unreachable (CS0162 under TreatWarningsAsErrors), so only the selected shape is asserted.
        Assert.True(DbxSchemaMap.NaiveTimestampAsString);
        Assert.Equal(typeof(string), naive.SpoolField.ClrType);
        Assert.Equal("TIMESTAMP_NTZ", naive.CastTo);
    }

    [Fact]
    public void Decimals_and_uint64_spool_as_strings_and_cast_back()
    {
        var dec = PlanOne(new Decimal128Type(38, 9));
        Assert.Equal(typeof(string), dec.SpoolField.ClrType);
        Assert.Equal("DECIMAL(38,9)", dec.DatabricksType);
        Assert.Equal("DECIMAL(38,9)", dec.CastTo);
        Assert.Equal("cast(`c` as DECIMAL(38,9)) as `c`", DbxSchemaMap.SelectExpression(dec));

        var u64 = PlanOne(UInt64Type.Default);
        Assert.Equal(typeof(string), u64.SpoolField.ClrType);
        Assert.Equal("DECIMAL(20,0)", u64.CastTo);
    }

    [Theory]
    [InlineData(typeof(Time32Type))]
    [InlineData(typeof(Time64Type))]
    [InlineData(typeof(NullType))]
    public void Unwritable_types_are_PZDB0303(Type arrow)
    {
        var type = Default(arrow);
        var ex = Assert.Throws<PzConnectorException>(() => PlanOne(type));
        Assert.StartsWith("databricks: PZDB0303: out: column 'c' has unsupported Arrow type", ex.Message);
    }

    [Fact]
    public void Nested_types_and_decimal256_are_PZDB0303()
    {
        Assert.Throws<PzConnectorException>(() => PlanOne(new ListType(Int32Type.Default)));
        Assert.Throws<PzConnectorException>(() => PlanOne(new StructType([new Apache.Arrow.Field("a", Int32Type.Default, true)])));
        Assert.Throws<PzConnectorException>(() => PlanOne(new Decimal256Type(50, 10)));
    }
}
