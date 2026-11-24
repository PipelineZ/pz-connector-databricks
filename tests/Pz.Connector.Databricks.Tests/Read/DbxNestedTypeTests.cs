using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxNestedTypeTests
{
    [Fact]
    public void An_array_of_ints_is_a_list_of_int32()
    {
        var list = Assert.IsType<ListType>(DbxNestedType.TryParse("ARRAY<SMALLINT>"));
        Assert.IsType<Int32Type>(list.ValueDataType);
    }

    [Fact]
    public void A_map_with_a_decimal_value_keeps_precision_and_scale()
    {
        var map = Assert.IsType<MapType>(DbxNestedType.TryParse("MAP<STRING,DECIMAL(12,3)>"));
        Assert.IsType<StringType>(map.KeyField.DataType);
        var value = Assert.IsType<Decimal128Type>(map.ValueField.DataType);
        Assert.Equal((12, 3), (value.Precision, value.Scale));
    }

    [Fact]
    public void A_struct_reads_names_nested_types_comments_and_not_null()
    {
        var type = DbxNestedType.TryParse(
            "STRUCT<id: BIGINT NOT NULL, `we ird`: STRING COMMENT 'it''s, odd', tags: ARRAY<STRING>, at: TIMESTAMP, d: DATE, ok: BOOLEAN, x: FLOAT>");
        var s = Assert.IsType<StructType>(type);
        Assert.Equal(["id", "we ird", "tags", "at", "d", "ok", "x"], s.Fields.Select(f => f.Name));
        Assert.IsType<Int64Type>(s.GetFieldByName("id").DataType);
        Assert.IsType<ListType>(s.GetFieldByName("tags").DataType);
        Assert.Equal("Etc/UTC", Assert.IsType<TimestampType>(s.GetFieldByName("at").DataType).Timezone);
        Assert.IsType<DoubleType>(s.GetFieldByName("x").DataType);
    }

    [Fact]
    public void Types_nest_to_any_depth()
    {
        var list = Assert.IsType<ListType>(DbxNestedType.TryParse("ARRAY<STRUCT<a: MAP<STRING,ARRAY<INT>>>>"));
        var s = Assert.IsType<StructType>(list.ValueDataType);
        Assert.IsType<MapType>(s.GetFieldByName("a").DataType);
    }

    [Theory]
    [InlineData("ARRAY<BINARY>")]
    [InlineData("ARRAY<TIMESTAMP_NTZ>")]
    [InlineData("MAP<INT,STRING>x")]
    [InlineData("MAP<ARRAY<INT>,STRING>")]
    [InlineData("STRUCT<a: INTERVAL DAY>")]
    [InlineData("ARRAY<VARIANT>")]
    [InlineData("ARRAY<INT")]
    [InlineData("STRUCT<>")]
    [InlineData("INT")]
    public void A_type_with_no_engine_representation_stays_text(string typeText)
    {
        Assert.Null(DbxNestedType.TryParse(typeText));
    }

    [Fact]
    public void ToReadSchema_declares_nested_columns_and_still_serializes_them_with_to_json()
    {
        var read = DbxTypeMap.ToReadSchema(new DbxResultSchema(3,
        [
            new DbxColumnInfo("id", "BIGINT", "LONG", 0),
            new DbxColumnInfo("tags", "ARRAY<STRING>", "ARRAY", 1),
            new DbxColumnInfo("blobs", "ARRAY<BINARY>", "ARRAY", 2),
        ]));
        Assert.IsType<ListType>(read.Schema.GetFieldByName("tags")!.DataType);
        Assert.IsType<StringType>(read.Schema.GetFieldByName("blobs")!.DataType);
        Assert.Equal("to_json(`tags`)", read.Columns[1].SerializeExpression);
        Assert.Equal("to_json(`blobs`)", read.Columns[2].SerializeExpression);
        Assert.Equal(["tags"], read.NestedColumns.Keys);
    }
}
