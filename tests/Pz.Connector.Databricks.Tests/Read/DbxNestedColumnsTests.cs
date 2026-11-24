using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxNestedColumnsTests
{
    private static RecordBatch Text(params string?[] json)
    {
        var ids = new Int64Array.Builder();
        var text = new StringArray.Builder();
        for (var i = 0; i < json.Length; i++)
        {
            ids.Append(i);
            text.Append(json[i]);
        }

        var schema = new Schema([new Field("id", Int64Type.Default, true), new Field("v", StringType.Default, true)], null);
        return new RecordBatch(schema, [ids.Build(), text.Build()], json.Length);
    }

    private static IReadOnlyDictionary<string, IArrowType> Nested(string typeText) =>
        new Dictionary<string, IArrowType> { ["v"] = DbxNestedType.TryParse(typeText)! };

    [Fact]
    public void A_list_column_becomes_an_arrow_list_and_other_columns_pass_through()
    {
        using var batch = DbxNestedColumns.Convert(Text("[1,2]", null, "[]", "[null,4]"), Nested("ARRAY<INT>"), "ctx");
        Assert.Equal(4, batch.Length);
        Assert.IsType<Int64Array>(batch.Column(0));
        var list = Assert.IsType<ListArray>(batch.Column(1));
        Assert.True(list.IsNull(1));
        var values = Assert.IsType<Int32Array>(list.Values);
        Assert.Equal([1, 2], Enumerable.Range(list.ValueOffsets[0], 2).Select(i => values.GetValue(i)!.Value));
        Assert.Equal(0, list.GetValueLength(2));
        Assert.True(values.IsNull(list.ValueOffsets[3]));
        Assert.IsType<ListType>(batch.Schema.GetFieldByName("v").DataType);
    }

    [Fact]
    public void A_struct_with_a_map_and_a_timestamp_round_trips()
    {
        var type = "STRUCT<n: INT, at: TIMESTAMP, m: MAP<STRING,BIGINT>, d: DATE>";
        using var batch = DbxNestedColumns.Convert(
            Text("""{"n":7,"at":"2024-05-06T07:08:09.123456Z","m":{"a":1,"b":null},"d":"2024-05-06"}""", """{"n":null}"""),
            Nested(type), "ctx");
        var s = Assert.IsType<StructArray>(batch.Column(1));
        Assert.Equal(7, ((Int32Array)s.Fields[0]).GetValue(0));
        Assert.True(((Int32Array)s.Fields[0]).IsNull(1));
        var at = (TimestampArray)s.Fields[1];
        Assert.Equal(new DateTimeOffset(2024, 5, 6, 7, 8, 9, TimeSpan.Zero).AddTicks(1234560), at.GetTimestamp(0));
        Assert.Equal("a", ((StringArray)((MapArray)s.Fields[2]).KeyValues.Fields[0]).GetString(0));
        Assert.Equal(new DateOnly(2024, 5, 6), ((Date32Array)s.Fields[3]).GetDateOnly(0));
    }

    [Fact]
    public void A_pruned_batch_missing_the_nested_column_is_returned_as_is()
    {
        using var batch = Text("1");
        var other = new Dictionary<string, IArrowType> { ["gone"] = new ListType(Int32Type.Default) };
        Assert.Same(batch, DbxNestedColumns.Convert(batch, other, "ctx"));
    }

    [Fact]
    public void A_value_of_the_wrong_shape_names_the_column_and_row_but_never_the_value()
    {
        var ex = Assert.Throws<PzConnectorException>(() => DbxNestedColumns.Convert(Text("[1]", "{\"secret\":1}"), Nested("ARRAY<INT>"), "reading dataset 'd'"));
        Assert.Contains("PZDB0207", ex.Message);
        Assert.Contains("column 'v' row 2", ex.Message);
        Assert.DoesNotContain("secret", ex.Message);
    }
}
