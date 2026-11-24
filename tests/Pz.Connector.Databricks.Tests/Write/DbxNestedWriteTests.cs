using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxNestedWriteTests
{
    private static readonly ListType Tags = new(new Apache.Arrow.Field("item", StringType.Default, true));

    private static readonly StructType Rec = new(
    [
        new Apache.Arrow.Field("n", Int32Type.Default, true),
        new Apache.Arrow.Field("at", new TimestampType(TimeUnit.Microsecond, "UTC"), true),
        new Apache.Arrow.Field("we`ird", new Decimal128Type(38, 18), true),
    ]);

    private static DbxColumnPlan PlanOne(IArrowType type) =>
        Assert.Single(DbxSchemaMap.Plan(new Schema([new Apache.Arrow.Field("c", type, true)], null), "out"));

    [Fact]
    public void Nested_arrow_types_map_to_databricks_ddl_and_a_from_json_read()
    {
        var list = PlanOne(Tags);
        Assert.Equal("ARRAY<STRING>", list.DatabricksType);
        Assert.Equal("from_json(`c`, 'ARRAY<STRING>') as `c`", DbxSchemaMap.SelectExpression(list));

        Assert.Equal("ARRAY<INT>", PlanOne(new FixedSizeListType(new Apache.Arrow.Field("item", Int32Type.Default, true), 2)).DatabricksType);
        Assert.Equal("MAP<STRING, ARRAY<BIGINT>>",
            PlanOne(new MapType(StringType.Default, new ListType(new Apache.Arrow.Field("item", Int64Type.Default, true)))).DatabricksType);
        Assert.Equal("STRUCT<`n`: INT, `at`: TIMESTAMP, `we``ird`: DECIMAL(38,18)>", PlanOne(Rec).DatabricksType);
    }

    [Fact]
    public void A_quote_or_backslash_in_the_type_is_escaped_in_the_from_json_literal()
    {
        var plan = PlanOne(new StructType([new Apache.Arrow.Field("it's\\", Int32Type.Default, true)]));
        Assert.Equal("from_json(`c`, 'STRUCT<`it\\'s\\\\`: INT>') as `c`", DbxSchemaMap.SelectExpression(plan));
    }

    [Fact]
    public void Map_keys_other_than_varchar_and_leaves_outside_the_flat_set_are_PZDB0303()
    {
        var intKeys = Assert.Throws<PzConnectorException>(() => PlanOne(new MapType(Int32Type.Default, StringType.Default)));
        Assert.Contains("PZDB0303", intKeys.Message);
        Assert.Contains("'c'", intKeys.Message);
        Assert.Throws<PzConnectorException>(() => PlanOne(new ListType(new Apache.Arrow.Field("item", BinaryType.Default, true))));
    }

    [Fact]
    public void The_create_statement_declares_the_nested_column_types()
    {
        var target = new TableRef("main", "sales", "t");
        var sql = DbxSql.CreateIfNotExists(target, DbxSchemaMap.Plan(new Schema([new Apache.Arrow.Field("rec", Rec, true), new Apache.Arrow.Field("tags", Tags, true)], null), "out"));
        Assert.Equal("create table if not exists `main`.`sales`.`t` (`rec` STRUCT<`n`: INT, `at`: TIMESTAMP, `we``ird`: DECIMAL(38,18)>, `tags` ARRAY<STRING>)", sql);
    }

    [Fact]
    public async Task Nested_cells_are_spooled_as_json_text_and_the_insert_rebuilds_them()
    {
        var tags = new ListArray.Builder(StringType.Default);
        var values = (StringArray.Builder)tags.ValueBuilder;
        tags.Append();
        values.Append("a").Append("b");
        tags.AppendNull();
        var schema = new Schema([new Apache.Arrow.Field("tags", Tags, true)], null);
        using var batch = new RecordBatch(schema, [tags.Build()], 2);

        var columns = DbxSchemaMap.Plan(schema, "out");
        var dir = Path.Combine(Path.GetTempPath(), "pz-dbx-nested-" + Guid.NewGuid().ToString("N"));
        var spool = new DbxParquetSpool(dir, columns, withSequence: false, rollBytes: 1L << 30);
        try
        {
            await spool.WriteBatchAsync(batch, 0, CancellationToken.None);
            var file = Assert.Single(await spool.CloseAsync());

            await using var stream = File.OpenRead(file);
            await using var reader = await ParquetReader.CreateAsync(stream);
            using var group = reader.OpenRowGroupReader(0);
            var field = reader.Schema.DataFields[0];
            Assert.Equal(typeof(string), field.ClrType);
            var read = new string?[2];
            await group.ReadAsync(field, read.AsMemory(), null);
            Assert.Equal("[\"a\",\"b\"]", read[0]);
            Assert.Null(read[1]);
        }
        finally
        {
            spool.Delete();
        }

        var sql = DbxSql.Append(new TableRef("main", "sales", "t"), columns, "/Volumes/v/pz/tag");
        Assert.Contains("select from_json(`tags`, 'ARRAY<STRING>') as `tags` from parquet.`/Volumes/v/pz/tag/`", sql);
    }

    [Theory]
    [InlineData("rec")]
    [InlineData("tags")]
    public async Task A_nested_merge_key_is_refused_naming_the_column_before_any_request(string key)
    {
        var fake = new FakeDatabricks();
        var connector = new DbxConnector(null, TimeProvider.System, 128L * 1024 * 1024, () => new HttpClient(fake, disposeHandler: false));
        await using var sink = await ((ISinkConnector)connector).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);
        var schema = new Schema([new Apache.Arrow.Field("rec", Rec, true), new Apache.Arrow.Field("tags", Tags, true)], null);
        var spec = new OutputSpec("dbx", "out", "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = [key] };

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => sink.BeginWriteAsync(spec, schema, CancellationToken.None).AsTask());
        Assert.Contains("PZDB0302", ex.Message);
        Assert.Contains($"'{key}'", ex.Message);
        Assert.Contains("a merge key must be a flat value", ex.Message);
        Assert.Empty(fake.Requests);
    }

    [Fact]
    public async Task A_flat_merge_key_next_to_nested_columns_is_accepted()
    {
        var fake = new FakeDatabricks();
        var connector = new DbxConnector(null, TimeProvider.System, 128L * 1024 * 1024, () => new HttpClient(fake, disposeHandler: false));
        await using var sink = await ((ISinkConnector)connector).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);
        var schema = new Schema([new Apache.Arrow.Field("id", Int64Type.Default, true), new Apache.Arrow.Field("tags", Tags, true)], null);
        var spec = new OutputSpec("dbx", "out", "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] };
        await using var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
        await session.AbortAsync(CancellationToken.None);
    }
}
