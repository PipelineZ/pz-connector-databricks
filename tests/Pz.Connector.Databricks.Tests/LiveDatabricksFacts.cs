using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

/// <summary>Facts against a real workspace, proving what the in-process fake cannot: the Arrow
/// types the Statement API actually emits (complex types as strings, both timestamp kinds), real
/// multi-chunk results, path reads of the staging volume, append/replace/merge against Delta, and
/// service-principal OAuth. Every fact skips unless the PZ_DATABRICKS_* variables are set, or under
/// PZ_TESTS_OFFLINE=1. Each fact owns and drops its own schema.</summary>
[Trait("Category", "LiveDatabricks")]
public sealed class LiveDatabricksFacts
{
    private static string? Env(string name) => Environment.GetEnvironmentVariable(name) is { Length: > 0 } v ? v : null;

    private static void SkipUnlessLive() =>
        Skip.If(Environment.GetEnvironmentVariable("PZ_TESTS_OFFLINE") == "1"
            || Env("PZ_DATABRICKS_HOST") is null || Env("PZ_DATABRICKS_WAREHOUSE_ID") is null || Env("PZ_DATABRICKS_TOKEN") is null
            || Env("PZ_DATABRICKS_STAGING_VOLUME") is null,
            "set PZ_DATABRICKS_HOST, PZ_DATABRICKS_WAREHOUSE_ID, PZ_DATABRICKS_TOKEN and PZ_DATABRICKS_STAGING_VOLUME to run against a workspace");

    private sealed class Live : IAsyncDisposable
    {
        public string Catalog { get; }
        public string Schema { get; } = $"pz_live_{Guid.NewGuid():N}"[..16];
        public DbxConnectionConfig Cfg { get; }
        public DbxRestClient Rest { get; }
        public Dictionary<string, object?> Config { get; }

        public Live()
        {
            var volume = Env("PZ_DATABRICKS_STAGING_VOLUME")!;
            Catalog = volume.Split('.')[0];
            Config = new Dictionary<string, object?>
            {
                ["host"] = Env("PZ_DATABRICKS_HOST"), ["warehouse_id"] = Env("PZ_DATABRICKS_WAREHOUSE_ID"),
                ["auth"] = "token", ["token"] = Env("PZ_DATABRICKS_TOKEN"), ["catalog"] = Catalog, ["schema"] = Schema,
                ["staging_volume"] = volume,
            };
            Cfg = DbxConnectionConfig.Parse(new ConnectorConfig(Config), [])!;
            var http = new HttpClient();
            Rest = new DbxRestClient(http, Cfg, DbxAuth.Create(Cfg, http, TimeProvider.System), Cfg.Redactor, NullLogger.Instance);
        }

        public async Task SqlAsync(string sql) =>
            await DbxStatement.ExecuteRowsAsync(Rest, Cfg with { Schema = null }, sql, null, "live setup", DbxCodes.Read_StatementFailed, TimeProvider.System, NullLogger.Instance, CancellationToken.None);

        public Task<string?[][]> RowsAsync(string sql) =>
            DbxStatement.ExecuteRowsAsync(Rest, Cfg, sql, null, "live read-back", DbxCodes.Read_StatementFailed, TimeProvider.System, NullLogger.Instance, CancellationToken.None);

        public Task InitAsync() => SqlAsync($"create schema `{Catalog}`.`{Schema}`");

        public async ValueTask DisposeAsync() => await SqlAsync($"drop schema if exists `{Catalog}`.`{Schema}` cascade");
    }

    private static DbxConnector Connector() => new();

    [SkippableFact]
    public async Task Types_declared_by_the_probe_match_the_batches()
    {
        SkipUnlessLive();
        await using var live = new Live();
        await live.InitAsync();
        await live.SqlAsync($"""
            create table `{live.Catalog}`.`{live.Schema}`.`types` as
            select cast(1 as tinyint) t, cast(2 as smallint) s, 3 i, cast(4 as bigint) l, true b, cast(1.5 as float) f, 2.5d d,
                   cast('12345678901234567890.123456789' as decimal(38,9)) dec, 'x' str, cast('ab' as binary) bin,
                   date '2026-09-10' dt, timestamp '2026-09-10 12:34:56.123456' ts, timestamp_ntz '2026-09-10 12:34:56.123456' ntz,
                   array(1,2) arr, map('k', 1) m, named_struct('a', 1) st
            """);

        await using var source = await ((ISourceConnector)Connector()).OpenAsync(new ConnectorConfig(live.Config), CancellationToken.None);
        var spec = new DatasetSpec("databricks", "types", new Dictionary<string, object?>());
        var declared = await source.GetSchemaAsync(spec, CancellationToken.None);
        foreach (var partition in await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None))
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                for (var i = 0; i < declared.Schema.FieldsList.Count; i++)
                {
                    Assert.Equal(declared.Schema.FieldsList[i].DataType.Name, batch.Schema.FieldsList[i].DataType.Name);
                }

                Assert.IsType<Decimal128Type>(batch.Schema.GetFieldByName("dec")!.DataType);
                Assert.IsType<StringType>(batch.Schema.GetFieldByName("arr")!.DataType);
                batch.Dispose();
            }
        }
    }

    [SkippableFact]
    public async Task A_large_result_spans_several_chunks_and_reads_back_completely()
    {
        SkipUnlessLive();
        await using var live = new Live();
        await live.InitAsync();
        await live.SqlAsync($"create table `{live.Catalog}`.`{live.Schema}`.`big` as select id, repeat('x', 200) as pad from range(0, 400000) t(id)");

        await using var source = await ((ISourceConnector)Connector()).OpenAsync(new ConnectorConfig(live.Config), CancellationToken.None);
        var spec = new DatasetSpec("databricks", "big", new Dictionary<string, object?>());
        // Pruned to just `id` (8 bytes * 400,000 rows =~ 3.2MB), the result stayed under the
        // service's chunk threshold and came back as a single chunk -- pruning is already proven
        // offline (DbxSourceBehaviorTests.Watermark_and_pruning_reach_the_statement); reading both
        // columns here (~84MB) is what actually forces a multi-chunk result.
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        Assert.True(partitions.Count > 1, $"expected several chunks, got {partitions.Count}");

        long rows = 0;
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                rows += batch.Length;
                Assert.Equal(2, batch.Schema.FieldsList.Count);
                batch.Dispose();
            }
        }

        Assert.Equal(400000, rows);
    }

    [SkippableFact]
    public async Task Watermark_parameters_filter_server_side()
    {
        SkipUnlessLive();
        await using var live = new Live();
        await live.InitAsync();
        await live.SqlAsync($"create table `{live.Catalog}`.`{live.Schema}`.`w` as select id, timestamp '2026-01-01' + make_interval(0, 0, 0, cast(id as int)) as ts from range(0, 10) t(id)");

        await using var source = await ((ISourceConnector)Connector()).OpenAsync(new ConnectorConfig(live.Config), CancellationToken.None);
        var spec = new DatasetSpec("databricks", "w", new Dictionary<string, object?>())
        {
            WatermarkCursor = "ts", WatermarkValue = "2026-01-03T00:00:00", WatermarkUpperBound = "2026-01-06T00:00:00",
        };
        var ids = new List<long>();
        foreach (var partition in await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None))
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                var col = (Int64Array)batch.Column(0);
                for (var i = 0; i < col.Length; i++) ids.Add(col.GetValue(i)!.Value);
                batch.Dispose();
            }
        }

        Assert.Equal([3, 4, 5], ids.OrderBy(i => i));
    }

    [SkippableFact]
    public async Task Append_replace_and_merge_land_in_delta_with_exact_decimals_and_both_timestamp_kinds()
    {
        SkipUnlessLive();
        await using var live = new Live();
        await live.InitAsync();
        var schema = new Schema([
            new Field("id", Int64Type.Default, true),
            new Field("amount", new Decimal128Type(38, 9), true),
            new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true),
            new Field("ntz", new TimestampType(TimeUnit.Microsecond, (string?)null), true),
            new Field("name", StringType.Default, true),
        ], null);

        static RecordBatch Batch(Schema schema, params (long Id, string Amount, string Name)[] rows)
        {
            var ids = new Int64Array.Builder();
            var amounts = new Decimal128Array.Builder(new Decimal128Type(38, 9));
            var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
            var ntz = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, (string?)null));
            var names = new StringArray.Builder();
            foreach (var (id, amount, name) in rows)
            {
                ids.Append(id);
                amounts.Append(amount);
                ts.Append(new DateTimeOffset(2026, 9, 10, 12, 34, 56, TimeSpan.Zero).AddTicks(1234560));
                ntz.Append(new DateTimeOffset(2026, 9, 10, 12, 34, 56, TimeSpan.Zero).AddTicks(1234560));
                names.Append(name);
            }

            return new RecordBatch(schema, [ids.Build(), amounts.Build(), ts.Build(), ntz.Build(), names.Build()], rows.Length);
        }

        async Task CommitAsync(string mode, string[] keys, params (long, string, string)[] rows)
        {
            await using var sink = await ((ISinkConnector)Connector()).OpenAsync(new ConnectorConfig(live.Config), CancellationToken.None);
            var spec = new OutputSpec("databricks", "target", mode, "fail_on_change", new Dictionary<string, object?>()) { Keys = keys };
            await using var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
            using (var batch = Batch(schema, rows)) await session.WriteBatchAsync(batch, CancellationToken.None);
            await session.CommitAsync(CancellationToken.None);
        }

        await CommitAsync("append", [], (1, "12345678901234567890.123456789", "a"), (2, "1.500000000", "b"));
        await CommitAsync("append", [], (3, "2.250000000", "c"));
        var rows = await live.RowsAsync("select id, cast(amount as string), typeof(ts), typeof(ntz), cast(ntz as string), name from target order by id");
        Assert.Equal(3, rows.Length);
        Assert.Equal("12345678901234567890.123456789", rows[0][1]);
        Assert.Equal("timestamp", rows[0][2]);
        Assert.Equal("timestamp_ntz", rows[0][3]);
        Assert.Equal("2026-09-10 12:34:56.123456", rows[0][4]);

        await CommitAsync("replace", [], (9, "0.000000001", "z"));  // amounts carry the (38,9) scale as nine digits
        rows = await live.RowsAsync("select id from target");
        Assert.Single(rows);

        await CommitAsync("merge", ["id"], (9, "1.000000000", "changed"), (10, "2.000000000", "new"), (10, "3.000000000", "newer"));
        rows = await live.RowsAsync("select id, name, cast(amount as string) from target order by id");
        Assert.Equal(2, rows.Length);
        Assert.Equal("changed", rows[0][1]);
        Assert.Equal("newer", rows[1][1]);
        Assert.Equal("3.000000000", rows[1][2]);

        var leftovers = await live.RowsAsync($"list '{live.Cfg.StagingVolume!.Value.VolumePath}/pz/'");
        Assert.Empty(leftovers);
    }

    // A session that never calls WriteBatchAsync stages a schema-only Parquet file (no row groups)
    // so the target statement's `parquet.`<dir>/`` source always reads a real, uploaded directory --
    // DbxParquetSpool.CloseAsync's fallback, landed on this branch during this task's own live run
    // as the task 14 review's fix for the failure this suite was written to catch (a directory that
    // was never created is not an empty scan but a Databricks path-not-found error). All three facts
    // below pass with that fix in place; see the report for the timeline.
    [SkippableFact]
    public async Task Empty_append_is_a_no_op()
    {
        SkipUnlessLive();
        await using var live = new Live();
        await live.InitAsync();
        var schema = new Schema([new Field("id", Int64Type.Default, true), new Field("name", StringType.Default, true)], null);

        await using (var sink = await ((ISinkConnector)Connector()).OpenAsync(new ConnectorConfig(live.Config), CancellationToken.None))
        {
            var spec = new OutputSpec("databricks", "empty_append", "append", "fail_on_change", new Dictionary<string, object?>());
            await using var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
            await session.CommitAsync(CancellationToken.None);
        }

        var rows = await live.RowsAsync("select id from empty_append");
        Assert.Empty(rows);
    }

    [SkippableFact]
    public async Task Empty_replace_leaves_an_empty_table_with_the_columns()
    {
        SkipUnlessLive();
        await using var live = new Live();
        await live.InitAsync();
        var schema = new Schema([new Field("id", Int64Type.Default, true), new Field("name", StringType.Default, true)], null);

        await using (var sink = await ((ISinkConnector)Connector()).OpenAsync(new ConnectorConfig(live.Config), CancellationToken.None))
        {
            var spec = new OutputSpec("databricks", "empty_replace", "replace", "fail_on_change", new Dictionary<string, object?>());
            await using var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
            await session.CommitAsync(CancellationToken.None);
        }

        var rows = await live.RowsAsync("select id from empty_replace");
        Assert.Empty(rows);

        var described = await live.RowsAsync("describe table empty_replace");
        var names = described.Where(r => r[0] is { Length: > 0 } n && !n.StartsWith('#')).Select(r => r[0]!).ToArray();
        Assert.Equal(["id", "name"], names);
    }

    [SkippableFact]
    public async Task Empty_merge_is_a_no_op()
    {
        SkipUnlessLive();
        await using var live = new Live();
        await live.InitAsync();
        var schema = new Schema([new Field("id", Int64Type.Default, true), new Field("name", StringType.Default, true)], null);

        await using (var sink = await ((ISinkConnector)Connector()).OpenAsync(new ConnectorConfig(live.Config), CancellationToken.None))
        {
            var spec = new OutputSpec("databricks", "empty_merge", "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] };
            await using var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
            await session.CommitAsync(CancellationToken.None);
        }

        var rows = await live.RowsAsync("select id from empty_merge");
        Assert.Empty(rows);
    }

    [SkippableFact]
    public async Task Service_principal_oauth_authenticates()
    {
        SkipUnlessLive();
        Skip.If(Env("PZ_DATABRICKS_CLIENT_ID") is null || Env("PZ_DATABRICKS_CLIENT_SECRET") is null,
            "set PZ_DATABRICKS_CLIENT_ID and PZ_DATABRICKS_CLIENT_SECRET for the oauth fact");
        var config = new Dictionary<string, object?>
        {
            ["host"] = Env("PZ_DATABRICKS_HOST"), ["warehouse_id"] = Env("PZ_DATABRICKS_WAREHOUSE_ID"), ["auth"] = "oauth",
            ["client_id"] = Env("PZ_DATABRICKS_CLIENT_ID"), ["client_secret"] = Env("PZ_DATABRICKS_CLIENT_SECRET"),
        };

        var check = await Connector().CheckConnectionAsync(new ConnectorConfig(config), CancellationToken.None);

        Assert.True(check.Ok, check.Message);
    }

    [SkippableFact]
    public async Task Check_connection_reports_the_warehouse()
    {
        SkipUnlessLive();
        await using var live = new Live();
        var check = await Connector().CheckConnectionAsync(new ConnectorConfig(live.Config), CancellationToken.None);
        Assert.True(check.Ok, check.Message);
        Assert.StartsWith("warehouse ", check.Message);
    }
}
