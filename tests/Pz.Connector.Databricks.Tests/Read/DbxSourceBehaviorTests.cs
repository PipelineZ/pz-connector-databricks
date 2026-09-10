using Apache.Arrow.Types;
using Microsoft.Extensions.Time.Testing;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxSourceBehaviorTests
{
    private static FakeDatabricks NewFake()
    {
        var fake = new FakeDatabricks();
        var table = new FakeTable(("id", Int64Type.Default), ("name", StringType.Default));
        for (var i = 0; i < 10; i++) table.Rows.Add([(long)i, $"n{i}"]);
        fake.Tables["main.sales.orders"] = table;
        return fake;
    }

    private static DbxConnector Connector(FakeDatabricks fake, TimeProvider? time = null) =>
        new(null, time ?? TimeProvider.System, 128L * 1024 * 1024, () => new HttpClient(fake, disposeHandler: false));

    private static DatasetSpec Spec(string dataset, params (string, object?)[] options) =>
        new("dbx", dataset, options.ToDictionary(o => o.Item1, o => o.Item2, StringComparer.Ordinal));

    private static async Task<List<long>> ReadIdsAsync(ISource source, DatasetSpec spec, ReadHints? hints = null)
    {
        var ids = new List<long>();
        foreach (var partition in await source.PlanReadAsync(spec, hints ?? ReadHints.None, CancellationToken.None))
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                var col = (Apache.Arrow.Int64Array)batch.Column(0);
                for (var i = 0; i < col.Length; i++) ids.Add(col.GetValue(i)!.Value);
                batch.Dispose();
            }
        }

        return ids;
    }

    [Fact]
    public async Task Schema_comes_from_a_limit_0_probe_and_is_cached()
    {
        var fake = NewFake();
        await using var source = await ((ISourceConnector)Connector(fake)).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);

        var schema = await source.GetSchemaAsync(Spec("orders"), CancellationToken.None);
        await source.GetSchemaAsync(Spec("orders"), CancellationToken.None);

        Assert.Equal(["id", "name"], schema.Schema.FieldsList.Select(f => f.Name));
        Assert.IsType<Int64Type>(schema.Schema.FieldsList[0].DataType);
        Assert.Single(fake.Statements);
        Assert.Equal("select * from (select * from `main`.`sales`.`orders`) as pz_probe limit 0", fake.Statements[0]);
    }

    [Fact]
    public async Task One_partition_per_chunk_and_pending_statements_are_polled()
    {
        var fake = NewFake();
        fake.RowsPerChunk = 4;
        var time = new FakeTimeProvider();
        await using var source = await ((ISourceConnector)Connector(fake, time)).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);
        await source.GetSchemaAsync(Spec("orders"), CancellationToken.None);
        fake.PendingPollsBeforeSuccess = 2;

        var planTask = source.PlanReadAsync(Spec("orders"), ReadHints.None, CancellationToken.None).AsTask();
        for (var i = 0; i < 4 && !planTask.IsCompleted; i++)
        {
            for (var y = 0; y < 20; y++) await Task.Yield();
            time.Advance(TimeSpan.FromSeconds(5));
        }

        var partitions = await planTask;
        Assert.Equal(3, partitions.Count);
    }

    [Fact]
    public async Task Watermark_and_pruning_reach_the_statement()
    {
        var fake = NewFake();
        await using var source = await ((ISourceConnector)Connector(fake)).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);
        var spec = Spec("orders") with { WatermarkCursor = "id", WatermarkValue = "3", WatermarkUpperBound = "7" };

        var ids = await ReadIdsAsync(source, spec, new ReadHints(Columns: ["id"]));

        Assert.Equal([4, 5, 6, 7], ids);
        Assert.Equal("select `id` from `main`.`sales`.`orders` where (`id` > :pz_lower) and (`id` <= :pz_upper)", fake.Statements[^1]);
        Assert.Equal("BIGINT", fake.StatementParameters[^1]![0].Type);
    }

    [Fact]
    public async Task Query_mode_runs_verbatim()
    {
        var fake = NewFake();
        fake.Queries["select id, name from sales.orders where id < 3"] = "main.sales.orders";
        await using var source = await ((ISourceConnector)Connector(fake)).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);

        var ids = await ReadIdsAsync(source, Spec("recent", ("query", "select id, name from sales.orders where id < 3")));

        Assert.Equal(10, ids.Count); // the fake serves the aliased table whole; what matters is the SQL went verbatim
        Assert.Equal("select id, name from sales.orders where id < 3", fake.Statements[^1]);
        Assert.Null(fake.StatementParameters[^1]);
    }

    [Fact]
    public async Task A_failed_statement_is_PZDB0203_naming_the_dataset()
    {
        var fake = NewFake();
        await using var source = await ((ISourceConnector)Connector(fake)).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);
        await source.GetSchemaAsync(Spec("orders"), CancellationToken.None);
        fake.FailNextStatement = ("BAD_REQUEST", "[TABLE_OR_VIEW_NOT_FOUND] boom");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => source.PlanReadAsync(Spec("orders"), ReadHints.None, CancellationToken.None).AsTask());

        Assert.StartsWith("databricks: PZDB0203: reading dataset 'orders' (main.sales.orders): statement failed (BAD_REQUEST): [TABLE_OR_VIEW_NOT_FOUND] boom", ex.Message);
    }

    [Fact]
    public async Task Bad_dataset_options_are_PZDB0201_or_entity_errors()
    {
        var fake = NewFake();
        await using var source = await ((ISourceConnector)Connector(fake)).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => source.GetSchemaAsync(Spec("x", ("entity", "a"), ("query", "b")), CancellationToken.None).AsTask());
        Assert.Contains("PZDB0201", ex.Message);

        ex = await Assert.ThrowsAsync<PzConnectorException>(() => source.GetSchemaAsync(Spec("a.b.c.d"), CancellationToken.None).AsTask());
        Assert.Contains("PZDB0104", ex.Message);
    }

    [Fact]
    public async Task A_rejected_token_is_PZDB0401()
    {
        var fake = NewFake();
        fake.RejectToken = true;
        await using var source = await ((ISourceConnector)Connector(fake)).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => source.GetSchemaAsync(Spec("orders"), CancellationToken.None).AsTask());
        Assert.StartsWith("databricks: PZDB0401: ", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task Oauth_connection_mints_a_token_through_the_fake()
    {
        var fake = NewFake();
        fake.OAuthClient = ("sp-id", "sp-secret");
        var config = fake.ConnectionConfig();
        config.Remove("token");
        config["auth"] = "oauth";
        config["client_id"] = "sp-id";
        config["client_secret"] = "sp-secret";
        await using var source = await ((ISourceConnector)Connector(fake)).OpenAsync(new ConnectorConfig(config), CancellationToken.None);

        var schema = await source.GetSchemaAsync(Spec("orders"), CancellationToken.None);

        Assert.Equal(2, schema.Schema.FieldsList.Count);
        Assert.Contains(fake.Requests, r => r.Url.AbsolutePath == "/oidc/v1/token");
    }

    [Theory]
    [InlineData("RUNNING", true, "warehouse fake-wh RUNNING")]
    [InlineData("STOPPED", true, "warehouse fake-wh is STOPPED; it starts on the first statement")]
    [InlineData("DELETED", false, "warehouse fake-wh is DELETED")]
    public async Task CheckConnection_reports_the_warehouse_state(string state, bool ok, string message)
    {
        var fake = NewFake();
        fake.WarehouseState = state;

        var check = await Connector(fake).CheckConnectionAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);

        Assert.Equal(ok, check.Ok);
        Assert.StartsWith(message, check.Message);
        if (state == "RUNNING")
        {
            Assert.Contains("select 1", fake.Statements);
        }
    }

    [Fact]
    public async Task Disposing_a_source_disposes_the_http_client_it_was_opened_with()
    {
        var fake = NewFake();
        var tracker = new TrackingHandler(fake);
        var connector = new DbxConnector(null, TimeProvider.System, 128L * 1024 * 1024, () => new HttpClient(tracker));
        var source = await ((ISourceConnector)connector).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);
        await source.GetSchemaAsync(Spec("orders"), CancellationToken.None);
        Assert.False(tracker.Disposed);

        await source.DisposeAsync();

        Assert.True(tracker.Disposed);
    }

    [Fact]
    public async Task CheckConnection_reports_config_and_auth_failures_without_throwing()
    {
        var fake = NewFake();
        var bad = await Connector(fake).CheckConnectionAsync(new ConnectorConfig(new Dictionary<string, object?>()), CancellationToken.None);
        Assert.False(bad.Ok);
        Assert.Contains("'host' is required", bad.Message);

        fake.RejectToken = true;
        var unauthorized = await Connector(fake).CheckConnectionAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);
        Assert.False(unauthorized.Ok);
        Assert.Contains("PZDB0401", unauthorized.Message);
    }

    /// <summary>Records that the <see cref="HttpClient"/> wrapping it was disposed -- the only way to
    /// observe from outside that the source closed the client it was handed.</summary>
    private sealed class TrackingHandler(HttpMessageHandler inner) : DelegatingHandler(inner)
    {
        public bool Disposed { get; private set; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Disposed = true;
            }

            base.Dispose(disposing);
        }
    }
}
