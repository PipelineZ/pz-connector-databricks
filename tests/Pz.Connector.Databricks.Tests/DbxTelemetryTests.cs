using System.Diagnostics;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace Pz.Connector.Databricks.Tests;

/// <summary>What the connector reports into the ambient stats: one statement and one chunk per
/// download as extras, requests from the counting HTTP handler, and throttles that arrive with a
/// status the handler does not recognise.</summary>
public sealed class DbxTelemetryTests
{
    private static DbxConnector Connector(FakeDatabricks fake, RemoteCalls remote) =>
        new(null, TimeProvider.System, 128L * 1024 * 1024, () => new HttpClient(remote.HttpHandler(fake)));

    private static RemoteCalls NewRemote() => new(new ActivitySource("dbx-test"));

    private static FakeDatabricks TwoChunkFake()
    {
        var fake = new FakeDatabricks { RowsPerChunk = 5 };
        var table = new FakeTable(("id", Int64Type.Default));
        for (var i = 0; i < 10; i++) table.Rows.Add([(long)i]);
        fake.Tables["main.sales.orders"] = table;
        return fake;
    }

    private static DatasetSpec Spec() => new("dbx", "orders", new Dictionary<string, object?>());

    [Fact]
    public async Task Planning_a_two_chunk_read_reports_its_statements_chunks_and_every_request()
    {
        var fake = TwoChunkFake();
        var acc = new ConnectorStatsAccumulator();
        using var _ = ConnectorStats.Use(acc);
        await using var source = await ((ISourceConnector)Connector(fake, NewRemote()))
            .OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);

        var partitions = await source.PlanReadAsync(Spec(), ReadHints.None, CancellationToken.None);
        Assert.Equal(2, partitions.Count);
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        }

        var s = acc.Snapshot();
        Assert.True(s.Requests >= 4, $"requests was {s.Requests}");
        // The schema probe and the read itself are two statements.
        Assert.Contains(new KeyValuePair<string, double>("databricks.statements", 2), s.Extras);
        Assert.Contains(new KeyValuePair<string, double>("databricks.chunks", 2), s.Extras);
        Assert.Equal(0, s.Throttled);
    }

    [Fact]
    public async Task Temporarily_unavailable_on_a_503_is_counted_once_by_the_handler()
    {
        var fake = TwoChunkFake();
        fake.UnavailableNextRequests = 1;
        var acc = new ConnectorStatsAccumulator();
        using var _ = ConnectorStats.Use(acc);
        await using var source = await ((ISourceConnector)Connector(fake, NewRemote()))
            .OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.PlanReadAsync(Spec(), ReadHints.None, CancellationToken.None));

        Assert.True(ex.IsTransient);
        Assert.Equal(1, acc.Snapshot().Throttled);
    }

    [Fact]
    public async Task A_failed_statement_with_a_throttle_code_counts_one_throttle()
    {
        var fake = TwoChunkFake();
        fake.FailNextStatement = ("RESOURCE_EXHAUSTED", "too many concurrent queries");
        var acc = new ConnectorStatsAccumulator();
        using var _ = ConnectorStats.Use(acc);
        await using var source = await ((ISourceConnector)Connector(fake, NewRemote()))
            .OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);

        await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.PlanReadAsync(Spec(), ReadHints.None, CancellationToken.None));

        Assert.Equal(1, acc.Snapshot().Throttled);
    }

    [Fact]
    public void A_non_throttle_http_error_counts_no_throttle()
    {
        var acc = new ConnectorStatsAccumulator();
        using var _ = ConnectorStats.Use(acc);

        DbxErrors.FromHttp(500, "INTERNAL_ERROR", "boom", null, new DbxRedactor([]), "ctx");
        DbxErrors.FromStatement("SYNTAX_ERROR", "bad", new DbxRedactor([]), "ctx", DbxCodes.Read_ChunkDownloadFailed);

        Assert.Equal(0, acc.Snapshot().Throttled);
    }

    [Fact]
    public void A_throttle_code_arriving_with_another_status_counts_one_throttle()
    {
        var acc = new ConnectorStatsAccumulator();
        using var _ = ConnectorStats.Use(acc);

        DbxErrors.FromHttp(500, "RESOURCE_EXHAUSTED", "busy", null, new DbxRedactor([]), "ctx");

        Assert.Equal(1, acc.Snapshot().Throttled);
    }

    [Fact]
    public void Capabilities_declare_reports_stats()
    {
        Assert.True(new DbxConnector().Capabilities.HasFlag(ConnectorCapabilities.ReportsStats));
    }
}
