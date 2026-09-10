using Apache.Arrow.Types;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxArrowChunkReaderTests
{
    private static (FakeDatabricks Fake, DbxRestClient Client) Setup()
    {
        var fake = new FakeDatabricks();
        var table = new FakeTable(("id", Int64Type.Default), ("name", StringType.Default));
        for (var i = 0; i < 120; i++) table.Rows.Add([(long)i, $"n{i}"]);
        fake.Tables["main.sales.orders"] = table;
        var errors = new List<string>();
        var cfg = DbxConnectionConfig.Parse(new ConnectorConfig(fake.ConnectionConfig()), errors)!;
        var http = new HttpClient(fake);
        return (fake, new DbxRestClient(http, cfg, new StaticTokenSource("fake-token"), cfg.Redactor, NullLogger.Instance));
    }

    private static async Task<(string Id, int Chunks)> SubmitAsync(DbxRestClient rest)
    {
        var response = await rest.SubmitStatementAsync(
            new DbxStatementRequest("abc123", "select * from `main`.`sales`.`orders`", "EXTERNAL_LINKS", "ARROW_STREAM", "50s", "CONTINUE", null, null, null),
            "ctx", CancellationToken.None);
        return (response.StatementId!, (int)response.Manifest!.TotalChunkCount!);
    }

    [Fact]
    public async Task Reads_every_batch_of_a_chunk()
    {
        var (fake, rest) = Setup();
        fake.RowsPerChunk = 50;
        var (id, chunks) = await SubmitAsync(rest);
        Assert.Equal(3, chunks);

        var rows = 0L;
        await foreach (var batch in DbxArrowChunkReader.ReadAsync(rest, id, 2, "ctx", NullLogger.Instance, CancellationToken.None))
        {
            rows += batch.Length;
            Assert.Equal("id", batch.Schema.FieldsList[0].Name);
            batch.Dispose();
        }

        Assert.Equal(20, rows);
        Assert.DoesNotContain(fake.Requests, r => r.Url.Host == "links.fake.test" && r.Headers.ContainsKey("Authorization"));
        Assert.Contains(fake.Requests, r => r.Url.Host == "links.fake.test" && r.Headers["x-fake"] == "1");
    }

    [Fact]
    public async Task An_expired_link_is_refreshed_once()
    {
        var (fake, rest) = Setup();
        fake.ExpireFirstLinkFetch = true;
        var (id, _) = await SubmitAsync(rest);

        var rows = 0L;
        await foreach (var batch in DbxArrowChunkReader.ReadAsync(rest, id, 0, "ctx", NullLogger.Instance, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        Assert.Equal(50, rows);
        Assert.Equal(2, fake.Requests.Count(r => r.Url.AbsolutePath.EndsWith("/result/chunks/0", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task A_link_that_keeps_failing_is_PZDB0204_or_transient_by_status()
    {
        var (fake, rest) = Setup();
        fake.Tables["main.sales.orders"].LinkStatus = 403;
        var (id, _) = await SubmitAsync(rest);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var b in DbxArrowChunkReader.ReadAsync(rest, id, 0, "reading orders", NullLogger.Instance, CancellationToken.None)) b.Dispose();
        });
        Assert.StartsWith("databricks: PZDB0204: reading orders: chunk 0", ex.Message);
        Assert.False(ex.IsTransient);

        fake.Tables["main.sales.orders"].LinkStatus = 503;
        (id, _) = await SubmitAsync(rest);
        ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var b in DbxArrowChunkReader.ReadAsync(rest, id, 0, "reading orders", NullLogger.Instance, CancellationToken.None)) b.Dispose();
        });
        Assert.True(ex.IsTransient);
    }
}
