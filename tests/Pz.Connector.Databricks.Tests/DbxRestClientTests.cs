using System.Net;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxRestClientTests
{
    private static readonly DbxConnectionConfig Cfg =
        new(new Uri("https://ws.example/"), "wh1", DbxAuthKind.Token, "dapi-secret", null, null, null, null, null, new DbxRedactor(["dapi-secret"]));

    private static DbxRestClient Client(FakeHandler handler) =>
        new(new HttpClient(handler), Cfg, new StaticTokenSource("dapi-secret"), Cfg.Redactor, NullLogger.Instance);

    [Fact]
    public async Task Submit_posts_snake_case_json_with_a_bearer_token()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/api/2.0/sql/statements", 200, """{"statement_id":"s1","status":{"state":"PENDING"}}""");
        var client = Client(handler);

        var response = await client.SubmitStatementAsync(
            new DbxStatementRequest("wh1", "select 1", "INLINE", "JSON_ARRAY", "50s", "CONTINUE", "main", null,
                [new DbxParameter("pz_lower", "3", "BIGINT")]),
            "ctx", CancellationToken.None);

        Assert.Equal("s1", response.StatementId);
        Assert.Equal("PENDING", response.Status?.State);
        var req = Assert.Single(handler.Requests);
        Assert.Equal("Bearer dapi-secret", req.Headers["Authorization"]);
        Assert.Contains("\"warehouse_id\":\"wh1\"", req.Body);
        Assert.Contains("\"wait_timeout\":\"50s\"", req.Body);
        Assert.Contains("\"on_wait_timeout\":\"CONTINUE\"", req.Body);
        Assert.Contains("\"catalog\":\"main\"", req.Body);
        Assert.DoesNotContain("\"schema\"", req.Body);
        Assert.Contains("\"parameters\":[{\"name\":\"pz_lower\",\"value\":\"3\",\"type\":\"BIGINT\"}]", req.Body);
    }

    [Fact]
    public async Task Non_2xx_is_classified_with_the_envelope_and_retry_after()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/api/2.0/sql/statements/s1", 429,
            """{"error_code":"RESOURCE_EXHAUSTED","message":"slow down dapi-secret"}""", new Dictionary<string, string> { ["Retry-After"] = "5" });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Client(handler).GetStatementAsync("s1", "polling orders", CancellationToken.None));

        Assert.Equal("databricks: PZDB0402: polling orders: HTTP 429 (RESOURCE_EXHAUSTED): slow down ***", ex.Message);
        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(5), ex.RetryAfter);
    }

    [Fact]
    public async Task GetTable_returns_null_on_404_and_columns_otherwise()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/api/2.1/unity-catalog/tables/main.sales.orders", 200,
            """{"name":"orders","full_name":"main.sales.orders","table_type":"MANAGED","columns":[{"name":"id","type_text":"bigint","type_name":"LONG","position":0}]}""");
        var client = Client(handler);

        var table = await client.GetTableAsync(new TableRef("main", "sales", "orders"), "ctx", CancellationToken.None);
        Assert.Equal("bigint", table!.Columns![0].TypeText);

        Assert.Null(await client.GetTableAsync(new TableRef("main", "sales", "missing"), "ctx", CancellationToken.None));
    }

    [Fact]
    public async Task Upload_puts_octet_stream_with_overwrite()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Put, "/api/2.0/fs/files/Volumes/main/pz/staging/pz/t1/part-00000.parquet?overwrite=true", 204, "");
        var bytes = Encoding.UTF8.GetBytes("PAR1");

        await Client(handler).UploadFileAsync("/Volumes/main/pz/staging/pz/t1/part-00000.parquet", new MemoryStream(bytes), bytes.Length, "ctx", CancellationToken.None);

        var req = Assert.Single(handler.Requests);
        Assert.Equal("application/octet-stream", req.Headers["Content-Type"]);
        Assert.Equal(bytes, req.BodyBytes);
    }

    [Fact]
    public async Task Delete_tolerates_404()
    {
        var handler = new FakeHandler();
        await Client(handler).DeleteFileAsync("/Volumes/a/b/c/x", CancellationToken.None);
        await Client(handler).DeleteDirectoryAsync("/Volumes/a/b/c", CancellationToken.None);
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task External_link_get_sends_no_authorization_header_and_returns_the_raw_response()
    {
        var handler = new FakeHandler();
        handler.AddBytes(HttpMethod.Get, "/chunk/0?X-Amz-Signature=abc", 200, [1, 2, 3], "application/octet-stream");

        using var response = await Client(handler).OpenExternalLinkAsync("https://bucket.s3.amazonaws.com/chunk/0?X-Amz-Signature=abc",
            new Dictionary<string, string> { ["x-ms-blob-type"] = "BlockBlob" }, "ctx", CancellationToken.None);

        var req = Assert.Single(handler.Requests);
        Assert.False(req.Headers.ContainsKey("Authorization"));
        Assert.Equal("BlockBlob", req.Headers["x-ms-blob-type"]);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(new byte[] { 1, 2, 3 }, await response.Content.ReadAsByteArrayAsync());
    }

    [Fact]
    public async Task Transport_failure_is_transient_and_cancellation_propagates()
    {
        var handler = new FakeHandler { Interceptor = _ => throw new HttpRequestException("connection refused") };
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Client(handler).GetWarehouseAsync("checking", CancellationToken.None));
        Assert.StartsWith("databricks: PZDB0402: checking: connection refused", ex.Message);
        Assert.True(ex.IsTransient);

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var cancelling = new FakeHandler { Interceptor = _ => throw new OperationCanceledException(cts.Token) };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Client(cancelling).GetWarehouseAsync("checking", cts.Token));
    }

    [Fact]
    public async Task Unparsable_2xx_body_is_a_permanent_PZDB0402()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/api/2.0/sql/warehouses/wh1", 200, "<html/>");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Client(handler).GetWarehouseAsync("checking", CancellationToken.None));
        Assert.Contains("response body did not parse", ex.Message);
        Assert.False(ex.IsTransient);
    }
}
