using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxStatementTests
{
    private static readonly DbxConnectionConfig Cfg =
        new(new Uri("https://ws.example/"), "wh1", DbxAuthKind.Token, "t", null, null, "main", "sales", null, DbxRedactor.None);

    private static DbxRestClient Client(FakeHandler handler) =>
        new(new HttpClient(handler), Cfg, new StaticTokenSource("t"), DbxRedactor.None, NullLogger.Instance);

    private static string Status(string state, string? errorCode = null, string? message = null) =>
        // Four '$'/four-brace holes: the JSON below ends in runs of three consecutive closing braces,
        // which two-brace interpolation cannot disambiguate from literal content (CS9007).
        errorCode is null
            ? $$$$"""{"statement_id":"s1","status":{"state":"{{{{state}}}}"},"manifest":{"total_chunk_count":0,"schema":{"column_count":0,"columns":[]}}}"""
            : $$$$"""{"statement_id":"s1","status":{"state":"{{{{state}}}}","error":{"error_code":"{{{{errorCode}}}}","message":"{{{{message}}}}"}}}""";

    /// <summary>Serves the submit body once, then each poll body in order; the last poll body repeats.
    /// Signals a gate after each poll so tests wait on a real event, never on wall-clock time.</summary>
    private sealed class SequenceHandler : FakeHandler
    {
        private readonly Queue<string> _polls;
        private string _last;
        private readonly SemaphoreSlim _polled = new(0);

        public int PollCalls { get; private set; }

        public SequenceHandler(string submitBody, params string[] pollBodies)
        {
            _polls = new Queue<string>(pollBodies);
            _last = pollBodies[^1];
            Add(HttpMethod.Post, "/api/2.0/sql/statements", 200, submitBody);
            Interceptor = request =>
            {
                if (request.Method != HttpMethod.Get || !request.RequestUri!.AbsolutePath.EndsWith("/s1", StringComparison.Ordinal))
                {
                    return null;
                }

                PollCalls++;
                var body = _polls.Count > 0 ? _polls.Dequeue() : _last;
                _last = body;
                _polled.Release();
                return new HttpResponseMessage(System.Net.HttpStatusCode.OK) { Content = new StringContent(body, System.Text.Encoding.UTF8, "application/json") };
            };
        }

        public Task WaitForPollAsync(int n) => Task.Run(async () =>
        {
            while (PollCalls < n)
            {
                await _polled.WaitAsync();
            }
        });
    }

    /// <summary>Lets the poll loop's continuation reach its Task.Delay registration before the
    /// fake clock advances -- a timer registered after the advance would never see it.</summary>
    private static async Task LetPendingContinuationsRunAsync()
    {
        for (var i = 0; i < 20; i++)
        {
            await Task.Yield();
        }
    }

    [Fact]
    public async Task Execute_returns_immediately_when_the_submit_already_succeeded()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/api/2.0/sql/statements", 200, Status("SUCCEEDED"));

        var response = await DbxStatement.ExecuteAsync(Client(handler), Cfg, "select 1", null, arrow: false, "ctx", DbxCodes.Read_StatementFailed,
            new FakeTimeProvider(), NullLogger.Instance, CancellationToken.None);

        Assert.Equal("SUCCEEDED", response.Status?.State);
        var req = Assert.Single(handler.Requests);
        Assert.Contains("\"disposition\":\"INLINE\"", req.Body);
        Assert.Contains("\"format\":\"JSON_ARRAY\"", req.Body);
        Assert.Contains("\"catalog\":\"main\"", req.Body);
        Assert.Contains("\"schema\":\"sales\"", req.Body);
    }

    [Fact]
    public async Task Execute_polls_with_the_backoff_schedule_until_terminal()
    {
        var time = new FakeTimeProvider();
        var handler = new SequenceHandler(Status("PENDING"), Status("RUNNING"), Status("RUNNING"), Status("SUCCEEDED"));

        var task = DbxStatement.ExecuteAsync(Client(handler), Cfg, "select 1", null, arrow: true, "ctx", DbxCodes.Read_StatementFailed,
            time, NullLogger.Instance, CancellationToken.None);

        // Poll #1 goes out immediately after the submit (the server-side wait already happened).
        await handler.WaitForPollAsync(1);

        // The 500 ms delay before poll #2: prove both edges -- 499 ms is not enough, the next 1 ms is.
        await LetPendingContinuationsRunAsync();
        time.Advance(TimeSpan.FromMilliseconds(499));
        await LetPendingContinuationsRunAsync();
        Assert.Equal(1, handler.PollCalls);
        time.Advance(TimeSpan.FromMilliseconds(1));
        await handler.WaitForPollAsync(2);

        // The 1 s delay before poll #3.
        await LetPendingContinuationsRunAsync();
        time.Advance(TimeSpan.FromMilliseconds(999));
        await LetPendingContinuationsRunAsync();
        Assert.Equal(2, handler.PollCalls);
        time.Advance(TimeSpan.FromMilliseconds(1));
        await handler.WaitForPollAsync(3);

        var response = await task;
        Assert.Equal("SUCCEEDED", response.Status?.State);
        Assert.Contains("\"format\":\"ARROW_STREAM\"", handler.Requests[0].Body);
    }

    [Fact]
    public async Task Execute_throws_the_caller_code_for_a_failed_statement()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/api/2.0/sql/statements", 200, Status("FAILED", "BAD_REQUEST", "[PARSE_SYNTAX_ERROR] near x"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => DbxStatement.ExecuteAsync(Client(handler), Cfg, "selec 1", null, false,
            "reading orders", DbxCodes.Write_TargetStatementFailed, new FakeTimeProvider(), NullLogger.Instance, CancellationToken.None));

        Assert.Equal("databricks: PZDB0306: reading orders: statement failed (BAD_REQUEST): [PARSE_SYNTAX_ERROR] near x", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task Execute_cancels_the_statement_when_the_caller_cancels_mid_poll()
    {
        var time = new FakeTimeProvider();
        var handler = new SequenceHandler(Status("PENDING"), Status("RUNNING"));
        handler.Add(HttpMethod.Post, "/api/2.0/sql/statements/s1/cancel", 200, "{}");
        using var cts = new CancellationTokenSource();

        var task = DbxStatement.ExecuteAsync(Client(handler), Cfg, "select 1", null, false, "ctx", DbxCodes.Read_StatementFailed,
            time, NullLogger.Instance, cts.Token);
        await handler.WaitForPollAsync(1);
        await LetPendingContinuationsRunAsync();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
        Assert.Contains(handler.Requests, r => r.Url.AbsolutePath == "/api/2.0/sql/statements/s1/cancel");
    }

    [Fact]
    public async Task ExecuteRows_returns_the_inline_data_array()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/api/2.0/sql/statements", 200,
            """{"statement_id":"s1","status":{"state":"SUCCEEDED"},"result":{"data_array":[["1","a"],["2",null]]}}""");

        var rows = await DbxStatement.ExecuteRowsAsync(Client(handler), Cfg, "select 1", null, "ctx", DbxCodes.Read_StatementFailed,
            new FakeTimeProvider(), NullLogger.Instance, CancellationToken.None);

        Assert.Equal(2, rows.Length);
        Assert.Equal("1", rows[0][0]);
        Assert.Null(rows[1][1]);
    }

    [Fact]
    public async Task GetLink_returns_the_first_link_or_PZDB0204()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/api/2.0/sql/statements/s1/result/chunks/2", 200,
            """{"chunk_index":2,"external_links":[{"chunk_index":2,"external_link":"https://x/2?sig=1","expiration":"2026-09-10T00:00:00Z","http_headers":{"a":"b"}}]}""");
        handler.Add(HttpMethod.Get, "/api/2.0/sql/statements/s1/result/chunks/3", 200, """{"chunk_index":3,"external_links":[]}""");

        var link = await DbxStatement.GetLinkAsync(Client(handler), "s1", 2, "ctx", CancellationToken.None);
        Assert.Equal("https://x/2?sig=1", link.ExternalLink);
        Assert.Equal("b", link.HttpHeaders!["a"]);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => DbxStatement.GetLinkAsync(Client(handler), "s1", 3, "ctx", CancellationToken.None));
        Assert.StartsWith("databricks: PZDB0204: ", ex.Message);
    }
}
