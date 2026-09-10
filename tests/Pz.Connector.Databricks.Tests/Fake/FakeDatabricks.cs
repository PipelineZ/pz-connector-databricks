using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Pz.Connector.Databricks.Tests;

/// <summary>A statement-shape-aware Databricks workspace double, served in process as an
/// <see cref="HttpMessageHandler"/>. It implements the exact endpoints the connector uses and
/// recognizes the exact statement shapes <c>DbxSql</c>/<c>DbxReadPlan</c> generate -- a test double
/// for this connector's own protocol, not a SQL engine. Scripted failures are one-shot properties a
/// test sets before the call that should trip over them.</summary>
internal sealed partial class FakeDatabricks : HttpMessageHandler
{
    public const string HostUrl = "https://fake.databricks.test/";
    private const string LinkHost = "https://links.fake.test/";

    private sealed class Statement
    {
        public required string Id { get; init; }
        public required string Sql { get; init; }
        public string State { get; set; } = "SUCCEEDED";
        public int PollsLeft { get; set; }
        public (string Code, string Message)? Error { get; set; }
        public FakeTable? Table { get; set; }
        public List<object?[]> ResultRows { get; set; } = [];
        public string?[][]? Inline { get; set; }
        public bool Arrow { get; set; }

        /// <summary>The chunk size this statement's result was actually cut at: the table's own
        /// override when it has one, otherwise the workspace default at submit time.</summary>
        public required int ChunkRows { get; set; }

        public List<byte[]> Chunks { get; } = [];
        public HashSet<long> LinksServedOnce { get; } = [];
    }

    private readonly Dictionary<string, Statement> _statements = new(StringComparer.Ordinal);
    private int _nextStatement;

    public Dictionary<string, FakeTable> Tables { get; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, string> Queries { get; } = new(StringComparer.Ordinal);
    public int RowsPerChunk { get; set; } = 50;
    public int PendingPollsBeforeSuccess { get; set; }
    public (string Code, string Message)? FailNextStatement { get; set; }
    public int RateLimitNextRequests { get; set; }
    public bool ExpireFirstLinkFetch { get; set; }

    /// <summary>When set, a presigned-link GET answers 200 and its body hands over the chunk's first
    /// bytes and then throws, standing in for a download that stops part-way through.</summary>
    public Func<Exception>? LinkBodyFault { get; set; }

    /// <summary>When set, a presigned-link GET answers 200 with bytes that are not an Arrow stream.</summary>
    public bool CorruptLinkBody { get; set; }
    public bool RejectToken { get; set; }
    public string WarehouseState { get; set; } = "RUNNING";
    public HashSet<string> AcceptedTokens { get; } = new(["fake-token", "fake-oauth-token"], StringComparer.Ordinal);
    public (string Id, string Secret)? OAuthClient { get; set; }
    public List<string> Statements { get; } = [];
    public List<DbxParameter[]?> StatementParameters { get; } = [];
    public List<(HttpMethod Method, Uri Url, IReadOnlyDictionary<string, string> Headers)> Requests { get; } = [];
    public Dictionary<string, byte[]> Uploads { get; } = new(StringComparer.Ordinal);
    public int CancelCalls { get; private set; }

    public Dictionary<string, object?> ConnectionConfig(string? catalog = "main", string? schema = "sales", string? stagingVolume = "main.pz.staging")
    {
        var values = new Dictionary<string, object?>
        {
            ["host"] = HostUrl.TrimEnd('/'), ["warehouse_id"] = "abc123", ["auth"] = "token", ["token"] = "fake-token",
        };
        if (catalog is not null) values["catalog"] = catalog;
        if (schema is not null) values["schema"] = schema;
        if (stagingVolume is not null) values["staging_volume"] = stagingVolume;
        return values;
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        var url = request.RequestUri!;
        var headers = request.Headers.ToDictionary(h => h.Key, h => string.Join(", ", h.Value), StringComparer.OrdinalIgnoreCase);
        lock (Requests) { Requests.Add((request.Method, url, headers)); }

        if (url.ToString().StartsWith(LinkHost, StringComparison.Ordinal))
        {
            return ServeLink(url, headers);
        }

        if (url.AbsolutePath == "/oidc/v1/token")
        {
            return ServeToken(headers);
        }

        if (RateLimitNextRequests > 0)
        {
            RateLimitNextRequests--;
            return Json(429, """{"error_code":"RESOURCE_EXHAUSTED","message":"too many requests"}""", ("Retry-After", "1"));
        }

        if (RejectToken || !headers.TryGetValue("Authorization", out var auth) || !auth.StartsWith("Bearer ", StringComparison.Ordinal)
            || !AcceptedTokens.Contains(auth["Bearer ".Length..]))
        {
            return Json(401, """{"error_code":"PERMISSION_DENIED","message":"Invalid Token"}""");
        }

        var body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(ct);
        var path = url.AbsolutePath;

        if (request.Method == HttpMethod.Post && path == "/api/2.0/sql/statements")
        {
            return Submit(body);
        }

        var m = StatementPath().Match(path);
        if (m.Success)
        {
            var statement = _statements[m.Groups["id"].Value];
            if (request.Method == HttpMethod.Post && m.Groups["rest"].Value == "/cancel")
            {
                CancelCalls++;
                statement.State = "CANCELED";
                return Json(200, "{}");
            }

            if (m.Groups["rest"].Value.StartsWith("/result/chunks/", StringComparison.Ordinal))
            {
                var index = long.Parse(m.Groups["rest"].Value["/result/chunks/".Length..], CultureInfo.InvariantCulture);
                return Json(200, ChunkJson(statement, index));
            }

            if (statement.PollsLeft > 0 && --statement.PollsLeft == 0)
            {
                statement.State = "SUCCEEDED";
            }

            return Json(200, StatementJson(statement));
        }

        if (request.Method == HttpMethod.Get && path == "/api/2.0/sql/warehouses/abc123")
        {
            return Json(200, $$"""{"id":"abc123","name":"fake-wh","state":"{{WarehouseState}}"}""");
        }

        if (request.Method == HttpMethod.Get && path.StartsWith("/api/2.1/unity-catalog/tables/", StringComparison.Ordinal))
        {
            var full = Uri.UnescapeDataString(path["/api/2.1/unity-catalog/tables/".Length..]);
            if (!Tables.TryGetValue(full, out var table))
            {
                return Json(404, """{"error_code":"TABLE_DOES_NOT_EXIST","message":"no such table"}""");
            }

            var cols = string.Join(",", table.Columns.Select((c, i) =>
                $$"""{"name":"{{c.Name}}","type_text":"{{table.TypeText(i).ToLowerInvariant()}}","type_name":"{{table.TypeName(i)}}","position":{{i}}}"""));
            return Json(200, $$"""{"name":"{{full.Split('.')[^1]}}","full_name":"{{full}}","table_type":"MANAGED","columns":[{{cols}}]}""");
        }

        if (path.StartsWith("/api/2.0/fs/files/", StringComparison.Ordinal))
        {
            var volumePath = path["/api/2.0/fs/files".Length..];
            if (request.Method == HttpMethod.Put)
            {
                Uploads[volumePath] = await request.Content!.ReadAsByteArrayAsync(ct);
                return new HttpResponseMessage(HttpStatusCode.NoContent);
            }

            if (request.Method == HttpMethod.Delete)
            {
                return new HttpResponseMessage(Uploads.Remove(volumePath) ? HttpStatusCode.NoContent : HttpStatusCode.NotFound);
            }
        }

        if (request.Method == HttpMethod.Delete && path.StartsWith("/api/2.0/fs/directories/", StringComparison.Ordinal))
        {
            var dir = path["/api/2.0/fs/directories".Length..];
            return new HttpResponseMessage(Uploads.Keys.Any(k => k.StartsWith(dir + "/", StringComparison.Ordinal))
                ? HttpStatusCode.BadRequest
                : HttpStatusCode.NoContent);
        }

        return Json(404, """{"error_code":"NOT_FOUND","message":"no route"}""");
    }

    private HttpResponseMessage ServeToken(Dictionary<string, string> headers)
    {
        if (OAuthClient is { } client && headers.TryGetValue("Authorization", out var auth)
            && auth == "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes($"{client.Id}:{client.Secret}")))
        {
            return Json(200, """{"access_token":"fake-oauth-token","token_type":"Bearer","expires_in":3600}""");
        }

        return Json(401, """{"error":"invalid_client","error_description":"client authentication failed"}""");
    }

    private HttpResponseMessage ServeLink(Uri url, Dictionary<string, string> headers)
    {
        // Cloud storage rejects a workspace bearer token on a presigned URL.
        if (headers.ContainsKey("Authorization"))
        {
            return new HttpResponseMessage(HttpStatusCode.BadRequest);
        }

        var parts = url.AbsolutePath.Trim('/').Split('/');
        var statement = _statements[parts[0]];
        var index = long.Parse(parts[1], CultureInfo.InvariantCulture);

        if (statement.Table?.LinkStatus is { } forced)
        {
            return new HttpResponseMessage((HttpStatusCode)forced);
        }

        if (ExpireFirstLinkFetch && statement.LinksServedOnce.Add(index))
        {
            return new HttpResponseMessage(HttpStatusCode.Forbidden) { Content = new StringContent("<Error><Code>ExpiredToken</Code></Error>") };
        }

        if (CorruptLinkBody)
        {
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent(Encoding.UTF8.GetBytes("this is not an arrow ipc stream, not even close"))
                {
                    Headers = { ContentType = new("application/octet-stream") },
                },
            };
        }

        if (LinkBodyFault is { } fault)
        {
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new FaultingStream(statement.Chunks[(int)index], fault)) };
        }

        return new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new ByteArrayContent(statement.Chunks[(int)index]) { Headers = { ContentType = new("application/octet-stream") } },
        };
    }

    /// <summary>A body that hands over a prefix of the real bytes and then fails, so a decoder sees a
    /// download that stopped rather than a clean end of stream.</summary>
    private sealed class FaultingStream(byte[] body, Func<Exception> fault) : Stream
    {
        private const int Prefix = 8;

        private int _position;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => _position; set => throw new NotSupportedException(); }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsMemory(offset, count).Span);

        public override int Read(Span<byte> buffer)
        {
            var available = Math.Min(Prefix, body.Length) - _position;
            if (available <= 0)
            {
                throw fault();
            }

            var n = Math.Min(buffer.Length, available);
            body.AsSpan(_position, n).CopyTo(buffer);
            _position += n;
            return n;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default) =>
            ValueTask.FromResult(Read(buffer.Span));

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct) =>
            Task.FromResult(Read(buffer.AsSpan(offset, count)));

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private HttpResponseMessage Submit(string body)
    {
        var request = JsonSerializer.Deserialize(body, DbxJsonContext.Default.DbxStatementRequest)!;
        lock (Statements)
        {
            Statements.Add(request.Statement);
            StatementParameters.Add(request.Parameters);
        }

        var statement = new Statement
        {
            Id = $"st-{++_nextStatement}",
            Sql = request.Statement,
            Arrow = request.Format == "ARROW_STREAM",
            ChunkRows = RowsPerChunk,
        };
        _statements[statement.Id] = statement;

        if (FailNextStatement is { } failure)
        {
            FailNextStatement = null;
            statement.State = "FAILED";
            statement.Error = failure;
            return Json(200, StatementJson(statement));
        }

        try
        {
            Execute(statement, request);
        }
        catch (FakeSqlException ex)
        {
            statement.State = "FAILED";
            statement.Error = ("BAD_REQUEST", ex.Message);
            return Json(200, StatementJson(statement));
        }

        if (PendingPollsBeforeSuccess > 0)
        {
            statement.State = "PENDING";
            statement.PollsLeft = PendingPollsBeforeSuccess;
            PendingPollsBeforeSuccess = 0;
        }

        return Json(200, StatementJson(statement));
    }

    /// <summary>Recognizes the connector's own statement shapes. Anything else is a fake SQL error.</summary>
    private void Execute(Statement statement, DbxStatementRequest request)
    {
        var sql = statement.Sql.Trim();
        if (sql == "select 1")
        {
            statement.Inline = [["1"]];
            return;
        }

        var probe = ProbePattern().Match(sql);
        if (probe.Success)
        {
            statement.Table = ResolveSource(probe.Groups["inner"].Value, request);
            statement.ResultRows = [];
            statement.Inline = [];
            return;
        }

        if (TryExecuteWrite(statement, sql))
        {
            return;
        }

        FakeTable source;
        List<object?[]> rows;
        if (Queries.TryGetValue(sql, out var aliased))
        {
            // A query: read runs verbatim; the fake serves the aliased table whole rather than
            // interpreting SQL it did not generate.
            source = Tables[aliased];
            rows = [.. source.Rows];
        }
        else
        {
            var read = ReadPattern().Match(sql);
            if (!read.Success)
            {
                throw new FakeSqlException($"[PARSE_SYNTAX_ERROR] unrecognized statement shape: {sql}");
            }

            source = ResolveSource(sql, request);
            rows = FilterByWatermark(source, read.Groups["where"].Value, request.Parameters);
        }

        statement.Table = source;
        statement.ResultRows = rows;
        statement.ChunkRows = source.RowsPerChunk ?? RowsPerChunk;
        if (statement.Arrow)
        {
            for (var offset = 0; offset < rows.Count; offset += statement.ChunkRows)
            {
                statement.Chunks.Add(source.ToArrowStream(rows.Skip(offset).Take(statement.ChunkRows).ToList()));
            }
        }
        else
        {
            statement.Inline = rows.Select(r => r.Select(v => v?.ToString()).ToArray()).ToArray();
        }
    }

    private FakeTable ResolveSource(string sql, DbxStatementRequest request)
    {
        if (Queries.TryGetValue(sql.Trim(), out var aliased))
        {
            return Tables[aliased];
        }

        var m = TablePattern().Match(sql);
        if (!m.Success)
        {
            throw new FakeSqlException($"[TABLE_OR_VIEW_NOT_FOUND] cannot resolve a table in: {sql}");
        }

        var key = $"{m.Groups["c"].Value}.{m.Groups["s"].Value}.{m.Groups["t"].Value}";
        return Tables.TryGetValue(key, out var table) ? table : throw new FakeSqlException($"[TABLE_OR_VIEW_NOT_FOUND] {key}");
    }

    private static List<object?[]> FilterByWatermark(FakeTable table, string where, DbxParameter[]? parameters)
    {
        IEnumerable<object?[]> rows = table.Rows;
        var lower = LowerPattern().Match(where);
        if (lower.Success)
        {
            var col = Array.FindIndex(table.Columns, c => c.Name == lower.Groups["c"].Value);
            var bound = long.Parse(parameters!.Single(p => p.Name == "pz_lower").Value!, CultureInfo.InvariantCulture);
            var inclusive = lower.Groups["op"].Value == ">=";
            rows = rows.Where(r => inclusive ? Convert.ToInt64(r[col]) >= bound : Convert.ToInt64(r[col]) > bound);
        }

        var upper = UpperPattern().Match(where);
        if (upper.Success)
        {
            var col = Array.FindIndex(table.Columns, c => c.Name == upper.Groups["c"].Value);
            var bound = long.Parse(parameters!.Single(p => p.Name == "pz_upper").Value!, CultureInfo.InvariantCulture);
            rows = rows.Where(r => Convert.ToInt64(r[col]) <= bound);
        }

        return rows.ToList();
    }

    private static string StatementJson(Statement s)
    {
        // The error object is rendered on its own and interpolated in: two JSON objects closing back
        // to back would put a bare "}}" in the literal, which a $$-raw string reads as a delimiter.
        var status = $$"""{"state":"{{s.State}}"}""";
        if (s.Error is { } e)
        {
            var error = $$"""{"error_code":"{{e.Code}}","message":"{{JsonEncodedText.Encode(e.Message)}}"}""";
            status = $$"""{"state":"{{s.State}}","error":{{error}}}""";
        }

        var manifest = "";
        var result = "";
        if (s.State == "SUCCEEDED" && s.Table is { } table)
        {
            var cols = string.Join(",", table.Columns.Select((c, i) =>
                $$"""{"name":"{{c.Name}}","type_text":"{{table.TypeText(i)}}","type_name":"{{table.TypeName(i)}}","position":{{i}}}"""));
            var chunks = string.Join(",", s.Chunks.Select((bytes, i) =>
                $$"""{"chunk_index":{{i}},"row_offset":{{(long)i * s.ChunkRows}},"row_count":{{RowsInChunk(s, i)}},"byte_count":{{bytes.Length}}}"""));
            manifest = $$""","manifest":{"format":"{{(s.Arrow ? "ARROW_STREAM" : "JSON_ARRAY")}}","schema":{"column_count":{{table.Columns.Length}},"columns":[{{cols}}]},"total_chunk_count":{{s.Chunks.Count}},"chunks":[{{chunks}}],"total_row_count":{{s.ResultRows.Count}},"truncated":false}""";
            if (!s.Arrow)
            {
                result = $$""","result":{"chunk_index":0,"row_count":{{s.ResultRows.Count}},"data_array":{{JsonSerializer.Serialize(s.Inline ?? [], DbxJsonContext.Default.StringArrayArray)}}}""";
            }
            else if (s.Chunks.Count > 0)
            {
                result = $$""","result":{{ChunkJson(s, 0)}}""";
            }
        }
        else if (s.State == "SUCCEEDED" && s.Inline is not null)
        {
            result = $$""","result":{"chunk_index":0,"row_count":{{s.Inline.Length}},"data_array":{{JsonSerializer.Serialize(s.Inline, DbxJsonContext.Default.StringArrayArray)}}}""";
        }

        return $$"""{"statement_id":"{{s.Id}}","status":{{status}}{{manifest}}{{result}}}""";
    }

    private static string ChunkJson(Statement s, long index) =>
        $$"""{"chunk_index":{{index}},"row_count":{{RowsInChunk(s, index)}},"external_links":[{"chunk_index":{{index}},"row_count":0,"byte_count":{{s.Chunks[(int)index].Length}},"external_link":"{{LinkHost}}{{s.Id}}/{{index}}?X-Amz-Signature=fake","http_headers":{"x-fake":"1"},"expiration":"2099-01-01T00:00:00Z"}]}""";

    private static long RowsInChunk(Statement s, long index) => Math.Min(s.ChunkRows, s.ResultRows.Count - (index * s.ChunkRows));

    private static HttpResponseMessage Json(int status, string body, params (string Key, string Value)[] headers)
    {
        var response = new HttpResponseMessage((HttpStatusCode)status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
        foreach (var (key, value) in headers)
        {
            response.Headers.TryAddWithoutValidation(key, value);
        }

        return response;
    }

    private sealed class FakeSqlException(string message) : Exception(message);

    [GeneratedRegex(@"^/api/2\.0/sql/statements/(?<id>[^/]+)(?<rest>/.*)?$")]
    private static partial Regex StatementPath();

    [GeneratedRegex(@"^select \* from \((?<inner>.+)\) as pz_probe limit 0$", RegexOptions.Singleline)]
    private static partial Regex ProbePattern();

    [GeneratedRegex(@"^select (?<cols>.+?) from `[^`]+`\.`[^`]+`\.`[^`]+`(?: where (?<where>.+))?$", RegexOptions.Singleline)]
    private static partial Regex ReadPattern();

    [GeneratedRegex(@"from `(?<c>[^`]+)`\.`(?<s>[^`]+)`\.`(?<t>[^`]+)`")]
    private static partial Regex TablePattern();

    [GeneratedRegex(@"`(?<c>[^`]+)` (?<op>>=|>) :pz_lower")]
    private static partial Regex LowerPattern();

    [GeneratedRegex(@"`(?<c>[^`]+)` <= :pz_upper")]
    private static partial Regex UpperPattern();

    // The write statement shapes land with the sink; until then nothing is a write.
    private static bool TryExecuteWrite(Statement statement, string sql) => false;
}
