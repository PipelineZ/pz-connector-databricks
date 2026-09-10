using System.Text;

namespace Pz.Connector.Databricks.Tests;

/// <summary>A routable <see cref="HttpMessageHandler"/> double for the connector's HTTP clients.
/// Routes are keyed by <c>(method, path-and-query)</c> matched against the request's
/// <see cref="Uri.PathAndQuery"/> ordinally -- exact match only, no wildcards, so a test that expects
/// a particular query string (or its absence) says so directly in the route key. Every request that
/// arrives, matched or not, is recorded before the response is produced.</summary>
internal class FakeHandler : HttpMessageHandler
{
    public sealed record Recorded(HttpMethod Method, Uri Url, IReadOnlyDictionary<string, string> Headers, string Body, byte[] BodyBytes);

    private readonly Dictionary<(string Method, string PathAndQuery), (int Status, string Body, IDictionary<string, string>? Headers)> _routes = new();
    private readonly Dictionary<(string Method, string PathAndQuery), (int Status, byte[] Body, string ContentType, IDictionary<string, string>? Headers)> _byteRoutes = new();

    public List<Recorded> Requests { get; } = [];

    public Func<HttpRequestMessage, HttpResponseMessage?>? Interceptor { get; set; }

    /// <summary>Set once <see cref="Dispose(bool)"/> runs -- <c>new HttpClient(handler)</c> owns and
    /// disposes the handler by default, so this is how a test proves a caller actually disposed the
    /// <see cref="HttpClient"/> wrapping this handler rather than leaking it.</summary>
    public bool Disposed { get; private set; }

    public void Add(HttpMethod method, string pathAndQuery, int status, string body, IDictionary<string, string>? headers = null)
    {
        _routes[(method.Method, pathAndQuery)] = (status, body, headers);
    }

    public void AddBytes(HttpMethod method, string pathAndQuery, int status, byte[] body, string contentType, IDictionary<string, string>? headers = null)
    {
        _byteRoutes[(method.Method, pathAndQuery)] = (status, body, contentType, headers);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Disposed = true;
        }

        base.Dispose(disposing);
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        var bodyBytes = request.Content is null ? [] : await request.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);
        var body = Encoding.UTF8.GetString(bodyBytes);
        var headers = request.Headers
            .ToDictionary(h => h.Key, h => string.Join(", ", h.Value), StringComparer.OrdinalIgnoreCase);
        if (request.Content is not null)
        {
            // Content-Type/Content-Length live on HttpContent.Headers, not HttpRequestMessage.Headers
            // -- merged in so a test can assert on either without knowing which bucket .NET put it in.
            foreach (var header in request.Content.Headers)
            {
                headers[header.Key] = string.Join(", ", header.Value);
            }
        }

        var url = request.RequestUri ?? throw new InvalidOperationException("request has no RequestUri");
        Requests.Add(new Recorded(request.Method, url, headers, body, bodyBytes));

        if (Interceptor?.Invoke(request) is { } intercepted)
        {
            return intercepted;
        }

        if (_byteRoutes.TryGetValue((request.Method.Method, url.PathAndQuery), out var byteRoute))
        {
            return RespondBytes(byteRoute.Status, byteRoute.Body, byteRoute.ContentType, byteRoute.Headers);
        }

        if (_routes.TryGetValue((request.Method.Method, url.PathAndQuery), out var route))
        {
            return Respond(route.Status, route.Body, route.Headers);
        }

        return Respond(404, """{"error_code":"NOT_FOUND","message":"no route"}""", null);
    }

    private static HttpResponseMessage Respond(int status, string body, IDictionary<string, string>? headers)
    {
        var response = new HttpResponseMessage((System.Net.HttpStatusCode)status)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        };

        AddHeaders(response, headers);
        return response;
    }

    private static HttpResponseMessage RespondBytes(int status, byte[] body, string contentType, IDictionary<string, string>? headers)
    {
        var response = new HttpResponseMessage((System.Net.HttpStatusCode)status)
        {
            Content = new ByteArrayContent(body),
        };
        response.Content.Headers.TryAddWithoutValidation("Content-Type", contentType);

        AddHeaders(response, headers);
        return response;
    }

    private static void AddHeaders(HttpResponseMessage response, IDictionary<string, string>? headers)
    {
        if (headers is null)
        {
            return;
        }

        foreach (var (key, value) in headers)
        {
            // Location/Retry-After are response headers, not content headers -- TryAddWithoutValidation
            // accepts either bucket and lets HttpResponseMessage sort it out, matching how a real
            // server's raw header line is agnostic to which .NET splits it into.
            response.Headers.TryAddWithoutValidation(key, value);
        }
    }
}
