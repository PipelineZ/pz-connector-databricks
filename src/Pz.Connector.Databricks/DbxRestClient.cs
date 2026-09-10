using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>The Databricks REST surface this connector touches: SQL statements, SQL warehouses,
/// Unity Catalog tables, and the Files API. Every workspace request path is relative to the
/// connection's host (never a leading <c>/</c>, which would clobber a path-bearing base). External
/// links are fetched through <see cref="OpenExternalLinkAsync"/> with no workspace credential at
/// all: they are presigned cloud-storage URLs, and an <c>Authorization</c> header on one is rejected
/// by the storage service.</summary>
internal sealed class DbxRestClient(HttpClient http, DbxConnectionConfig cfg, IDbxTokenSource tokens, DbxRedactor redactor, ILogger logger)
{
    private const string JsonContentType = "application/json; charset=UTF-8";

    public DbxRedactor Redactor => redactor;

    public async Task<DbxStatementResponse> SubmitStatementAsync(DbxStatementRequest request, string context, CancellationToken ct)
    {
        var json = JsonSerializer.Serialize(request, DbxJsonContext.Default.DbxStatementRequest);
        var (_, body) = await ExecuteAsync(HttpMethod.Post, "api/2.0/sql/statements", json, context, ct).ConfigureAwait(false);
        return DeserializeOrThrow(body, DbxJsonContext.Default.DbxStatementResponse, context);
    }

    public async Task<DbxStatementResponse> GetStatementAsync(string statementId, string context, CancellationToken ct)
    {
        var (_, body) = await ExecuteAsync(HttpMethod.Get, $"api/2.0/sql/statements/{Uri.EscapeDataString(statementId)}", null, context, ct).ConfigureAwait(false);
        return DeserializeOrThrow(body, DbxJsonContext.Default.DbxStatementResponse, context);
    }

    public async Task<DbxResultData> GetChunkAsync(string statementId, long chunkIndex, string context, CancellationToken ct)
    {
        var path = $"api/2.0/sql/statements/{Uri.EscapeDataString(statementId)}/result/chunks/{chunkIndex}";
        var (_, body) = await ExecuteAsync(HttpMethod.Get, path, null, context, ct).ConfigureAwait(false);
        return DeserializeOrThrow(body, DbxJsonContext.Default.DbxResultData, context);
    }

    public Task CancelStatementAsync(string statementId, CancellationToken ct) =>
        ExecuteAsync(HttpMethod.Post, $"api/2.0/sql/statements/{Uri.EscapeDataString(statementId)}/cancel", null, "cancelling statement", ct, HttpStatusCode.NotFound);

    public async Task<DbxWarehouse> GetWarehouseAsync(string context, CancellationToken ct)
    {
        var (_, body) = await ExecuteAsync(HttpMethod.Get, $"api/2.0/sql/warehouses/{Uri.EscapeDataString(cfg.WarehouseId)}", null, context, ct).ConfigureAwait(false);
        return DeserializeOrThrow(body, DbxJsonContext.Default.DbxWarehouse, context);
    }

    public async Task<DbxTableInfo?> GetTableAsync(TableRef table, string context, CancellationToken ct)
    {
        var path = $"api/2.1/unity-catalog/tables/{Uri.EscapeDataString(table.FullName)}";
        var (status, body) = await ExecuteAsync(HttpMethod.Get, path, null, context, ct, HttpStatusCode.NotFound).ConfigureAwait(false);
        return status == HttpStatusCode.NotFound ? null : DeserializeOrThrow(body, DbxJsonContext.Default.DbxTableInfo, context);
    }

    public async Task UploadFileAsync(string volumeFilePath, Stream content, long length, string context, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Put, new Uri(cfg.Host, $"api/2.0/fs/files{volumeFilePath}?overwrite=true"));
        request.Content = new StreamContent(content);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        request.Content.Headers.ContentLength = length;
        using var response = await SendCoreAsync(request, context, ct).ConfigureAwait(false);
        var body = await ReadBodyAsync(response, context, ct).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            var (code, message) = ParseError(body, response.ReasonPhrase);
            throw DbxErrors.FromHttp((int)response.StatusCode, code, message, RetryAfterOf(response), redactor, context);
        }
    }

    public Task DeleteFileAsync(string volumeFilePath, CancellationToken ct) =>
        ExecuteAsync(HttpMethod.Delete, $"api/2.0/fs/files{volumeFilePath}", null, "deleting a staged file", ct, HttpStatusCode.NotFound);

    public Task DeleteDirectoryAsync(string volumeDirPath, CancellationToken ct) =>
        ExecuteAsync(HttpMethod.Delete, $"api/2.0/fs/directories{volumeDirPath}", null, "deleting the staging directory", ct, HttpStatusCode.NotFound);

    /// <summary>A presigned cloud-storage GET: no workspace credential, headers as the link told us
    /// to send, headers-only completion so the body streams. The caller owns the response and decides
    /// what a non-2xx means (an expired link is refreshed once by the chunk reader).</summary>
    public Task<HttpResponseMessage> OpenExternalLinkAsync(string url, IReadOnlyDictionary<string, string>? headers, string context, CancellationToken ct) =>
        GuardTransportAsync(async () =>
        {
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            if (headers is not null)
            {
                foreach (var (key, value) in headers)
                {
                    request.Headers.TryAddWithoutValidation(key, value);
                }
            }

            logger.LogDebug("databricks GET external link for {Context}", context);
            return await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
        }, context, ct);

    private async Task<(HttpStatusCode Status, string Body)> ExecuteAsync(
        HttpMethod method, string relativePath, string? jsonBody, string context, CancellationToken ct, params HttpStatusCode[] tolerate)
    {
        using var request = new HttpRequestMessage(method, new Uri(cfg.Host, relativePath));
        if (jsonBody is not null)
        {
            request.Content = new StringContent(jsonBody, Encoding.UTF8);
            request.Content.Headers.ContentType = MediaTypeHeaderValue.Parse(JsonContentType);
        }

        using var response = await SendCoreAsync(request, context, ct).ConfigureAwait(false);
        var body = await ReadBodyAsync(response, context, ct).ConfigureAwait(false);

        if ((int)response.StatusCode is >= 200 and < 300 || Array.IndexOf(tolerate, response.StatusCode) >= 0)
        {
            return (response.StatusCode, body);
        }

        var (code, message) = ParseError(body, response.ReasonPhrase);
        throw DbxErrors.FromHttp((int)response.StatusCode, code, message, RetryAfterOf(response), redactor, context);
    }

    private Task<HttpResponseMessage> SendCoreAsync(HttpRequestMessage request, string context, CancellationToken ct) =>
        GuardTransportAsync(async () =>
        {
            var token = await tokens.GetTokenAsync(ct).ConfigureAwait(false);
            redactor.AddSecret(token);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            logger.LogDebug("databricks {Method} {Path}", request.Method, request.RequestUri?.AbsolutePath);
            return await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
        }, context, ct);

    private Task<string> ReadBodyAsync(HttpResponseMessage response, string context, CancellationToken ct) =>
        GuardTransportAsync(() => response.Content.ReadAsStringAsync(ct), context, ct);

    /// <summary>The one place a transport-level failure becomes a classified exception. Caller
    /// cancellation propagates unwrapped; an HttpClient-internal timeout arrives as a
    /// <see cref="TaskCanceledException"/> wrapping a <see cref="TimeoutException"/>, and it is that
    /// inner exception -- never the cancellation wrapper -- that is classified.</summary>
    private async Task<T> GuardTransportAsync<T>(Func<Task<T>> action, string context, CancellationToken ct)
    {
        try
        {
            return await action().ConfigureAwait(false);
        }
        catch (TaskCanceledException ex) when (ex.InnerException is TimeoutException timeout)
        {
            throw DbxErrors.Wrap(timeout, redactor, context);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex) when (ex is HttpRequestException or IOException or SocketException)
        {
            throw DbxErrors.Wrap(ex, redactor, context);
        }
    }

    private static (string? Code, string? Message) ParseError(string body, string? fallback)
    {
        if (!string.IsNullOrWhiteSpace(body))
        {
            try
            {
                var envelope = JsonSerializer.Deserialize(body, DbxJsonContext.Default.DbxErrorEnvelope);
                if (envelope is { ErrorCode: not null } or { Message: not null })
                {
                    return (envelope!.ErrorCode, envelope.Message);
                }
            }
            catch (JsonException)
            {
                // Not a Databricks envelope (a gateway's HTML, say) -- fall through.
            }
        }

        return (null, fallback);
    }

    /// <summary>The server's own retry hint, in seconds or as an HTTP-date. Reading it needs the live
    /// response, so any caller that disposes one must take this first.</summary>
    internal static TimeSpan? RetryAfterOf(HttpResponseMessage response)
    {
        var retryAfter = response.Headers.RetryAfter;
        if (retryAfter?.Delta is { } delta)
        {
            return delta;
        }

        if (retryAfter?.Date is { } date)
        {
            var remaining = date - DateTimeOffset.UtcNow;
            return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
        }

        return null;
    }

    private T DeserializeOrThrow<T>(string body, JsonTypeInfo<T> typeInfo, string context) where T : class
    {
        T? result = null;
        try
        {
            result = JsonSerializer.Deserialize(body, typeInfo);
        }
        catch (JsonException)
        {
            // A 2xx only means the transport succeeded; a rewriting proxy can still answer with a body
            // that is not the JSON this call expects. Surfaces as a classified error, never a raw JsonException.
        }

        return result ?? throw new PzConnectorException(
            DbxCodes.Message(DbxCodes.Remote_Transient, redactor, $"{context}: response body did not parse"), isTransient: false);
    }
}
