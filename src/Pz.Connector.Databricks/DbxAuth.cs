using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

internal interface IDbxTokenSource
{
    Task<string> GetTokenAsync(CancellationToken ct);
}

internal sealed class StaticTokenSource(string token) : IDbxTokenSource
{
    public Task<string> GetTokenAsync(CancellationToken ct) => Task.FromResult(token);
}

/// <summary>OAuth machine-to-machine client credentials against the workspace's own token endpoint.
/// The minted token is cached and refreshed once fewer than <see cref="RefreshMargin"/> remain, so a
/// long run never sends a token that expires mid-request. Every minted token is registered with the
/// redactor before it is returned. A refusal is an authentication problem, never transient.</summary>
internal sealed class OAuthTokenSource(HttpClient http, DbxConnectionConfig cfg, TimeProvider time, DbxRedactor redactor) : IDbxTokenSource
{
    private static readonly TimeSpan RefreshMargin = TimeSpan.FromSeconds(60);

    private readonly SemaphoreSlim _gate = new(1, 1);
    private string? _token;
    private DateTimeOffset _expiresAt;

    public async Task<string> GetTokenAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_token is not null && time.GetUtcNow() + RefreshMargin <= _expiresAt)
            {
                return _token;
            }

            var (token, expiresIn) = await MintAsync(ct).ConfigureAwait(false);
            _token = token;
            _expiresAt = time.GetUtcNow() + TimeSpan.FromSeconds(expiresIn);
            return token;
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<(string Token, long ExpiresIn)> MintAsync(CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(cfg.Host, "oidc/v1/token"));
        var basic = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{cfg.ClientId}:{cfg.ClientSecret}"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", basic);
        request.Content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["scope"] = "all-apis",
        });

        // response is declared outside the try so a failure between SendAsync succeeding and the
        // method returning (e.g. the content stream throwing mid-read) still disposes it in finally
        // -- the earlier "using (response)" only wrapped the body below and never ran on that path.
        HttpResponseMessage? response = null;
        try
        {
            response = await http.SendAsync(request, ct).ConfigureAwait(false);
            var body = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                throw Refused($"HTTP {(int)response.StatusCode}: {body}");
            }

            DbxTokenResponse? parsed = null;
            try
            {
                parsed = JsonSerializer.Deserialize(body, DbxJsonContext.Default.DbxTokenResponse);
            }
            catch (JsonException)
            {
                // Not a token response at all (a proxy's HTML page, say) -- reported just below.
            }

            if (parsed?.AccessToken is not { Length: > 0 } token)
            {
                throw Refused("the token endpoint's response carried no access_token");
            }

            redactor.AddSecret(token);
            return (token, parsed.ExpiresIn ?? 3600);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex) when (ex is HttpRequestException or IOException or System.Net.Sockets.SocketException)
        {
            throw DbxErrors.Wrap(ex, redactor, "minting an OAuth token");
        }
        finally
        {
            response?.Dispose();
        }
    }

    private PzConnectorException Refused(string detail) =>
        new(DbxCodes.Message(DbxCodes.Remote_TokenRefused, redactor,
                $"minting an OAuth token: {detail} -- check client_id/client_secret and that the service principal is added to the workspace"),
            isTransient: false);
}

internal static class DbxAuth
{
    public static IDbxTokenSource Create(DbxConnectionConfig cfg, HttpClient http, TimeProvider time) => cfg.AuthKind switch
    {
        DbxAuthKind.Token => new StaticTokenSource(cfg.Token!),
        _ => new OAuthTokenSource(http, cfg, time, cfg.Redactor),
    };
}
