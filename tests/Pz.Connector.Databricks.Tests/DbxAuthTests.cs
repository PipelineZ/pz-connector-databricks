using System.Net;
using System.Text;
using Microsoft.Extensions.Time.Testing;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxAuthTests
{
    private static DbxConnectionConfig OAuthConfig() =>
        new(new Uri("https://ws.example/"), "abc", DbxAuthKind.OAuth, null, "client-1", "shh-secret", null, null, null, new DbxRedactor(["shh-secret"]));

    [Fact]
    public async Task Static_token_source_returns_the_token()
    {
        var source = new StaticTokenSource("dapi-1");
        Assert.Equal("dapi-1", await source.GetTokenAsync(CancellationToken.None));
    }

    [Fact]
    public async Task OAuth_posts_client_credentials_with_basic_auth_and_caches_until_near_expiry()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/oidc/v1/token", 200, """{"access_token":"tok-1","token_type":"Bearer","expires_in":3600}""");
        var time = new FakeTimeProvider();
        var cfg = OAuthConfig();
        var source = new OAuthTokenSource(new HttpClient(handler), cfg, time, cfg.Redactor);

        Assert.Equal("tok-1", await source.GetTokenAsync(CancellationToken.None));
        Assert.Equal("tok-1", await source.GetTokenAsync(CancellationToken.None));
        Assert.Single(handler.Requests);

        var req = handler.Requests[0];
        Assert.Equal("Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("client-1:shh-secret")), req.Headers["Authorization"]);
        Assert.Contains("grant_type=client_credentials", req.Body);
        Assert.Contains("scope=all-apis", req.Body);
        Assert.Equal("*** and ***", cfg.Redactor.Redact("tok-1 and shh-secret"));

        // 59 s before expiry: refresh.
        time.Advance(TimeSpan.FromSeconds(3600 - 59));
        handler.Add(HttpMethod.Post, "/oidc/v1/token", 200, """{"access_token":"tok-2","token_type":"Bearer","expires_in":3600}""");
        Assert.Equal("tok-2", await source.GetTokenAsync(CancellationToken.None));
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task OAuth_refusal_is_PZDB0403_and_never_echoes_the_secret()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/oidc/v1/token", 401, """{"error":"invalid_client","error_description":"bad client shh-secret"}""");
        var cfg = OAuthConfig();
        var source = new OAuthTokenSource(new HttpClient(handler), cfg, new FakeTimeProvider(), cfg.Redactor);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => source.GetTokenAsync(CancellationToken.None));

        Assert.StartsWith("databricks: PZDB0403: ", ex.Message);
        Assert.Contains("HTTP 401", ex.Message);
        Assert.DoesNotContain("shh-secret", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task OAuth_unparsable_body_is_PZDB0403()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/oidc/v1/token", 200, "<html>proxy</html>");
        var cfg = OAuthConfig();
        var source = new OAuthTokenSource(new HttpClient(handler), cfg, new FakeTimeProvider(), cfg.Redactor);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => source.GetTokenAsync(CancellationToken.None));
        Assert.StartsWith("databricks: PZDB0403: ", ex.Message);
    }

    [Fact]
    public void Create_picks_the_source_by_auth_kind()
    {
        var token = new DbxConnectionConfig(new Uri("https://ws.example/"), "abc", DbxAuthKind.Token, "t", null, null, null, null, null, DbxRedactor.None);
        Assert.IsType<StaticTokenSource>(DbxAuth.Create(token, new HttpClient(new FakeHandler()), TimeProvider.System));
        Assert.IsType<OAuthTokenSource>(DbxAuth.Create(OAuthConfig(), new HttpClient(new FakeHandler()), TimeProvider.System));
    }
}
