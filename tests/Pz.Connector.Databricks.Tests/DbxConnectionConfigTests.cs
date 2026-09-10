using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxConnectionConfigTests
{
    // A later tuple for a key already seen overrides it (Valid() relies on this to override one base
    // key while keeping the rest) -- Enumerable.ToDictionary throws on a repeated key instead.
    private static ConnectorConfig Config(params (string Key, object? Value)[] values)
    {
        var byKey = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var (key, value) in values)
        {
            byKey[key] = value;
        }

        return new ConnectorConfig(byKey);
    }

    private static ConnectorConfig Valid(params (string Key, object? Value)[] extra) =>
        Config([("host", "https://adb-1.2.azuredatabricks.net"), ("warehouse_id", "abc123def456"), ("auth", "token"), ("token", "dapi-secret"), .. extra]);

    [Fact]
    public void Parses_a_token_connection()
    {
        var errors = new List<string>();
        var cfg = DbxConnectionConfig.Parse(Valid(("catalog", "main"), ("schema", "sales"), ("staging_volume", "main.pz.staging")), errors);

        Assert.Empty(errors);
        Assert.NotNull(cfg);
        Assert.Equal(new Uri("https://adb-1.2.azuredatabricks.net/"), cfg!.Host);
        Assert.Equal("abc123def456", cfg.WarehouseId);
        Assert.Equal(DbxAuthKind.Token, cfg.AuthKind);
        Assert.Equal("dapi-secret", cfg.Token);
        Assert.Equal("main", cfg.Catalog);
        Assert.Equal("sales", cfg.Schema);
        Assert.Equal(new TableRef("main", "pz", "staging"), cfg.StagingVolume);
        Assert.Equal("*** here", cfg.Redactor.Redact("dapi-secret here"));
    }

    [Fact]
    public void Parses_an_oauth_connection_and_registers_the_secret()
    {
        var errors = new List<string>();
        var cfg = DbxConnectionConfig.Parse(
            Config([("host", "https://x.cloud.databricks.com/"), ("warehouse_id", "abc"), ("auth", "oauth"), ("client_id", "id"), ("client_secret", "shh-secret")]),
            errors);

        Assert.Empty(errors);
        Assert.Equal(DbxAuthKind.OAuth, cfg!.AuthKind);
        Assert.Equal("id", cfg.ClientId);
        Assert.Equal("*** here", cfg.Redactor.Redact("shh-secret here"));
    }

    [Theory]
    [InlineData("http://x.cloud.databricks.com", "must be an https URL")]
    [InlineData("https://x.cloud.databricks.com/api", "no user info, path")]
    [InlineData("https://x.cloud.databricks.com/?a=1", "no user info, path")]
    [InlineData("https://x.cloud.databricks.com/#f", "no user info, path")]
    [InlineData("https://user:pass@x.cloud.databricks.com", "no user info, path")]
    [InlineData("not a url", "must be an https URL")]
    public void Host_must_be_a_bare_https_origin(string host, string fragment)
    {
        var errors = new List<string>();
        var cfg = DbxConnectionConfig.Parse(Valid(("host", host)), errors);

        Assert.Null(cfg);
        Assert.Contains(errors, e => e.StartsWith("databricks: PZDB0101: ", StringComparison.Ordinal) && e.Contains("'host'") && e.Contains(fragment));
    }

    [Fact]
    public void A_host_with_user_info_never_echoes_the_credential()
    {
        var errors = new List<string>();
        Assert.Null(DbxConnectionConfig.Parse(Valid(("host", "https://user:pass@x.cloud.databricks.com")), errors));
        Assert.DoesNotContain(errors, e => e.Contains("pass", StringComparison.Ordinal));
    }

    [Fact]
    public void Host_is_required()
    {
        var errors = new List<string>();
        Assert.Null(DbxConnectionConfig.Parse(Config([("warehouse_id", "abc"), ("auth", "token"), ("token", "t")]), errors));
        Assert.Contains(errors, e => e.StartsWith("databricks: PZDB0101: 'host' is required", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("")]
    [InlineData("ABC-123")]
    // The regex is anchored with \A/\z: a '$' anchor would accept a value with a trailing newline.
    [InlineData("abc123\n")]
    public void Warehouse_id_must_be_lowercase_hex(string id)
    {
        var errors = new List<string>();
        Assert.Null(DbxConnectionConfig.Parse(Valid(("warehouse_id", id)), errors));
        Assert.Contains(errors, e => e.StartsWith("databricks: PZDB0102: ", StringComparison.Ordinal) && e.Contains("'warehouse_id'"));
    }

    [Fact]
    public void Token_auth_requires_token_and_forbids_oauth_keys()
    {
        var errors = new List<string>();
        Assert.Null(DbxConnectionConfig.Parse(Config([("host", "https://h"), ("warehouse_id", "abc"), ("auth", "token"), ("client_id", "x")]), errors));
        Assert.Contains(errors, e => e.StartsWith("databricks: PZDB0103: ", StringComparison.Ordinal) && e.Contains("'token' is required"));
        Assert.Contains(errors, e => e.StartsWith("databricks: PZDB0103: ", StringComparison.Ordinal) && e.Contains("'client_id'") && e.Contains("not used"));
    }

    [Fact]
    public void Oauth_auth_requires_both_client_keys_and_forbids_token()
    {
        var errors = new List<string>();
        Assert.Null(DbxConnectionConfig.Parse(Config([("host", "https://h"), ("warehouse_id", "abc"), ("auth", "oauth"), ("client_id", "x"), ("token", "t")]), errors));
        Assert.Contains(errors, e => e.StartsWith("databricks: PZDB0103: ", StringComparison.Ordinal) && e.Contains("'client_secret' is required"));
        Assert.Contains(errors, e => e.StartsWith("databricks: PZDB0103: ", StringComparison.Ordinal) && e.Contains("'token'") && e.Contains("not used"));
    }

    [Fact]
    public void Auth_must_be_token_or_oauth()
    {
        var errors = new List<string>();
        Assert.Null(DbxConnectionConfig.Parse(Valid(("auth", "pat")), errors));
        Assert.Contains(errors, e => e.StartsWith("databricks: PZDB0103: ", StringComparison.Ordinal) && e.Contains("'auth' must be one of token, oauth"));
    }

    [Fact]
    public void Unknown_keys_are_reported()
    {
        var errors = new List<string>();
        DbxConnectionConfig.Parse(Valid(("warehouse", "x")), errors);
        Assert.Contains(errors, e => e.StartsWith("databricks: PZDB0107: ", StringComparison.Ordinal) && e.Contains("unknown connection key 'warehouse'"));
    }

    [Fact]
    public void Staging_volume_must_be_three_parts()
    {
        var errors = new List<string>();
        Assert.Null(DbxConnectionConfig.Parse(Valid(("staging_volume", "pz.staging")), errors));
        Assert.Contains(errors, e => e.StartsWith("databricks: PZDB0106: ", StringComparison.Ordinal) && e.Contains("'staging_volume'") && e.Contains("catalog.schema.volume"));
    }

    [Fact]
    public void An_empty_catalog_or_schema_is_PZDB0107()
    {
        var errors = new List<string>();
        Assert.Null(DbxConnectionConfig.Parse(Valid(("catalog", ""), ("schema", "")), errors));
        Assert.Contains(errors, e => e.StartsWith("databricks: PZDB0107: ", StringComparison.Ordinal) && e.Contains("'catalog' must not be empty"));
        Assert.Contains(errors, e => e.StartsWith("databricks: PZDB0107: ", StringComparison.Ordinal) && e.Contains("'schema' must not be empty"));
    }

    [Fact]
    public void Every_error_carries_its_own_code()
    {
        var errors = new List<string>();
        DbxConnectionConfig.Parse(Config([("warehouse", "x"), ("auth", "pat"), ("staging_volume", "a.b")]), errors);

        Assert.NotEmpty(errors);
        Assert.All(errors, e => Assert.StartsWith("databricks: PZDB01", e, StringComparison.Ordinal));
    }

    [Fact]
    public void Errors_never_echo_the_secret()
    {
        var errors = new List<string>();
        DbxConnectionConfig.Parse(Config([("host", "https://h"), ("warehouse_id", "abc"), ("auth", "oauth"), ("token", "dapi-leak")]), errors);
        Assert.DoesNotContain(errors, e => e.Contains("dapi-leak"));
    }
}
