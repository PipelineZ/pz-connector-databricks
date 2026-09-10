namespace Pz.Connector.Databricks.Tests;

public sealed class DbxRedactorTests
{
    [Fact]
    public void Registered_secrets_are_masked_longest_first()
    {
        var redactor = new DbxRedactor(["abc", "abcdef"]);

        Assert.Equal("x *** y *** z", redactor.Redact("x abcdef y abc z"));
    }

    [Fact]
    public void Secrets_shorter_than_three_characters_are_ignored()
    {
        var redactor = new DbxRedactor(["ab"]);

        Assert.Equal("ab is fine", redactor.Redact("ab is fine"));
    }

    [Fact]
    public void AddSecret_registers_a_token_minted_later()
    {
        var redactor = new DbxRedactor([]);
        redactor.AddSecret("minted-token");

        Assert.Equal("got ***", redactor.Redact("got minted-token"));
    }

    [Fact]
    public void AddSecret_of_a_secret_already_held_changes_nothing()
    {
        var redactor = new DbxRedactor(["first-secret"]);
        redactor.AddSecret("minted-token");
        redactor.AddSecret("minted-token");
        redactor.AddSecret("first-secret");

        Assert.Equal("*** and ***", redactor.Redact("minted-token and first-secret"));
        Assert.Equal("*** and ***", redactor.Redact("first-secret and minted-token"));
    }

    [Fact]
    public void AddSecret_on_None_is_a_no_op()
    {
        DbxRedactor.None.AddSecret("leaky");

        Assert.Equal("leaky", DbxRedactor.None.Redact("leaky"));
    }

    [Theory]
    [InlineData("Authorization: Bearer dapi123abc rest", "Authorization: *** rest")]
    [InlineData("authorization: Basic Zm9vOmJhcg== rest", "authorization: *** rest")]
    public void Authorization_headers_are_masked(string input, string expected)
    {
        Assert.Equal(expected, new DbxRedactor([]).Redact(input));
    }

    [Theory]
    [InlineData("access_token=abc&scope=x", "access_token=***&scope=x")]
    [InlineData("client_secret=s3cret;", "client_secret=***;")]
    [InlineData("""{"access_token": "abc.def", "token_type": "Bearer"}""", """{"access_token": "***", "token_type": "Bearer"}""")]
    public void Token_pairs_are_masked(string input, string expected)
    {
        Assert.Equal(expected, new DbxRedactor([]).Redact(input));
    }

    [Theory]
    [InlineData("GET https://bucket.s3.amazonaws.com/a/b?X-Amz-Signature=deadbeef&X-Amz-Expires=900 failed",
        "GET https://bucket.s3.amazonaws.com/<redacted> failed")]
    [InlineData("https://acct.blob.core.windows.net/c/p?sig=abc%3D ok", "https://acct.blob.core.windows.net/<redacted> ok")]
    [InlineData("https://storage.googleapis.com/b/o?X-Goog-Signature=1 ok", "https://storage.googleapis.com/<redacted> ok")]
    public void Presigned_urls_keep_only_their_host(string input, string expected)
    {
        Assert.Equal(expected, new DbxRedactor([]).Redact(input));
    }

    [Fact]
    public void Workspace_urls_are_left_alone()
    {
        const string text = "GET https://adb-1.2.azuredatabricks.net/api/2.0/sql/statements/abc -> 404";

        Assert.Equal(text, new DbxRedactor([]).Redact(text));
    }
}
