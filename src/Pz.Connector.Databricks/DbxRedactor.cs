using System.Text.RegularExpressions;

namespace Pz.Connector.Databricks;

/// <summary>Strips credentials from any text that may reach a <c>PzConnectorException</c> message.
/// Registered secrets are replaced longest first (a short secret that is a substring of a longer one
/// would otherwise shred the longer one's remnant). Beyond registered secrets, the shapes the
/// Databricks REST surface and cloud storage print are rewritten even when the value is not one of
/// ours: an <c>Authorization</c> header echoed into a diagnostic, an OAuth token pair or JSON member,
/// and a presigned storage URL (its query string is a bearer-equivalent capability for that object).
/// Secrets shorter than 3 characters are not matched; replacing them would shred unrelated text.</summary>
internal sealed partial class DbxRedactor
{
    public const string Mask = "***";

    public static readonly DbxRedactor None = new([]);

    private string[] _secrets;
    private readonly object _gate = new();

    public DbxRedactor(IReadOnlyList<string> secrets)
    {
        _secrets = Sorted(secrets, []);
    }

    public void AddSecret(string secret)
    {
        if (ReferenceEquals(this, None))
        {
            return;
        }

        lock (_gate)
        {
            _secrets = Sorted([secret], _secrets);
        }
    }

    public string Redact(string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        foreach (var secret in _secrets)
        {
            text = text.Replace(secret, Mask, StringComparison.Ordinal);
        }

        text = AuthorizationHeader().Replace(text, m => $"{m.Groups["key"].Value} {Mask}");
        text = JsonTokenMember().Replace(text, m => $"{m.Groups["prefix"].Value}{Mask}\"");
        text = TokenPair().Replace(text, m => $"{m.Groups["key"].Value}={Mask}");
        text = CloudStorageHostUrl().Replace(text, m => $"https://{m.Groups["host"].Value}/<redacted>");
        return SignedQueryUrl().Replace(text, m => $"https://{m.Groups["host"].Value}/<redacted>");
    }

    // "Authorization: Bearer <token>" / "Authorization: Basic <b64>": the credential runs to the next whitespace.
    [GeneratedRegex("""(?<key>\bAuthorization:)\s+(?:Bearer|Basic)\s+\S+""", RegexOptions.IgnoreCase)]
    private static partial Regex AuthorizationHeader();

    // "access_token": "..." / "client_secret": "..." / "token": "..." JSON members: only the value is masked.
    [GeneratedRegex("(?<prefix>\"(?:access_token|client_secret|token)\"\\s*:\\s*\")(?:[^\"\\\\]|\\\\.)*\"", RegexOptions.IgnoreCase)]
    private static partial Regex JsonTokenMember();

    // access_token=/client_secret=/token= in a query string or form body; '&', ';', ',' end the value.
    [GeneratedRegex("""(?<key>\b(?:access_token|refresh_token|client_secret|token)\b)=(?:"[^"]*"|[^\s;,&]+)""", RegexOptions.IgnoreCase)]
    private static partial Regex TokenPair();

    // A known cloud-storage host: the whole path and query are replaced, keeping only the host, since
    // any path segment on these hosts can itself be a capability (e.g. a signed CDN path).
    [GeneratedRegex("""https://(?<host>[^/\s]*(?:\.amazonaws\.com|\.blob\.core\.windows\.net|storage\.googleapis\.com))/[^\s"']*""", RegexOptions.IgnoreCase)]
    private static partial Regex CloudStorageHostUrl();

    // Any other https URL whose query string carries a signature parameter -- a presigned link even
    // on a host this redactor does not otherwise recognize as cloud storage.
    [GeneratedRegex("""https://(?<host>[^/\s]+)/[^\s"']*?[?&](?:X-Amz-Signature|sig|X-Goog-Signature)=[^\s"']*""", RegexOptions.IgnoreCase)]
    private static partial Regex SignedQueryUrl();

    private static string[] Sorted(IEnumerable<string> incoming, IEnumerable<string> existing) =>
        incoming.Concat(existing).Where(s => s.Length >= 3).Distinct(StringComparer.Ordinal)
            .OrderByDescending(s => s.Length).ToArray();
}
