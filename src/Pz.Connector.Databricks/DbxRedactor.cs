namespace Pz.Connector.Databricks;

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

        return text;
    }

    private static string[] Sorted(IEnumerable<string> incoming, IEnumerable<string> existing) =>
        incoming.Concat(existing).Where(s => s.Length >= 3).Distinct(StringComparer.Ordinal)
            .OrderByDescending(s => s.Length).ToArray();
}
