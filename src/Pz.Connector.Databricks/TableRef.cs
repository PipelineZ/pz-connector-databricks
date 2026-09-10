using System.Text;

namespace Pz.Connector.Databricks;

/// <summary>A fully-resolved Unity Catalog object address. Parsed once from an entity name plus the
/// connection's default catalog/schema, then carried everywhere an identity is needed: generated SQL
/// (<see cref="Quoted"/>), the Unity Catalog REST path (<see cref="FullName"/>) and a volume's
/// filesystem path (<see cref="VolumePath"/>). Databricks identifiers are case-insensitive; the name
/// is kept as written and quoted so the service applies its own folding rules, not ours.</summary>
internal readonly record struct TableRef(string Catalog, string Schema, string Table)
{
    public string Quoted => $"{QuoteIdentifier(Catalog)}.{QuoteIdentifier(Schema)}.{QuoteIdentifier(Table)}";

    public string FullName => $"{Catalog}.{Schema}.{Table}";

    public string VolumePath => $"/Volumes/{Catalog}/{Schema}/{Table}";

    /// <summary>Backtick-quotes one identifier part: a backtick inside becomes two, the only escape
    /// Databricks SQL recognizes inside a quoted identifier.</summary>
    public static string QuoteIdentifier(string part)
    {
        var sb = new StringBuilder(part.Length + 2);
        sb.Append('`');
        foreach (var c in part)
        {
            if (c == '`')
            {
                sb.Append('`');
            }

            sb.Append(c);
        }

        sb.Append('`');
        return sb.ToString();
    }

    public static bool TryParse(string entity, string? defaultCatalog, string? defaultSchema,
        out TableRef result, out string? error, out string? code)
    {
        result = default;
        if (entity.Contains('`', StringComparison.Ordinal))
        {
            error = $"'{entity}': backticks are not accepted; write table, schema.table or catalog.schema.table";
            code = DbxCodes.Config_BackticksRejected;
            return false;
        }

        var parts = entity.Split('.');
        if (parts.Length is < 1 or > 3 || parts.Any(string.IsNullOrEmpty))
        {
            error = $"'{entity}' is not a valid entity name; use table, schema.table or catalog.schema.table";
            code = DbxCodes.Config_EntityUnresolvable;
            return false;
        }

        string? catalog = parts.Length == 3 ? parts[0] : defaultCatalog;
        string? schema = parts.Length >= 2 ? parts[^2] : defaultSchema;
        var table = parts[^1];

        if (catalog is null || schema is null)
        {
            var missing = catalog is null && schema is null ? "'catalog' and 'schema'" : catalog is null ? "'catalog'" : "'schema'";
            error = $"'{entity}' cannot be resolved: set {missing} on the connection, or write the fully qualified name";
            code = DbxCodes.Config_EntityUnresolvable;
            return false;
        }

        result = new TableRef(catalog, schema, table);
        error = null;
        code = null;
        return true;
    }

    /// <summary>A <c>staging_volume</c>: exactly <c>catalog.schema.volume</c>, no defaults applied --
    /// a volume address is spliced into a filesystem path, and a half-resolved one would name the wrong place.</summary>
    public static bool TryParseVolume(string text, out TableRef result, out string? error)
    {
        result = default;
        if (text.Contains('`', StringComparison.Ordinal))
        {
            error = $"'{text}': backticks are not accepted in a volume name";
            return false;
        }

        var parts = text.Split('.');
        if (parts.Length != 3 || parts.Any(string.IsNullOrEmpty))
        {
            error = $"'{text}' is not a valid volume name; use catalog.schema.volume";
            return false;
        }

        result = new TableRef(parts[0], parts[1], parts[2]);
        error = null;
        return true;
    }
}
