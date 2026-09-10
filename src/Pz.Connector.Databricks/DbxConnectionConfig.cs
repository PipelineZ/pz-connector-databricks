using System.Text.RegularExpressions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

internal enum DbxAuthKind { Token, OAuth }

/// <summary>The typed connection surface. <see cref="Host"/> always ends with <c>/</c> and carries
/// no path, so request paths compose relatively against it. Every error names a key or a rule, never
/// the value that failed: a rejected value here can be a credential.</summary>
internal sealed partial record DbxConnectionConfig(
    Uri Host,
    string WarehouseId,
    DbxAuthKind AuthKind,
    string? Token,
    string? ClientId,
    string? ClientSecret,
    string? Catalog,
    string? Schema,
    TableRef? StagingVolume,
    DbxRedactor Redactor)
{
    private static readonly string[] KnownKeys =
        ["host", "warehouse_id", "auth", "token", "client_id", "client_secret", "catalog", "schema", "staging_volume"];

    private static readonly string[] AuthKinds = ["token", "oauth"];

    public static DbxConnectionConfig? Parse(ConnectorConfig config, List<string> errors)
    {
        var start = errors.Count;
        var secrets = new List<string>();

        foreach (var key in config.Values.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"unknown connection key '{key}'; known keys: {string.Join(", ", KnownKeys)}");
        }

        var host = ParseHost(config.GetString("host"), errors);
        var warehouseId = ParseWarehouseId(config.GetString("warehouse_id"), errors);
        var authKind = ParseAuthKind(config.GetString("auth"), errors);

        var token = Optional(config, "token");
        var clientId = Optional(config, "client_id");
        var clientSecret = Optional(config, "client_secret");
        if (token is not null) secrets.Add(token);
        if (clientSecret is not null) secrets.Add(clientSecret);

        switch (authKind)
        {
            case DbxAuthKind.Token:
                if (token is null) errors.Add("'token' is required for 'auth: token'");
                if (clientId is not null) errors.Add("'client_id' is not used by 'auth: token'; remove it or switch to 'auth: oauth'");
                if (clientSecret is not null) errors.Add("'client_secret' is not used by 'auth: token'; remove it or switch to 'auth: oauth'");
                break;
            case DbxAuthKind.OAuth:
                if (clientId is null) errors.Add("'client_id' is required for 'auth: oauth'");
                if (clientSecret is null) errors.Add("'client_secret' is required for 'auth: oauth'");
                if (token is not null) errors.Add("'token' is not used by 'auth: oauth'; remove it or switch to 'auth: token'");
                break;
        }

        var catalog = Optional(config, "catalog");
        var schema = Optional(config, "schema");
        if (config.Values.ContainsKey("catalog") && catalog is null) errors.Add("'catalog' must not be empty");
        if (config.Values.ContainsKey("schema") && schema is null) errors.Add("'schema' must not be empty");

        TableRef? stagingVolume = null;
        if (Optional(config, "staging_volume") is { } volumeText)
        {
            if (TableRef.TryParseVolume(volumeText, out var parsed, out var volumeError))
            {
                stagingVolume = parsed;
            }
            else
            {
                errors.Add($"'staging_volume': {volumeError}");
            }
        }

        if (errors.Count != start || host is null || warehouseId is null || authKind is null)
        {
            return null;
        }

        return new DbxConnectionConfig(host, warehouseId, authKind.Value, token, clientId, clientSecret, catalog, schema,
            stagingVolume, new DbxRedactor(secrets));
    }

    private static string? Optional(ConnectorConfig config, string key)
    {
        var value = config.GetString(key);
        return string.IsNullOrEmpty(value) ? null : value;
    }

    private static Uri? ParseHost(string? text, List<string> errors)
    {
        if (string.IsNullOrEmpty(text))
        {
            errors.Add("'host' is required (the workspace URL, e.g. https://adb-123.4.azuredatabricks.net)");
            return null;
        }

        if (!Uri.TryCreate(text, UriKind.Absolute, out var uri) || uri.Scheme != "https")
        {
            errors.Add("'host' must be an https URL (the workspace URL, e.g. https://adb-123.4.azuredatabricks.net)");
            return null;
        }

        if (uri.AbsolutePath is not ("/" or "") || !string.IsNullOrEmpty(uri.Query) || !string.IsNullOrEmpty(uri.Fragment))
        {
            errors.Add("'host' must be the workspace origin with no path, query or fragment");
            return null;
        }

        return new Uri(uri.GetLeftPart(UriPartial.Authority) + "/");
    }

    private static string? ParseWarehouseId(string? text, List<string> errors)
    {
        if (string.IsNullOrEmpty(text))
        {
            errors.Add("'warehouse_id' is required (the SQL warehouse id from its Connection details tab)");
            return null;
        }

        if (!WarehouseIdPattern().IsMatch(text))
        {
            errors.Add("'warehouse_id' must be a lowercase hexadecimal id");
            return null;
        }

        return text;
    }

    private static DbxAuthKind? ParseAuthKind(string? text, List<string> errors)
    {
        if (string.IsNullOrEmpty(text))
        {
            errors.Add($"'auth' is required (one of: {string.Join(", ", AuthKinds)})");
            return null;
        }

        return text switch
        {
            "token" => DbxAuthKind.Token,
            "oauth" => DbxAuthKind.OAuth,
            _ => Fail(errors, $"'auth' must be one of {string.Join(", ", AuthKinds)}"),
        };
    }

    private static DbxAuthKind? Fail(List<string> errors, string message)
    {
        errors.Add(message);
        return null;
    }

    [GeneratedRegex("^[a-f0-9]+$")]
    private static partial Regex WarehouseIdPattern();
}
