using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>A dataset's read options, parsed once. Exactly one of <see cref="Table"/>/<see cref="Query"/>
/// is set: an <c>entity:</c> read names a table (or view) directly, a <c>query:</c> read runs SQL verbatim.</summary>
internal sealed record DbxReadConfig(TableRef? Table, string? Query)
{
    private static readonly string[] KnownOptions = ["entity", "query"];

    public static DbxReadConfig? Parse(DatasetSpec spec, DbxConnectionConfig cfg, List<string> errors)
    {
        var start = errors.Count;

        foreach (var key in spec.Options.Keys.Where(k => !KnownOptions.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"unknown dataset option '{key}'; known options: {string.Join(", ", KnownOptions)}");
        }

        var entityText = GetString(spec.Options, "entity");
        var queryText = GetString(spec.Options, "query");
        var hasEntity = !string.IsNullOrEmpty(entityText);
        var hasQuery = !string.IsNullOrEmpty(queryText);

        if (hasEntity && hasQuery)
        {
            errors.Add(DbxCodes.Message(DbxCodes.Read_EntityAndQuery, cfg.Redactor,
                $"dataset '{spec.Dataset}': 'entity' and 'query' cannot both be set; use one or the other"));
            return null;
        }

        if (hasQuery)
        {
            return errors.Count == start ? new DbxReadConfig(null, queryText) : null;
        }

        var entity = hasEntity ? entityText! : spec.Dataset;
        if (!TableRef.TryParse(entity, cfg.Catalog, cfg.Schema, out var table, out var error, out var code))
        {
            errors.Add(DbxCodes.Message(code!, cfg.Redactor, error!));
            return null;
        }

        return errors.Count == start ? new DbxReadConfig(table, null) : null;
    }

    private static string? GetString(IReadOnlyDictionary<string, object?> options, string key) =>
        options.TryGetValue(key, out var value) ? value?.ToString() : null;
}
