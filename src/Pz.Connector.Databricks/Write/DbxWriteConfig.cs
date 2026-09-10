using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>An output's write options, parsed once at <c>BeginWriteAsync</c>. A write always names a
/// concrete table; there is no <c>query:</c> counterpart.</summary>
internal sealed record DbxWriteConfig(TableRef Target)
{
    private static readonly string[] KnownOptions = ["entity"];

    public static DbxWriteConfig? Parse(OutputSpec spec, DbxConnectionConfig cfg, List<string> errors)
    {
        var start = errors.Count;
        foreach (var key in spec.Options.Keys.Where(k => !KnownOptions.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"unknown output option '{key}'; known options: {string.Join(", ", KnownOptions)}");
        }

        var entityText = spec.Options.TryGetValue("entity", out var v) ? v?.ToString() : null;
        var entity = string.IsNullOrEmpty(entityText) ? spec.Output : entityText;
        if (!TableRef.TryParse(entity, cfg.Catalog, cfg.Schema, out var target, out var error, out var code))
        {
            errors.Add(DbxCodes.Message(code!, cfg.Redactor, error!));
            return null;
        }

        return errors.Count == start ? new DbxWriteConfig(target) : null;
    }
}
