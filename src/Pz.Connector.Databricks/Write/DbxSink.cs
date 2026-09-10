using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>Opens one write session per output. Every check here is offline: mode, schema policy,
/// merge keys, the reserved sequence column, the staging volume, output options, and the schema
/// map. Nothing touches the network until <see cref="DbxWriteSession.CommitAsync"/>.
///
/// <para><paramref name="owned"/> is whatever the caller handed over with the sink -- the
/// <see cref="HttpClient"/> the connector opened for it -- and is disposed exactly once with the
/// sink. The engine disposes a sink it opened, so nothing else may hold that client.</para></summary>
internal sealed class DbxSink(
    DbxConnectionConfig cfg, DbxRestClient rest, TimeProvider time, ILogger logger, long spoolRollBytes, IDisposable? owned = null) : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var context = $"output '{spec.Output}'";
        if (spec.Mode is not ("append" or "replace" or "merge"))
        {
            throw Refuse(DbxCodes.Write_BadWriteMode, $"{context}: write mode '{spec.Mode}' is not supported here; use append, replace, or merge");
        }

        if (string.Equals(spec.SchemaPolicy, "evolve", StringComparison.Ordinal))
        {
            throw Refuse(DbxCodes.Write_BadOutputOption, $"{context}: schema evolution is not supported; use 'fail_on_change' and align the target by hand, or drop it and let the sink recreate it");
        }

        if (spec.Mode == "merge")
        {
            if (spec.Keys.Count == 0)
            {
                throw Refuse(DbxCodes.Write_MergeKeys, $"{context}: merge mode requires at least one merge key (write.keys)");
            }

            var fieldNames = new HashSet<string>(schema.FieldsList.Select(f => f.Name), StringComparer.Ordinal);
            var missing = spec.Keys.Where(k => !fieldNames.Contains(k)).ToList();
            if (missing.Count > 0)
            {
                throw Refuse(DbxCodes.Write_MergeKeys, $"{context}: merge key(s) {string.Join(", ", missing.Select(k => $"'{k}'"))} not present in the write schema");
            }
        }

        // Databricks folds identifiers, so a differently-cased spelling would collide with the
        // sequence column just the same.
        if (schema.FieldsList.Any(f => string.Equals(f.Name, DbxSchemaMap.SequenceColumn, StringComparison.OrdinalIgnoreCase)))
        {
            throw Refuse(DbxCodes.Write_MergeKeys, $"{context}: column '{DbxSchemaMap.SequenceColumn}' is reserved for merge-mode row ordering and cannot appear in the write schema");
        }

        if (cfg.StagingVolume is not { } volume)
        {
            throw Refuse(DbxCodes.Config_StagingVolumeInvalid, $"{context}: 'staging_volume' is required on the connection for any write (a Unity Catalog volume, catalog.schema.volume)");
        }

        // The errors are already coded and redacted where they are raised; joining them adds no
        // second prefix, matching how a dataset config's errors reach the caller.
        var errors = new List<string>();
        var output = DbxWriteConfig.Parse(spec, cfg, errors)
            ?? throw new PzConnectorException(string.Join("; ", errors), isTransient: false);

        var columns = DbxSchemaMap.Plan(schema, spec.Output);
        var withSequence = spec.Mode == "merge";
        var tag = Guid.NewGuid().ToString("N")[..16];
        var dir = Path.Combine(Path.GetTempPath(), "pz-databricks", tag);
        var spool = new DbxParquetSpool(dir, columns, withSequence, spoolRollBytes);
        var files = new DbxFilesApi(rest, volume, tag);

        return ValueTask.FromResult<ISinkWriteSession>(
            new DbxWriteSession(cfg, rest, time, logger, output.Target, spec, columns, spool, files));
    }

    public ValueTask DisposeAsync()
    {
        owned?.Dispose();
        return ValueTask.CompletedTask;
    }

    private PzConnectorException Refuse(string code, string text) =>
        new(DbxCodes.Message(code, cfg.Redactor, text), isTransient: false);
}
