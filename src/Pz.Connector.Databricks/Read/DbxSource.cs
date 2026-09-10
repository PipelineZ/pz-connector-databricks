using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>A Databricks table, view or query read through the SQL Statement Execution API. The
/// schema comes from a <c>limit 0</c> probe and is cached per dataset; a plan runs the real
/// statement to completion and exposes one partition per result chunk.</summary>
internal sealed class DbxSource(DbxConnectionConfig cfg, DbxRestClient rest, TimeProvider time, ILogger logger) : ISource
{
    private readonly ConcurrentDictionary<string, DatasetSchema> _schemaCache = new(StringComparer.Ordinal);

    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var config = ParseConfig(spec);
        return await ResolveSchemaAsync(spec, config, ct).ConfigureAwait(false);
    }

    public async ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        var config = ParseConfig(spec);
        var schema = await ResolveSchemaAsync(spec, config, ct).ConfigureAwait(false);
        var plan = DbxReadPlan.Build(config, spec, hints, schema.Schema, cfg.Redactor);
        var context = Context(spec, config);

        var start = time.GetTimestamp();
        var response = await DbxStatement.ExecuteAsync(rest, cfg, plan.Sql, plan.Parameters, arrow: true, context,
            DbxCodes.Read_StatementFailed, time, logger, ct).ConfigureAwait(false);

        var statementId = response.StatementId!;
        var chunks = response.Manifest?.Chunks ?? [];
        logger.LogDebug("databricks: planned dataset {Dataset} into {Chunks} chunk(s) ({Rows} rows) in {ElapsedMs}ms",
            spec.Dataset, chunks.Length, response.Manifest?.TotalRowCount, time.GetElapsedTime(start).TotalMilliseconds);

        return chunks
            .Select(c => (IDatasetPartition)new DbxPartition(rest, statementId, c.ChunkIndex ?? 0, c.RowCount ?? 0, context, logger))
            .ToList();
    }

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private async Task<DatasetSchema> ResolveSchemaAsync(DatasetSpec spec, DbxReadConfig config, CancellationToken ct)
    {
        var key = $"{spec.Dataset}|{config.Table?.FullName ?? config.Query}";
        if (_schemaCache.TryGetValue(key, out var cached))
        {
            return cached;
        }

        var response = await DbxStatement.ExecuteAsync(rest, cfg, DbxReadPlan.ProbeSql(config), null, arrow: false,
            $"{Context(spec, config)}: resolving the schema", DbxCodes.Read_StatementFailed, time, logger, ct).ConfigureAwait(false);
        var resultSchema = response.Manifest?.Schema
            ?? throw new PzConnectorException(
                DbxCodes.Message(DbxCodes.Read_StatementFailed, cfg.Redactor, $"{Context(spec, config)}: the schema probe returned no manifest"),
                isTransient: false);

        var schema = new DatasetSchema(DbxTypeMap.ToArrowSchema(resultSchema));
        _schemaCache[key] = schema;
        return schema;
    }

    // DbxReadConfig.Parse already prefixes every error it adds with "databricks: PZDB####: ", so the
    // join must not prefix again.
    private DbxReadConfig ParseConfig(DatasetSpec spec)
    {
        var errors = new List<string>();
        return DbxReadConfig.Parse(spec, cfg, errors)
            ?? throw new PzConnectorException(string.Join("; ", errors), isTransient: false);
    }

    private static string Context(DatasetSpec spec, DbxReadConfig config) =>
        config.Table is { } table ? $"reading dataset '{spec.Dataset}' ({table.FullName})" : $"reading dataset '{spec.Dataset}' (query)";
}
