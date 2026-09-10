using Apache.Arrow;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>One result chunk of a succeeded statement. Reading it fetches a fresh link every time,
/// so a partition the engine reads more than once never depends on a link it saw earlier.</summary>
internal sealed class DbxPartition(DbxRestClient rest, string statementId, long chunkIndex, long rowCount, string context, ILogger logger)
    : IDatasetPartition
{
    public long RowCount => rowCount;

    public IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, CancellationToken ct) =>
        DbxArrowChunkReader.ReadAsync(rest, statementId, chunkIndex, context, logger, ct);
}
