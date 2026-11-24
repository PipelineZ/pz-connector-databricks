using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>One result chunk of a succeeded statement. Reading it fetches a fresh link every time,
/// so a partition the engine reads more than once never depends on a link it saw earlier.</summary>
internal sealed class DbxPartition(DbxRestClient rest, string statementId, long chunkIndex, long rowCount, string context, ILogger logger,
    IReadOnlyDictionary<string, IArrowType>? nested = null)
    : IDatasetPartition
{
    public long RowCount => rowCount;

    public IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, CancellationToken ct)
    {
        // Counted here, not in the iterator, so the ambient stats are read on the caller's flow
        // when the read starts. A refreshed link within one read is the same chunk.
        ConnectorStats.Current.AddExtra("databricks.chunks", 1);
        var batches = DbxArrowChunkReader.ReadAsync(rest, statementId, chunkIndex, context, logger, ct);
        return nested is { Count: > 0 } ? ConvertAsync(batches, nested, ct) : batches;
    }

    private async IAsyncEnumerable<RecordBatch> ConvertAsync(
        IAsyncEnumerable<RecordBatch> batches, IReadOnlyDictionary<string, IArrowType> nestedColumns,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var batch in batches.WithCancellation(ct).ConfigureAwait(false))
        {
            yield return DbxNestedColumns.Convert(batch, nestedColumns, context);
        }
    }
}
