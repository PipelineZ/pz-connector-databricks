using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>Streams one result chunk: a fresh presigned link from the chunk endpoint, then a plain
/// GET on it decoded through <see cref="ArrowStreamReader"/>. A link the storage service refuses as
/// expired (403, or 400 from a signature check) is fetched again exactly once; anything else on the
/// link is classified by status like any other HTTP failure. Batches are yielded as the reader
/// produces them; the engine owns and disposes each one.</summary>
internal static class DbxArrowChunkReader
{
    public static async IAsyncEnumerable<RecordBatch> ReadAsync(
        DbxRestClient rest, string statementId, long chunkIndex, string context, ILogger logger,
        [EnumeratorCancellation] CancellationToken ct)
    {
        using var response = await OpenAsync(rest, statementId, chunkIndex, context, logger, ct).ConfigureAwait(false);
        await using var stream = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
        using var reader = new ArrowStreamReader(stream);

        while (true)
        {
            RecordBatch? batch;
            try
            {
                batch = await reader.ReadNextRecordBatchAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex) when (ex is IOException or HttpRequestException or InvalidDataException or InvalidOperationException)
            {
                throw new PzConnectorException(
                    DbxCodes.Message(DbxCodes.Read_ChunkDownloadFailed, rest.Redactor, $"{context}: chunk {chunkIndex} could not be decoded: {ex.Message}"),
                    isTransient: true, innerException: ex);
            }

            if (batch is null)
            {
                yield break;
            }

            if (batch.Length == 0)
            {
                batch.Dispose();
                continue;
            }

            yield return batch;
        }
    }

    private static async Task<HttpResponseMessage> OpenAsync(
        DbxRestClient rest, string statementId, long chunkIndex, string context, ILogger logger, CancellationToken ct)
    {
        for (var attempt = 0; ; attempt++)
        {
            var link = await DbxStatement.GetLinkAsync(rest, statementId, chunkIndex, context, ct).ConfigureAwait(false);
            var response = await rest.OpenExternalLinkAsync(link.ExternalLink!, link.HttpHeaders, context, ct).ConfigureAwait(false);
            if (response.IsSuccessStatusCode)
            {
                return response;
            }

            var status = (int)response.StatusCode;
            response.Dispose();

            if (status is 403 or 400 && attempt == 0)
            {
                logger.LogDebug("databricks: chunk {Chunk} link refused with HTTP {Status}; fetching a fresh link", chunkIndex, status);
                continue;
            }

            if (status is 429 or >= 500)
            {
                throw DbxErrors.FromHttp(status, null, "presigned chunk download failed", null, rest.Redactor, $"{context}: chunk {chunkIndex}");
            }

            throw new PzConnectorException(
                DbxCodes.Message(DbxCodes.Read_ChunkDownloadFailed, rest.Redactor,
                    $"{context}: chunk {chunkIndex} download was refused with HTTP {status} after refreshing the link"),
                isTransient: false);
        }
    }
}
