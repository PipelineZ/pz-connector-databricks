using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>The SQL Statement Execution protocol: submit with a server-side wait, then poll with a
/// bounded backoff until the statement is terminal. Poll delays go through the injected
/// <see cref="TimeProvider"/> so a test drives them without sleeping. A statement is never closed
/// (the API has no close call); caller cancellation cancels a statement still running so the
/// warehouse does not keep working for a result nobody will read.</summary>
internal static class DbxStatement
{
    private const string WaitTimeout = "50s";

    internal static readonly TimeSpan[] PollDelays =
    [
        TimeSpan.FromMilliseconds(500),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(2),
        TimeSpan.FromSeconds(5),
    ];

    public static async Task<DbxStatementResponse> ExecuteAsync(
        DbxRestClient rest, DbxConnectionConfig cfg, string sql, DbxParameter[]? parameters, bool arrow, string context,
        string failureCode, TimeProvider time, ILogger logger, CancellationToken ct)
    {
        var request = new DbxStatementRequest(
            cfg.WarehouseId, sql,
            arrow ? "EXTERNAL_LINKS" : "INLINE",
            arrow ? "ARROW_STREAM" : "JSON_ARRAY",
            WaitTimeout, "CONTINUE", cfg.Catalog, cfg.Schema, parameters);

        var response = await rest.SubmitStatementAsync(request, context, ct).ConfigureAwait(false);
        var statementId = response.StatementId
            ?? throw new PzConnectorException(DbxCodes.Message(failureCode, rest.Redactor, $"{context}: the service returned no statement id"), isTransient: false);

        // The submit already waited server-side (wait_timeout), so the first poll goes out at once;
        // the backoff applies between polls.
        var polls = 0;
        while (IsPending(response.Status?.State))
        {
            try
            {
                if (polls > 0)
                {
                    var delay = PollDelays[Math.Min(polls - 1, PollDelays.Length - 1)];
                    logger.LogDebug("databricks statement {StatementId} ({Context}) is {State}; polling again in {Delay}",
                        statementId, context, response.Status?.State, delay);
                    await Task.Delay(delay, time, ct).ConfigureAwait(false);
                }

                polls++;
                response = await rest.GetStatementAsync(statementId, context, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                await TryCancelAsync(rest, statementId, logger).ConfigureAwait(false);
                throw;
            }
        }

        var state = response.Status?.State;
        if (state == "SUCCEEDED")
        {
            return response;
        }

        if (state == "FAILED")
        {
            throw DbxErrors.FromStatement(response.Status?.Error?.ErrorCode, response.Status?.Error?.Message, rest.Redactor, context, failureCode);
        }

        throw new PzConnectorException(
            DbxCodes.Message(failureCode, rest.Redactor, $"{context}: statement ended in state {state ?? "(none)"} before producing a result"),
            isTransient: false);
    }

    public static async Task<string?[][]> ExecuteRowsAsync(
        DbxRestClient rest, DbxConnectionConfig cfg, string sql, DbxParameter[]? parameters, string context, string failureCode,
        TimeProvider time, ILogger logger, CancellationToken ct)
    {
        var response = await ExecuteAsync(rest, cfg, sql, parameters, arrow: false, context, failureCode, time, logger, ct).ConfigureAwait(false);
        return response.Result?.DataArray ?? [];
    }

    public static async Task<DbxExternalLink> GetLinkAsync(DbxRestClient rest, string statementId, long chunkIndex, string context, CancellationToken ct)
    {
        var data = await rest.GetChunkAsync(statementId, chunkIndex, context, ct).ConfigureAwait(false);
        return data.ExternalLinks is { Length: > 0 } links && links[0].ExternalLink is not null
            ? links[0]
            : throw new PzConnectorException(
                DbxCodes.Message(DbxCodes.Read_ChunkDownloadFailed, rest.Redactor, $"{context}: chunk {chunkIndex} carried no external link"),
                isTransient: false);
    }

    private static bool IsPending(string? state) => state is "PENDING" or "RUNNING";

    private static async Task TryCancelAsync(DbxRestClient rest, string statementId, ILogger logger)
    {
        try
        {
            await rest.CancelStatementAsync(statementId, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "databricks: could not cancel statement {StatementId} after caller cancellation", statementId);
        }
    }
}
