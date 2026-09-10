using Apache.Arrow;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>One output's write: rows spool to Parquet as they arrive; every network call -- the
/// target lookup, the uploads, and the one statement that touches the target -- happens in
/// <see cref="CommitAsync"/>. CLEANUP NEVER THROWS: every cleanup step is guarded and logged, so a
/// leftover staged file can never turn a committed write into a reported failure or mask the
/// commit's own exception.</summary>
internal sealed class DbxWriteSession(
    DbxConnectionConfig cfg, DbxRestClient rest, TimeProvider time, ILogger logger, TableRef target, OutputSpec spec,
    IReadOnlyList<DbxColumnPlan> columns, DbxParquetSpool spool, DbxFilesApi files) : ISinkWriteSession
{
    private enum State { Open, Committed, Aborted, Disposed }

    private readonly List<string> _uploaded = [];
    private long _nextSequence;
    private long _rows;
    private long _batches;
    private State _state = State.Open;

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        if (_state != State.Open)
        {
            throw new InvalidOperationException($"the session is not open (state: {_state})");
        }

        await spool.WriteBatchAsync(batch, _nextSequence, ct).ConfigureAwait(false);
        _nextSequence += batch.Length;
        _rows += batch.Length;
        _batches++;
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        EnsureNotDisposed();
        if (_state == State.Committed)
        {
            throw new InvalidOperationException("the session is already committed");
        }

        if (_state == State.Aborted)
        {
            throw new InvalidOperationException("the session is aborted");
        }

        _state = State.Committed;

        var context = $"output '{spec.Output}'";
        var start = time.GetTimestamp();
        try
        {
            var localFiles = await spool.CloseAsync().ConfigureAwait(false);
            var existing = await rest.GetTableAsync(target, $"{context}: looking up {target.FullName}", ct).ConfigureAwait(false);
            if (spec.Mode != "replace")
            {
                EnsureColumnsPresent(existing, context);
            }

            for (var i = 0; i < localFiles.Count; i++)
            {
                _uploaded.Add(await files.UploadAsync(localFiles[i], i, context, ct).ConfigureAwait(false));
            }

            if (existing is null && spec.Mode is "append" or "merge")
            {
                await RunAsync(DbxSql.CreateIfNotExists(target, columns), context, ct).ConfigureAwait(false);
            }

            var statement = spec.Mode switch
            {
                "append" => DbxSql.Append(target, columns, files.Dir),
                "replace" => DbxSql.Replace(target, columns, files.Dir),
                "merge" => DbxSql.Merge(target, columns, spec.Keys, files.Dir),
                _ => throw new InvalidOperationException($"unexpected write mode '{spec.Mode}' reached CommitAsync -- BeginWriteAsync should have refused it"),
            };
            await RunAsync(statement, context, ct).ConfigureAwait(false);

            logger.LogDebug("databricks: {Context}: committed {Rows} row(s) in {Batches} batch(es) over {Files} file(s) in {ElapsedMs}ms",
                context, _rows, _batches, localFiles.Count, time.GetElapsedTime(start).TotalMilliseconds);
            return new WriteResult(_rows, _batches);
        }
        finally
        {
            await CleanupAsync().ConfigureAwait(false);
        }
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        EnsureNotDisposed();
        if (_state == State.Committed)
        {
            throw new InvalidOperationException("AbortAsync after CommitAsync is not allowed");
        }

        if (_state == State.Aborted)
        {
            throw new InvalidOperationException("the session is already aborted");
        }

        _state = State.Aborted;
        await CleanupAsync().ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_state == State.Open)
        {
            await AbortAsync(CancellationToken.None).ConfigureAwait(false);
        }

        _state = State.Disposed;
    }

    /// <summary>Disposal is terminal: it does not reopen the session, and it must not erase the
    /// verdict a commit or abort already reached -- a second commit after disposal would otherwise
    /// re-run the whole commit body.</summary>
    private void EnsureNotDisposed()
    {
        if (_state == State.Disposed)
        {
            throw new InvalidOperationException("the session is disposed");
        }
    }

    /// <summary>Every write column must exist on an existing target, compared case-insensitively
    /// as Databricks compares identifiers. Type differences are left to Spark's insert casts. Only
    /// append and merge insert into the target as it stands; replace redefines its schema outright,
    /// so a column the write does not carry is not a mismatch there.</summary>
    private void EnsureColumnsPresent(DbxTableInfo? existing, string context)
    {
        if (existing is null)
        {
            return;
        }

        var present = new HashSet<string>((existing.Columns ?? []).Select(c => c.Name ?? ""), StringComparer.OrdinalIgnoreCase);
        var missing = columns.Where(c => !present.Contains(c.Name)).Select(c => c.Name).ToList();
        if (missing.Count > 0)
        {
            throw new PzConnectorException(
                DbxCodes.Message(DbxCodes.Write_TargetColumnMissing, cfg.Redactor,
                    $"{context}: the target table {target.FullName} has no column {string.Join(", ", missing.Select(m => $"'{m}'"))} -- add it to the target, or drop the table and let the sink recreate it"),
                isTransient: false);
        }
    }

    private Task RunAsync(string sql, string context, CancellationToken ct) =>
        DbxStatement.ExecuteRowsAsync(rest, cfg, sql, null, context, DbxCodes.Write_TargetStatementFailed, time, logger, ct);

    private async Task CleanupAsync()
    {
        if (_uploaded.Count > 0)
        {
            await files.DeleteAllAsync(_uploaded, logger).ConfigureAwait(false);
            _uploaded.Clear();
        }

        try
        {
            spool.Delete();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "databricks: output {Output}: failed to delete the spool directory {Dir}", spec.Output, spool.Dir);
        }
    }
}
