using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Parquet;
using Parquet.Schema;

namespace Pz.Connector.Databricks;

/// <summary>The on-disk Parquet spool a write session appends to before any network call: one
/// Parquet file per roll, one row group per batch, opened lazily so a session that never writes
/// never touches disk. Every Arrow value is copied into a managed array inside
/// <see cref="WriteBatchAsync"/>; by the time it returns nothing references the batch's buffers.
/// Not sealed: <see cref="Delete"/> is virtual so a test can force a deterministic cleanup failure.</summary>
internal class DbxParquetSpool(string dir, IReadOnlyList<DbxColumnPlan> columns, bool withSequence, long rollBytes)
{
    private readonly List<string> _closedFiles = [];
    private readonly DataField[] _fields = withSequence
        ? [.. columns.Select(c => c.SpoolField), DbxSchemaMap.SequenceField]
        : [.. columns.Select(c => c.SpoolField)];

    private FileStream? _stream;
    private ParquetWriter? _writer;
    private int _index;

    public string Dir => dir;

    public bool IsOpen => _writer is not null;

    public async Task WriteBatchAsync(RecordBatch batch, long firstSequence, CancellationToken ct)
    {
        var writer = await EnsureWriterAsync(ct).ConfigureAwait(false);
        using (var group = writer.CreateRowGroup())
        {
            for (var i = 0; i < columns.Count; i++)
            {
                await DbxColumnCopy.WriteAsync(group, _fields[i], batch.Column(i), columns[i], ct).ConfigureAwait(false);
            }

            if (withSequence)
            {
                var seq = new long[batch.Length];
                for (var r = 0; r < seq.Length; r++)
                {
                    seq[r] = firstSequence + r;
                }

                await group.WriteAsync<long>(DbxSchemaMap.SequenceField, seq, null, cancellationToken: ct).ConfigureAwait(false);
            }
        }

        await _stream!.FlushAsync(ct).ConfigureAwait(false);
        if (_stream.Length > rollBytes)
        {
            await CloseCurrentAsync().ConfigureAwait(false);
            _index++;
        }
    }

    public async Task<IReadOnlyList<string>> CloseAsync()
    {
        await CloseCurrentAsync().ConfigureAwait(false);
        return _closedFiles;
    }

    /// <summary>Discards whatever was spooled. Not part of the normal commit path -- a failed or
    /// abandoned write session's cleanup -- so a writer still open (mid-batch cancellation) is
    /// disposed synchronously rather than leaving the file locked.</summary>
    public virtual void Delete()
    {
        if (_writer is not null)
        {
            _writer.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _writer = null;
        }

        _stream?.Dispose();
        _stream = null;

        if (Directory.Exists(dir))
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    private async Task<ParquetWriter> EnsureWriterAsync(CancellationToken ct)
    {
        if (_writer is not null)
        {
            return _writer;
        }

        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, $"part-{_index.ToString("D5", CultureInfo.InvariantCulture)}.parquet");
        _stream = new FileStream(path, FileMode.Create, FileAccess.ReadWrite, FileShare.None, 64 * 1024, FileOptions.Asynchronous);
        _writer = await ParquetWriter.CreateAsync(new ParquetSchema(_fields), _stream,
            new ParquetOptions { CompressionMethod = CompressionMethod.Snappy }, cancellationToken: ct).ConfigureAwait(false);
        return _writer;
    }

    private async Task CloseCurrentAsync()
    {
        if (_writer is null)
        {
            return;
        }

        var path = _stream!.Name;
        await _writer.DisposeAsync().ConfigureAwait(false);
        _writer = null;
        await _stream.FlushAsync().ConfigureAwait(false);
        await _stream.DisposeAsync().ConfigureAwait(false);
        _stream = null;
        _closedFiles.Add(path);
    }
}

/// <summary>Copies one Arrow column into the Parquet row group being written. Decimals go through
/// <see cref="Decimal128Array.GetSqlDecimal"/>, exact to 38 digits and rendered with their declared
/// scale; uint64 renders as digits; a naive (timezone-less) timestamp renders as an ISO 8601 string
/// with microsecond precision, no zone suffix -- Databricks reads a naive Parquet TIMESTAMP back as
/// plain TIMESTAMP indistinguishable from a UTC one, so the meaning has to travel as text and get
/// cast back by the target statement. Every write goes through Parquet.Net's own typed
/// <c>WriteAsync&lt;T&gt;</c>/<c>WriteAsync</c> overloads with an explicit type argument where one is
/// needed: dispatch is a compile-time switch over the array's concrete type, never reflection, so
/// this stays Native-AOT safe.</summary>
internal static class DbxColumnCopy
{
    public static Task WriteAsync(ParquetRowGroupWriter group, DataField field, IArrowArray array, DbxColumnPlan plan, CancellationToken ct) => array switch
    {
        Int8Array a => group.WriteAsync<int>(field, Map(a.Length, i => a.IsNull(i) ? null : (int?)a.GetValue(i)), null, cancellationToken: ct),
        Int16Array a => group.WriteAsync<int>(field, Map(a.Length, i => a.IsNull(i) ? null : (int?)a.GetValue(i)), null, cancellationToken: ct),
        Int32Array a => group.WriteAsync<int>(field, Map(a.Length, a.GetValue), null, cancellationToken: ct),
        UInt8Array a => group.WriteAsync<int>(field, Map(a.Length, i => a.IsNull(i) ? null : (int?)a.GetValue(i)), null, cancellationToken: ct),
        UInt16Array a => group.WriteAsync<int>(field, Map(a.Length, i => a.IsNull(i) ? null : (int?)a.GetValue(i)), null, cancellationToken: ct),
        Int64Array a => group.WriteAsync<long>(field, Map(a.Length, a.GetValue), null, cancellationToken: ct),
        UInt32Array a => group.WriteAsync<long>(field, Map(a.Length, i => a.IsNull(i) ? null : (long?)a.GetValue(i)), null, cancellationToken: ct),
        UInt64Array a => group.WriteAsync(field, MapRef(a.Length, i => a.IsNull(i) ? null : a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture)), null),
        FloatArray a => group.WriteAsync<float>(field, Map(a.Length, a.GetValue), null, cancellationToken: ct),
        DoubleArray a => group.WriteAsync<double>(field, Map(a.Length, a.GetValue), null, cancellationToken: ct),
        Decimal128Array a => group.WriteAsync(field, MapRef(a.Length, i => a.IsNull(i) ? null : a.GetSqlDecimal(i)!.Value.ToString()), null),
        StringArray a => group.WriteAsync(field, MapRef(a.Length, i => a.IsNull(i) ? null : a.GetString(i)), null),
        LargeStringArray a => group.WriteAsync(field, MapRef(a.Length, i => a.IsNull(i) ? null : a.GetString(i)), null),
        BinaryArray a => group.WriteAsync(field, MapRef(a.Length, i => a.IsNull(i) ? null : a.GetBytes(i).ToArray()), null),
        LargeBinaryArray a => group.WriteAsync(field, MapRef(a.Length, i => a.IsNull(i) ? null : a.GetBytes(i).ToArray()), null),
        FixedSizeBinaryArray a => group.WriteAsync(field, MapRef(a.Length, i => a.IsNull(i) ? null : a.GetBytes(i).ToArray()), null),
        BooleanArray a => group.WriteAsync<bool>(field, Map(a.Length, a.GetValue), null, cancellationToken: ct),
        Date32Array a => group.WriteAsync<DateTime>(field, Map(a.Length, i => a.IsNull(i) ? (DateTime?)null : a.GetDateTime(i)!.Value.Date), null, cancellationToken: ct),
        Date64Array a => group.WriteAsync<DateTime>(field, Map(a.Length, i => a.IsNull(i) ? (DateTime?)null : a.GetDateTime(i)!.Value.Date), null, cancellationToken: ct),
        TimestampArray a when plan.CastTo == "TIMESTAMP_NTZ" =>
            group.WriteAsync(field, MapRef(a.Length, i => a.IsNull(i)
                ? null
                : a.GetTimestamp(i)!.Value.DateTime.ToString("yyyy-MM-ddTHH:mm:ss.ffffff", CultureInfo.InvariantCulture)), null),
        TimestampArray a when ((DateTimeDataField)field).IsAdjustedToUTC =>
            group.WriteAsync<DateTime>(field, Map(a.Length, i => a.IsNull(i) ? (DateTime?)null : a.GetTimestamp(i)!.Value.UtcDateTime), null, cancellationToken: ct),
        TimestampArray a =>
            group.WriteAsync<DateTime>(field, Map(a.Length, i => a.IsNull(i) ? (DateTime?)null : DateTime.SpecifyKind(a.GetTimestamp(i)!.Value.DateTime, DateTimeKind.Unspecified)), null, cancellationToken: ct),
        _ => throw new NotSupportedException($"column '{plan.Name}': Arrow array type '{array.GetType().Name}' has no spool encoding"),
    };

    private static T?[] Map<T>(int n, Func<int, T?> get) where T : struct
    {
        var values = new T?[n];
        for (var i = 0; i < n; i++)
        {
            values[i] = get(i);
        }

        return values;
    }

    private static T?[] MapRef<T>(int n, Func<int, T?> get) where T : class
    {
        var values = new T?[n];
        for (var i = 0; i < n; i++)
        {
            values[i] = get(i);
        }

        return values;
    }
}
