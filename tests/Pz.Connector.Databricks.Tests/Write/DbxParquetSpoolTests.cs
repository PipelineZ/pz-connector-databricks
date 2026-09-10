using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxParquetSpoolTests
{
    private static readonly Schema TestSchema = new([
        new Field("id", Int64Type.Default, true),
        new Field("amount", new Decimal128Type(38, 9), true),
        new Field("name", StringType.Default, true),
        new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true),
    ], null);

    private static RecordBatch Batch(int from, int count)
    {
        var ids = new Int64Array.Builder();
        var amounts = new Decimal128Array.Builder(new Decimal128Type(38, 9));
        var names = new StringArray.Builder();
        var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        for (var i = from; i < from + count; i++)
        {
            ids.Append(i);
            amounts.Append("12345678901234567890.123456789");
            if (i % 7 == 0) names.AppendNull(); else names.Append($"n{i}");
            ts.Append(new DateTimeOffset(2026, 9, 10, 0, 0, 0, TimeSpan.Zero).AddMicroseconds(i));
        }

        return new RecordBatch(TestSchema, [ids.Build(), amounts.Build(), names.Build(), ts.Build()], count);
    }

    private static string TempDir() => Path.Combine(Path.GetTempPath(), "pz-databricks-tests", Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task Writes_one_row_group_per_batch_and_rolls_by_size()
    {
        var columns = DbxSchemaMap.Plan(TestSchema, "out");
        var spool = new DbxParquetSpool(TempDir(), columns, withSequence: true, rollBytes: 1);

        using (var b1 = Batch(0, 10)) await spool.WriteBatchAsync(b1, 0, CancellationToken.None);
        using (var b2 = Batch(10, 5)) await spool.WriteBatchAsync(b2, 10, CancellationToken.None);
        var files = await spool.CloseAsync();

        Assert.Equal(2, files.Count);
        Assert.EndsWith("part-00000.parquet", files[0]);
        Assert.EndsWith("part-00001.parquet", files[1]);

        await using var reader = await ParquetReader.CreateAsync(files[1]);
        Assert.Equal(1, reader.RowGroupCount);
        Assert.Equal(["id", "amount", "name", "ts", "_pz_seq"], reader.Schema.DataFields.Select(f => f.Name));
        using var group = reader.OpenRowGroupReader(0);
        var rows = (int)group.RowCount;
        Assert.Equal(5, rows);

        var seq = new long[rows];
        await group.ReadAsync<long>(reader.Schema.DataFields[4], seq, null);
        Assert.Equal([10L, 11L, 12L, 13L, 14L], seq);

        var amount = new string[rows];
        await group.ReadAsync(reader.Schema.DataFields[1], amount, null);
        Assert.All(amount, s => Assert.Equal("12345678901234567890.123456789", s));

        // Proves DateTimeFormat.DateAndTimeMicros was the right fix: DateAndTime (Parquet's legacy
        // millisecond format) would have truncated these sub-millisecond deltas to zero.
        var ts = new DateTime?[rows];
        await group.ReadAsync<DateTime>(reader.Schema.DataFields[3], ts, null);
        var baseTs = new DateTime(2026, 9, 10, 0, 0, 0, DateTimeKind.Utc);
        for (var r = 0; r < rows; r++)
        {
            Assert.Equal(baseTs.AddMicroseconds(10 + r), ts[r]!.Value);
        }

        for (var r = 1; r < rows; r++)
        {
            Assert.Equal(10, (ts[r]!.Value - ts[r - 1]!.Value).Ticks); // one microsecond == 10 ticks
        }

        group.Dispose();
        await reader.DisposeAsync(); // Windows refuses to delete a file a reader still holds open.
        spool.Delete();
        Assert.False(Directory.Exists(spool.Dir));
    }

    [Fact]
    public async Task Without_sequence_the_column_is_absent_and_nulls_survive()
    {
        var columns = DbxSchemaMap.Plan(TestSchema, "out");
        var spool = new DbxParquetSpool(TempDir(), columns, withSequence: false, rollBytes: long.MaxValue);
        using (var b = Batch(0, 8)) await spool.WriteBatchAsync(b, 0, CancellationToken.None);
        var files = await spool.CloseAsync();

        await using var reader = await ParquetReader.CreateAsync(files[0]);
        Assert.Equal(4, reader.Schema.DataFields.Length);
        using var group = reader.OpenRowGroupReader(0);
        var names = new string[(int)group.RowCount];
        await group.ReadAsync(reader.Schema.DataFields[2], names, null);
        Assert.Null(names[0]);
        Assert.Equal("n1", names[1]);
        group.Dispose();
        await reader.DisposeAsync(); // Windows refuses to delete a file a reader still holds open.
        spool.Delete();
    }

    // The commit's target statement reads the uploaded directory by path, so a session that wrote
    // nothing still has to leave one file there: schema-only, no row groups.
    [Fact]
    public async Task An_empty_spool_closes_to_one_schema_only_file()
    {
        var spool = new DbxParquetSpool(TempDir(), DbxSchemaMap.Plan(TestSchema, "out"), false, 1);

        var file = Assert.Single(await spool.CloseAsync());

        Assert.EndsWith("part-00000.parquet", file);
        await using (var reader = await ParquetReader.CreateAsync(file))
        {
            Assert.Equal(0, reader.RowGroupCount);
            Assert.Equal(["id", "amount", "name", "ts"], reader.Schema.DataFields.Select(f => f.Name));
        }

        // Closing again adds nothing: the one file is already there.
        Assert.Single(await spool.CloseAsync());

        spool.Delete();
        Assert.False(Directory.Exists(spool.Dir));
    }

    // Databricks reads a naive Parquet TIMESTAMP(MICROS, isAdjustedToUTC=false) column back as plain
    // TIMESTAMP, indistinguishable from the UTC column, so a timezone-less Arrow timestamp must spool
    // as a string and cast back -- this proves the string actually lands in the file and the cast
    // that reads it back is the one the target statement will run.
    [Fact]
    public async Task Naive_timestamps_spool_as_iso_strings_and_cast_back()
    {
        var naiveSchema = new Schema([new Field("ts", new TimestampType(TimeUnit.Microsecond, (string?)null), true)], null);
        var columns = DbxSchemaMap.Plan(naiveSchema, "out");
        Assert.Equal(typeof(string), columns[0].SpoolField.ClrType);
        Assert.Equal("TIMESTAMP_NTZ", columns[0].CastTo);
        Assert.Equal("cast(`ts` as TIMESTAMP_NTZ) as `ts`", DbxSchemaMap.SelectExpression(columns[0]));

        var builder = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, (string?)null));
        builder.Append(new DateTimeOffset(2026, 9, 10, 13, 45, 30, TimeSpan.Zero).AddMicroseconds(123456));
        builder.AppendNull();
        var array = builder.Build();

        var spool = new DbxParquetSpool(TempDir(), columns, withSequence: false, rollBytes: long.MaxValue);
        using (var batch = new RecordBatch(naiveSchema, [array], 2))
        {
            await spool.WriteBatchAsync(batch, 0, CancellationToken.None);
        }

        var files = await spool.CloseAsync();

        await using var reader = await ParquetReader.CreateAsync(files[0]);
        using var group = reader.OpenRowGroupReader(0);
        var values = new string[2];
        await group.ReadAsync(reader.Schema.DataFields[0], values, null);
        Assert.Equal("2026-09-10T13:45:30.123456", values[0]);
        Assert.Null(values[1]);

        group.Dispose();
        await reader.DisposeAsync(); // Windows refuses to delete a file a reader still holds open.
        spool.Delete();
    }

    // Decimal128Array.GetSqlDecimal and UInt64Array's digit-string rendering both need proof beyond
    // the fixed positive value the other tests use: a negative decimal with scale digits, a null
    // decimal, a uint64 above long.MaxValue (Decimal128's own range, so it cannot round-trip through
    // System.Decimal or long), and a null uint64. A Date32 column rides along since it is otherwise
    // untested by any spool test.
    [Fact]
    public async Task Negative_decimals_nulls_and_large_uint64_round_trip()
    {
        var schema = new Schema([
            new Field("amt", new Decimal128Type(20, 4), true),
            new Field("big", UInt64Type.Default, true),
            new Field("d", Date32Type.Default, true),
        ], null);

        var amounts = new Decimal128Array.Builder(new Decimal128Type(20, 4));
        amounts.Append("-12345.6789");
        amounts.AppendNull();

        var bigs = new UInt64Array.Builder();
        bigs.Append(ulong.MaxValue); // 18446744073709551615, well above long.MaxValue
        bigs.AppendNull();

        var dates = new Date32Array.Builder();
        dates.Append(new DateTime(2026, 9, 10));
        dates.AppendNull();

        var columns = DbxSchemaMap.Plan(schema, "out");
        var spool = new DbxParquetSpool(TempDir(), columns, withSequence: false, rollBytes: long.MaxValue);
        using (var batch = new RecordBatch(schema, [amounts.Build(), bigs.Build(), dates.Build()], 2))
        {
            await spool.WriteBatchAsync(batch, 0, CancellationToken.None);
        }

        var files = await spool.CloseAsync();

        await using var reader = await ParquetReader.CreateAsync(files[0]);
        using var group = reader.OpenRowGroupReader(0);

        var amt = new string[2];
        await group.ReadAsync(reader.Schema.DataFields[0], amt, null);
        Assert.Equal("-12345.6789", amt[0]);
        Assert.Null(amt[1]);

        var big = new string[2];
        await group.ReadAsync(reader.Schema.DataFields[1], big, null);
        Assert.Equal(ulong.MaxValue.ToString(), big[0]);
        Assert.Null(big[1]);

        var d = new DateTime?[2];
        await group.ReadAsync<DateTime>(reader.Schema.DataFields[2], d, null);
        Assert.Equal(new DateTime(2026, 9, 10), d[0]!.Value);
        Assert.Null(d[1]);

        group.Dispose();
        await reader.DisposeAsync(); // Windows refuses to delete a file a reader still holds open.
        spool.Delete();
    }
}
