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
        spool.Delete();
    }

    [Fact]
    public async Task An_empty_spool_closes_to_no_files_and_never_touches_disk()
    {
        var spool = new DbxParquetSpool(TempDir(), DbxSchemaMap.Plan(TestSchema, "out"), false, 1);
        Assert.Empty(await spool.CloseAsync());
        Assert.False(Directory.Exists(spool.Dir));
        spool.Delete();
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

        spool.Delete();
    }
}
