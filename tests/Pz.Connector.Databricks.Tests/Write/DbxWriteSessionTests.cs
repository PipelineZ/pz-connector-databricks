using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxWriteSessionTests
{
    private static readonly Schema IdName = new([new Field("id", Int64Type.Default, true), new Field("name", StringType.Default, true)], null);

    private static RecordBatch Batch(params (long Id, string Name)[] rows)
    {
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        foreach (var (id, name) in rows) { ids.Append(id); names.Append(name); }
        return new RecordBatch(IdName, [ids.Build(), names.Build()], rows.Length);
    }

    private static DbxConnector Connector(FakeDatabricks fake) =>
        new(null, TimeProvider.System, 128L * 1024 * 1024, () => new HttpClient(fake, disposeHandler: false));

    private static OutputSpec Spec(string mode, params string[] keys) =>
        new("dbx", "orders_out", mode, "fail_on_change", new Dictionary<string, object?>()) { Keys = keys };

    private static async Task CommitAsync(FakeDatabricks fake, OutputSpec spec, params (long, string)[] rows)
    {
        await using var sink = await ((ISinkConnector)Connector(fake)).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(spec, IdName, CancellationToken.None);
        using (var batch = Batch(rows)) await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Append_creates_a_missing_target_then_inserts_from_the_uploaded_directory()
    {
        var fake = new FakeDatabricks();
        await CommitAsync(fake, Spec("append"), (1, "a"), (2, "b"));

        Assert.Equal(2, fake.Tables["main.sales.orders_out"].Rows.Count);
        Assert.Contains(fake.Statements, s => s.StartsWith("create table if not exists `main`.`sales`.`orders_out` (`id` BIGINT, `name` STRING)", StringComparison.Ordinal));
        var insert = Assert.Single(fake.Statements, s => s.StartsWith("insert into", StringComparison.Ordinal));
        Assert.Matches(@"from parquet\.`/Volumes/main/pz/staging/pz/[0-9a-f]{16}/`$", insert);
        Assert.Empty(fake.Uploads);
        Assert.Contains(fake.Requests, r => r.Url.AbsolutePath == "/api/2.1/unity-catalog/tables/main.sales.orders_out");
    }

    [Fact]
    public async Task Append_into_an_existing_target_skips_create_and_checks_columns()
    {
        var fake = new FakeDatabricks();
        fake.Tables["main.sales.orders_out"] = new FakeTable(("id", Int64Type.Default), ("name", StringType.Default), ("extra", StringType.Default));
        await CommitAsync(fake, Spec("append"), (1, "a"));

        Assert.DoesNotContain(fake.Statements, s => s.StartsWith("create table", StringComparison.Ordinal));
        Assert.Single(fake.Tables["main.sales.orders_out"].Rows);
    }

    [Fact]
    public async Task A_target_missing_a_write_column_is_PZDB0304_before_any_statement()
    {
        var fake = new FakeDatabricks();
        fake.Tables["main.sales.orders_out"] = new FakeTable(("id", Int64Type.Default));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => CommitAsync(fake, Spec("append"), (1, "a")));

        Assert.StartsWith("databricks: PZDB0304: output 'orders_out': the target table main.sales.orders_out has no column 'name'", ex.Message);
        Assert.Empty(fake.Statements);
        Assert.Empty(fake.Uploads);
    }

    [Fact]
    public async Task Replace_runs_create_or_replace_and_merge_runs_merge()
    {
        var fake = new FakeDatabricks();
        await CommitAsync(fake, Spec("replace"), (1, "a"), (2, "b"));
        await CommitAsync(fake, Spec("replace"), (3, "c"));
        Assert.Single(fake.Tables["main.sales.orders_out"].Rows);
        Assert.Equal(2, fake.Statements.Count(s => s.StartsWith("create or replace table", StringComparison.Ordinal)));

        await CommitAsync(fake, Spec("merge", "id"), (3, "changed"), (4, "d"));
        var rows = fake.Tables["main.sales.orders_out"].Rows.OrderBy(r => (long)r[0]!).ToList();
        Assert.Equal(2, rows.Count);
        Assert.Equal("changed", rows[0][1]);
        Assert.Contains(fake.Statements, s => s.StartsWith("merge into", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Merge_into_a_missing_target_creates_it_first()
    {
        var fake = new FakeDatabricks();
        await CommitAsync(fake, Spec("merge", "id"), (1, "a"), (1, "b"));
        var rows = fake.Tables["main.sales.orders_out"].Rows;
        Assert.Single(rows);
        Assert.Equal("b", rows[0][1]);
        Assert.Contains(fake.Statements, s => s.StartsWith("create table if not exists", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Empty_commit_still_runs_the_target_statement()
    {
        var fake = new FakeDatabricks();
        await using var sink = await ((ISinkConnector)Connector(fake)).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(Spec("append"), IdName, CancellationToken.None);
        var result = await session.CommitAsync(CancellationToken.None);

        Assert.Equal(0, result.RowsWritten);
        Assert.Contains(fake.Statements, s => s.StartsWith("insert into", StringComparison.Ordinal));
        Assert.Empty(fake.Uploads);
        Assert.Empty(fake.Tables["main.sales.orders_out"].Rows);
    }

    [Fact]
    public async Task Failed_target_statement_is_PZDB0306_and_staging_is_cleaned_up()
    {
        var fake = new FakeDatabricks();
        await using var sink = await ((ISinkConnector)Connector(fake)).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(Spec("append"), IdName, CancellationToken.None);
        using (var batch = Batch((1, "a"))) await session.WriteBatchAsync(batch, CancellationToken.None);
        fake.FailNextStatement = ("BAD_REQUEST", "[DELTA_FAILED] nope");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => session.CommitAsync(CancellationToken.None).AsTask());

        Assert.StartsWith("databricks: PZDB0306: output 'orders_out': statement failed (BAD_REQUEST): [DELTA_FAILED] nope", ex.Message);
        Assert.Empty(fake.Uploads);
    }

    [Fact]
    public async Task Abort_deletes_uploads_and_spool_and_double_commit_is_rejected()
    {
        var fake = new FakeDatabricks();
        await using var sink = await ((ISinkConnector)Connector(fake)).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);
        var session = await sink.BeginWriteAsync(Spec("append"), IdName, CancellationToken.None);
        using (var batch = Batch((1, "a"))) await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.AbortAsync(CancellationToken.None);

        Assert.Empty(fake.Statements);
        await Assert.ThrowsAsync<InvalidOperationException>(() => session.CommitAsync(CancellationToken.None).AsTask());

        var second = await sink.BeginWriteAsync(Spec("append"), IdName, CancellationToken.None);
        await second.CommitAsync(CancellationToken.None);
        await Assert.ThrowsAsync<InvalidOperationException>(() => second.CommitAsync(CancellationToken.None).AsTask());
    }

    /// <summary>Decimal, uint64 and timezone-less timestamp columns spool as strings and are cast
    /// back by the target statement; the decoded value must land as the target column's own type,
    /// not as the text that travelled through Parquet.</summary>
    [Fact]
    public async Task String_spooled_columns_land_as_the_target_column_type()
    {
        var naive = new TimestampType(TimeUnit.Microsecond, (string?)null);
        var when = new DateTimeOffset(2026, 9, 10, 1, 2, 3, TimeSpan.Zero).AddMicroseconds(7);

        var fake = new FakeDatabricks();
        fake.Tables["main.sales.wide"] = new FakeTable(
            ("id", Int64Type.Default), ("amount", StringType.Default), ("big", Int64Type.Default), ("ts", naive));

        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, true),
            new Field("amount", new Decimal128Type(18, 2), true),
            new Field("big", UInt64Type.Default, true),
            new Field("ts", naive, true),
        ], null);

        var ids = new Int64Array.Builder();
        var amounts = new Decimal128Array.Builder(new Decimal128Type(18, 2));
        var bigs = new UInt64Array.Builder();
        var timestamps = new TimestampArray.Builder(naive);
        ids.Append(1);
        amounts.Append("123.45");
        bigs.Append(9007199254740993UL);
        timestamps.Append(when);

        await using var sink = await ((ISinkConnector)Connector(fake)).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(
            new OutputSpec("dbx", "wide", "append", "fail_on_change", new Dictionary<string, object?>()), schema, CancellationToken.None);
        using (var batch = new RecordBatch(schema, [ids.Build(), amounts.Build(), bigs.Build(), timestamps.Build()], 1))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        await session.CommitAsync(CancellationToken.None);

        var row = Assert.Single(fake.Tables["main.sales.wide"].Rows);
        Assert.Equal(1L, row[0]);
        Assert.Equal("123.45", row[1]);
        Assert.Equal(9007199254740993L, row[2]);
        Assert.Equal(when, row[3]);
    }
}
