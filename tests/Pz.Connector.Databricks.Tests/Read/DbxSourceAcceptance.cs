using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Databricks.Tests;

/// <summary>TestKit source contract over the in-process fake workspace. <c>small</c> is 120 rows
/// (<c>id BIGINT</c> 0..119, <c>name STRING</c> ~90 chars) at 50 rows per chunk, so it spans three
/// partitions; <c>large</c> is 150 000 rows at 5 000 per chunk so mid-read cancellation is
/// observable; <c>window</c> seeds ids 0..10; <c>flaky</c> is a table whose presigned links always
/// answer 503, so its read fails transiently. Chunk count is server-decided, so no spec can force a
/// single-partition read and the union fact falls back to its re-plan comparison.</summary>
public sealed class DbxSourceAcceptance : SourceConnectorAcceptanceTests
{
    private readonly FakeDatabricks _fake = new();

    public DbxSourceAcceptance()
    {
        _fake.RowsPerChunk = 50;
        var name = new string('a', 90);
        var small = new FakeTable(("id", Int64Type.Default), ("name", StringType.Default));
        for (var i = 0; i < 120; i++) small.Rows.Add([(long)i, name]);
        _fake.Tables["main.sales.small"] = small;

        var large = new FakeTable(("id", Int64Type.Default)) { RowsPerChunk = 5000 };
        for (var i = 0; i < 150_000; i++) large.Rows.Add([(long)i]);
        _fake.Tables["main.sales.large"] = large;

        var window = new FakeTable(("id", Int64Type.Default));
        for (var i = 0; i <= 10; i++) window.Rows.Add([(long)i]);
        _fake.Tables["main.sales.window"] = window;

        var flaky = new FakeTable(("id", Int64Type.Default)) { LinkStatus = 503 };
        flaky.Rows.Add([1L]);
        _fake.Tables["main.sales.flaky"] = flaky;
    }

    protected override ISourceConnector CreateSource() =>
        new DbxConnector(null, TimeProvider.System, 128L * 1024 * 1024, () => new HttpClient(_fake, disposeHandler: false));

    protected override ConnectorConfig ValidConfig => new(_fake.ConnectionConfig());

    protected override DatasetSpec SmallDataset => new("databricks", "small", new Dictionary<string, object?>());

    protected override DatasetSpec? LargeDataset => new("databricks", "large", new Dictionary<string, object?>());

    protected override DatasetSpec? TransientFailureDataset => new("databricks", "flaky", new Dictionary<string, object?>());

    protected override DatasetSpec? BoundedWindowDataset =>
        new DatasetSpec("databricks", "window", new Dictionary<string, object?>()) { WatermarkCursor = "id", WatermarkValue = "3", WatermarkUpperBound = "7" };
}
