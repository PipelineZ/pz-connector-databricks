using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Databricks.Tests;

/// <summary>TestKit sink contract over the in-process fake workspace. Fresh target names per
/// instance (xunit constructs one per fact) so facts never share state; read-back decodes the
/// fake's table rows into one batch.</summary>
public sealed class DbxSinkAcceptance : SinkConnectorAcceptanceTests
{
    private readonly FakeDatabricks _fake = new();
    private readonly string _append = $"append_{Guid.NewGuid():N}";
    private readonly string _merge = $"merge_{Guid.NewGuid():N}";
    private readonly string _replace = $"replace_{Guid.NewGuid():N}";

    protected override ISinkConnector CreateSink() =>
        new DbxConnector(null, TimeProvider.System, 128L * 1024 * 1024, () => new HttpClient(_fake, disposeHandler: false));

    protected override ConnectorConfig ValidConfig => new(_fake.ConnectionConfig());

    protected override OutputSpec SmallOutput => new("databricks", _append, "append", "fail_on_change", new Dictionary<string, object?>());

    protected override OutputSpec? MergeOutput =>
        new OutputSpec("databricks", _merge, "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] };

    protected override Task ResetMergeTargetAsync()
    {
        _fake.Tables.Remove($"main.sales.{_merge}");
        return Task.CompletedTask;
    }

    protected override OutputSpec? ReplaceOutput => new("databricks", _replace, "replace", "fail_on_change", new Dictionary<string, object?>());

    protected override ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        if (!_fake.Tables.TryGetValue($"main.sales.{spec.Output}", out var table) || table.Rows.Count == 0)
        {
            return ValueTask.FromResult<IReadOnlyList<RecordBatch>>([]);
        }

        var idIndex = System.Array.FindIndex(table.Columns, c => c.Name == "id");
        var nameIndex = System.Array.FindIndex(table.Columns, c => c.Name == "name");
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        foreach (var row in table.Rows.OrderBy(r => Convert.ToInt64(r[idIndex])))
        {
            ids.Append(Convert.ToInt64(row[idIndex]));
            names.Append((string)row[nameIndex]!);
        }

        var schema = new Schema([new Field("id", Int64Type.Default, false), new Field("name", StringType.Default, false)], null);
        return ValueTask.FromResult<IReadOnlyList<RecordBatch>>([new RecordBatch(schema, [ids.Build(), names.Build()], table.Rows.Count)]);
    }
}
