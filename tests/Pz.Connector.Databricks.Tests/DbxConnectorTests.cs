using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxConnectorTests
{
    [Fact]
    public void Info_and_capabilities_are_declared()
    {
        var c = new DbxConnector();

        Assert.Equal("databricks", c.Info.Name);
        Assert.Equal(ProtocolVersion.Major, c.Info.ProtocolMajor);
        var expected = ConnectorCapabilities.ColumnPruning | ConnectorCapabilities.PredicatePushdown | ConnectorCapabilities.BoundedWindow
            | ConnectorCapabilities.InclusiveWatermarkBound | ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.Merge
            | ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Transactional;
        Assert.Equal(expected, c.Capabilities);
    }

    [Fact]
    public async Task ValidateAsync_aggregates_every_error()
    {
        var c = new DbxConnector();
        var result = await c.ValidateAsync(new ConnectorConfig(new Dictionary<string, object?> { ["auth"] = "nope" }), CancellationToken.None);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("'host' is required"));
        Assert.Contains(result.Errors, e => e.Contains("'warehouse_id' is required"));
        Assert.Contains(result.Errors, e => e.Contains("'auth' must be one of"));
    }

    [Fact]
    public void ParseOrThrow_wraps_config_errors_in_PZDB0107()
    {
        var ex = Assert.Throws<PzConnectorException>(() => DbxConnector.ParseOrThrow(new ConnectorConfig(new Dictionary<string, object?>())));

        Assert.StartsWith("databricks: PZDB0107: ", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Schemas_are_closed_objects()
    {
        var c = new DbxConnector();
        Assert.Contains("\"additionalProperties\": false", c.ConnectionConfigSchema);
        Assert.Contains("\"additionalProperties\": false", c.DatasetConfigSchema);
        Assert.Contains("\"staging_volume\"", c.ConnectionConfigSchema);
        Assert.Contains("\"query\"", c.DatasetConfigSchema);
    }
}
