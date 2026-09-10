using Pz.Connector.Databricks;
using Pz.Connectors.Sdk;

return await PzConnectorHost.RunAsync(args, ctx => new DbxConnector(ctx.LoggerFactory)).ConfigureAwait(false);

namespace Pz.Connector.Databricks
{
    /// <summary>Placeholder satisfying <c>IConnector</c> until Task 5 lands the real Databricks
    /// connector; every member here is replaced, not extended.</summary>
    internal sealed class DbxConnector : Pz.Connectors.Abstractions.IConnector
    {
        public DbxConnector(Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory)
        {
        }

        public Pz.Connectors.Abstractions.ConnectorInfo Info { get; } = new("databricks", "0.0.0", Pz.Connectors.Abstractions.ProtocolVersion.Major);
        public Pz.Connectors.Abstractions.ConnectorCapabilities Capabilities => Pz.Connectors.Abstractions.ConnectorCapabilities.None;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";
        public ValueTask<Pz.Connectors.Abstractions.ValidationResult> ValidateAsync(Pz.Connectors.Abstractions.ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult(Pz.Connectors.Abstractions.ValidationResult.Success);
        public ValueTask<Pz.Connectors.Abstractions.ConnectionCheck> CheckConnectionAsync(Pz.Connectors.Abstractions.ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult(new Pz.Connectors.Abstractions.ConnectionCheck(false, "not implemented"));
    }
}
