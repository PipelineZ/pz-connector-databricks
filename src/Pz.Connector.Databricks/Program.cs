using Pz.Connector.Databricks;
using Pz.Connectors.Sdk;

return await PzConnectorHost.RunAsync(args, ctx => new DbxConnector(ctx.LoggerFactory)).ConfigureAwait(false);
