using System.Reflection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>Databricks for pz: a source reads a table or a query result through the SQL Statement
/// Execution API as Arrow; a sink stages Parquet in a Unity Catalog volume and lands rows with one
/// append/replace/merge statement.</summary>
public sealed class DbxConnector : IConnector
{
    private readonly ILoggerFactory _loggerFactory;
    private readonly TimeProvider _time;
    private readonly long _spoolRollBytes;
    private readonly Func<HttpClient> _httpClientFactory;

    public DbxConnector(ILoggerFactory? loggerFactory = null)
        : this(loggerFactory, TimeProvider.System, 128L * 1024 * 1024, static () => new HttpClient(new SocketsHttpHandler
        {
            AutomaticDecompression = System.Net.DecompressionMethods.All,
        }))
    {
    }

    /// <summary>Test-only: supplies the <see cref="HttpClient"/> every REST call goes through, so a
    /// fake handler can stand in for the network, and the clock the poll loop waits on.</summary>
    internal DbxConnector(ILoggerFactory? loggerFactory, TimeProvider time, long spoolRollBytes, Func<HttpClient> httpClientFactory)
    {
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _time = time;
        _spoolRollBytes = spoolRollBytes;
        _httpClientFactory = httpClientFactory;
    }

    public ConnectorInfo Info { get; } = new(
        "databricks",
        typeof(DbxConnector).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "0.0.0",
        ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.ColumnPruning | ConnectorCapabilities.PredicatePushdown | ConnectorCapabilities.BoundedWindow
        | ConnectorCapabilities.InclusiveWatermarkBound | ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.Merge
        | ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Transactional;

    public string ConnectionConfigSchema => """
        { "type": "object", "required": ["host", "warehouse_id", "auth"], "properties": {
            "host": { "type": "string" },
            "warehouse_id": { "type": "string" },
            "auth": { "type": "string", "enum": ["token", "oauth"] },
            "token": { "type": "string" },
            "client_id": { "type": "string" },
            "client_secret": { "type": "string" },
            "catalog": { "type": "string" },
            "schema": { "type": "string" },
            "staging_volume": { "type": "string" } },
          "additionalProperties": false }
        """;

    public string DatasetConfigSchema => """
        { "type": "object", "properties": {
            "entity": { "type": "string" },
            "query": { "type": "string" } },
          "additionalProperties": false }
        """;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        DbxConnectionConfig.Parse(config, errors);
        return ValueTask.FromResult(errors.Count == 0 ? ValidationResult.Success : ValidationResult.Failed([.. errors]));
    }

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(new ConnectionCheck(false, "not implemented"));

    // Redaction-free: Parse never embeds a secret's own text in an error message (every error names a
    // key or a rule), so no redactor built from a successful parse exists yet to route this through.
    internal static DbxConnectionConfig ParseOrThrow(ConnectorConfig config)
    {
        var errors = new List<string>();
        return DbxConnectionConfig.Parse(config, errors)
            ?? throw new PzConnectorException(
                DbxCodes.Message(DbxCodes.Config_Invalid, DbxRedactor.None, string.Join("; ", errors)), isTransient: false);
    }
}
