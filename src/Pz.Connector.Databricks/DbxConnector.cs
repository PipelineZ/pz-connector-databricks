using System.Reflection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>Databricks for pz: a source reads a table or a query result through the SQL Statement
/// Execution API as Arrow; a sink stages Parquet in a Unity Catalog volume and lands rows with one
/// append/replace/merge statement.</summary>
public sealed class DbxConnector : IConnector, ISourceConnector, ISinkConnector
{
    private readonly ILoggerFactory _loggerFactory;
    private readonly TimeProvider _time;
    private readonly long _spoolRollBytes;
    private readonly Func<HttpClient> _httpClientFactory;

    public DbxConnector(ILoggerFactory? loggerFactory = null)
        : this(loggerFactory, TimeProvider.System, 128L * 1024 * 1024, CreateHttpClient)
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

    /// <summary>The client every request of one opened source or sink goes through. It carries no
    /// overall timeout: a spool upload or a chunk download is arbitrarily large and a fixed deadline
    /// would cut a healthy transfer, so the engine's <see cref="CancellationToken"/> -- threaded
    /// through every call on this connector -- is the only cutoff. A connect timeout stays, so a
    /// black-holed host still fails instead of hanging before a single byte moves.</summary>
    internal static HttpClient CreateHttpClient() =>
        new(new SocketsHttpHandler
        {
            AutomaticDecompression = System.Net.DecompressionMethods.All,
            ConnectTimeout = TimeSpan.FromSeconds(30),
        })
        {
            Timeout = Timeout.InfiniteTimeSpan,
        };

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

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var connection = ParseOrThrow(config);
        var httpClient = _httpClientFactory();
        var (_, rest) = OpenRest(connection, httpClient);

        // The client (and with it the connection pool behind its handler) belongs to the source from
        // here on: the engine disposes the source exactly once, and that is what closes it.
        return ValueTask.FromResult<ISource>(
            new DbxSource(connection, rest, _time, _loggerFactory.CreateLogger<DbxSource>(), httpClient));
    }

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var connection = ParseOrThrow(config);
        var httpClient = _httpClientFactory();
        var (_, rest) = OpenRest(connection, httpClient);

        // The client (and with it the connection pool behind its handler) belongs to the sink from
        // here on: the engine disposes the sink exactly once, and that is what closes it.
        return ValueTask.FromResult<ISink>(
            new DbxSink(connection, rest, _time, _loggerFactory.CreateLogger<DbxSink>(), _spoolRollBytes, httpClient));
    }

    /// <summary>Proves the credential and the warehouse: the warehouse lookup needs a valid token,
    /// and a running warehouse additionally answers <c>select 1</c>. A stopped warehouse is not a
    /// failure -- it starts on the first statement -- but it is worth telling the user about.</summary>
    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        var connection = DbxConnectionConfig.Parse(config, errors);
        if (connection is null)
        {
            return new ConnectionCheck(false, string.Join("; ", errors));
        }

        using var httpClient = _httpClientFactory();
        try
        {
            var (_, rest) = OpenRest(connection, httpClient);
            var warehouse = await rest.GetWarehouseAsync("checking the warehouse", ct).ConfigureAwait(false);
            var name = warehouse.Name ?? connection.WarehouseId;
            switch (warehouse.State)
            {
                case "RUNNING":
                    await DbxStatement.ExecuteRowsAsync(rest, connection, "select 1", null, "checking the warehouse",
                        DbxCodes.Remote_WarehouseUnavailable, _time, _loggerFactory.CreateLogger<DbxConnector>(), ct).ConfigureAwait(false);
                    return new ConnectionCheck(true, $"warehouse {name} RUNNING");
                case "STOPPED" or "STARTING" or "STOPPING":
                    return new ConnectionCheck(true, $"warehouse {name} is {warehouse.State}; it starts on the first statement");
                default:
                    return new ConnectionCheck(false, $"warehouse {name} is {warehouse.State ?? "in an unknown state"}");
            }
        }
        catch (PzConnectorException ex)
        {
            return new ConnectionCheck(false, ex.Message);
        }
    }

    private (IDbxTokenSource Tokens, DbxRestClient Client) OpenRest(DbxConnectionConfig connection, HttpClient httpClient)
    {
        var tokens = DbxAuth.Create(connection, httpClient, _time);
        var rest = new DbxRestClient(httpClient, connection, tokens, connection.Redactor, _loggerFactory.CreateLogger<DbxRestClient>());
        return (tokens, rest);
    }

    // Parse codes each error where it is raised, so joining them adds no second prefix -- the same
    // shape a dataset's or an output's config errors reach the caller in.
    internal static DbxConnectionConfig ParseOrThrow(ConnectorConfig config)
    {
        var errors = new List<string>();
        return DbxConnectionConfig.Parse(config, errors)
            ?? throw new PzConnectorException(string.Join("; ", errors), isTransient: false);
    }
}
