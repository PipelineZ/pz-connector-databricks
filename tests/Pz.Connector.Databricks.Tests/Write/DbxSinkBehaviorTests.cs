using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxSinkBehaviorTests
{
    private static readonly Schema IdName = new([new Field("id", Int64Type.Default, true), new Field("name", StringType.Default, true)], null);

    private static async Task<ISink> OpenAsync(FakeDatabricks fake, Dictionary<string, object?>? config = null)
    {
        var connector = new DbxConnector(null, TimeProvider.System, 128L * 1024 * 1024, () => new HttpClient(fake, disposeHandler: false));
        return await ((ISinkConnector)connector).OpenAsync(new ConnectorConfig(config ?? fake.ConnectionConfig()), CancellationToken.None);
    }

    private static OutputSpec Spec(string mode, string policy = "fail_on_change", params string[] keys) =>
        new("dbx", "out", mode, policy, new Dictionary<string, object?>()) { Keys = keys };

    [Theory]
    [InlineData("upsert", "fail_on_change", "PZDB0301")]
    [InlineData("append", "evolve", "PZDB0307")]
    public async Task Bad_mode_or_policy_is_refused_offline(string mode, string policy, string code)
    {
        var fake = new FakeDatabricks();
        await using var sink = await OpenAsync(fake);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => sink.BeginWriteAsync(Spec(mode, policy), IdName, CancellationToken.None).AsTask());
        Assert.Contains(code, ex.Message);
        Assert.Empty(fake.Requests);
    }

    [Fact]
    public async Task Merge_needs_keys_that_exist_and_never_the_sequence_column()
    {
        var fake = new FakeDatabricks();
        await using var sink = await OpenAsync(fake);
        var noKeys = await Assert.ThrowsAsync<PzConnectorException>(() => sink.BeginWriteAsync(Spec("merge"), IdName, CancellationToken.None).AsTask());
        Assert.Contains("PZDB0302", noKeys.Message);

        var missing = await Assert.ThrowsAsync<PzConnectorException>(() => sink.BeginWriteAsync(Spec("merge", "fail_on_change", "nope"), IdName, CancellationToken.None).AsTask());
        Assert.Contains("'nope'", missing.Message);

        var reserved = new Schema([new Field("_pz_seq", Int64Type.Default, true)], null);
        var seq = await Assert.ThrowsAsync<PzConnectorException>(() => sink.BeginWriteAsync(Spec("append"), reserved, CancellationToken.None).AsTask());
        Assert.Contains("PZDB0302", seq.Message);
        Assert.Contains("_pz_seq", seq.Message);

        // Databricks folds identifiers, so the guard has to as well.
        var shouted = new Schema([new Field("_PZ_SEQ", Int64Type.Default, true)], null);
        var upper = await Assert.ThrowsAsync<PzConnectorException>(() => sink.BeginWriteAsync(Spec("append"), shouted, CancellationToken.None).AsTask());
        Assert.Contains("PZDB0302", upper.Message);
    }

    [Fact]
    public async Task Missing_staging_volume_is_PZDB0106_at_begin()
    {
        var fake = new FakeDatabricks();
        await using var sink = await OpenAsync(fake, fake.ConnectionConfig(stagingVolume: null));
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => sink.BeginWriteAsync(Spec("append"), IdName, CancellationToken.None).AsTask());
        Assert.StartsWith("databricks: PZDB0106: output 'out': 'staging_volume' is required on the connection for any write", ex.Message);
    }

    [Fact]
    public async Task Unsupported_arrow_type_is_PZDB0303_at_begin()
    {
        var fake = new FakeDatabricks();
        await using var sink = await OpenAsync(fake);
        var schema = new Schema([new Field("t", new Time64Type(TimeUnit.Microsecond), true)], null);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => sink.BeginWriteAsync(Spec("append"), schema, CancellationToken.None).AsTask());
        Assert.Contains("PZDB0303", ex.Message);
    }

    [Fact]
    public async Task Disposing_a_sink_disposes_the_http_client_it_was_opened_with()
    {
        var fake = new FakeDatabricks();
        var tracker = new TrackingHandler(fake);
        var connector = new DbxConnector(null, TimeProvider.System, 128L * 1024 * 1024, () => new HttpClient(tracker));
        var sink = await ((ISinkConnector)connector).OpenAsync(new ConnectorConfig(fake.ConnectionConfig()), CancellationToken.None);
        await using (var session = await sink.BeginWriteAsync(Spec("append"), IdName, CancellationToken.None))
        {
            await session.CommitAsync(CancellationToken.None);
        }

        Assert.False(tracker.Disposed);

        await sink.DisposeAsync();

        Assert.True(tracker.Disposed);
    }

    /// <summary>Records that the <see cref="HttpClient"/> wrapping it was disposed -- the only way to
    /// observe from outside that the sink closed the client it was handed.</summary>
    private sealed class TrackingHandler(HttpMessageHandler inner) : DelegatingHandler(inner)
    {
        public bool Disposed { get; private set; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Disposed = true;
            }

            base.Dispose(disposing);
        }
    }
}
