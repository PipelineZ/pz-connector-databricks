using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxFilesApiTests
{
    private static (FakeDatabricks Fake, DbxRestClient Client) Setup()
    {
        var fake = new FakeDatabricks();
        var cfg = DbxConnectionConfig.Parse(new ConnectorConfig(fake.ConnectionConfig()), [])!;
        return (fake, new DbxRestClient(new HttpClient(fake), cfg, new StaticTokenSource("fake-token"), cfg.Redactor, NullLogger.Instance));
    }

    [Fact]
    public async Task Uploads_under_the_tagged_directory_and_deletes_everything()
    {
        var (fake, rest) = Setup();
        var files = new DbxFilesApi(rest, new TableRef("main", "pz", "staging"), "t1");
        var local = Path.GetTempFileName();
        await File.WriteAllBytesAsync(local, [1, 2, 3]);

        var path = await files.UploadAsync(local, 0, "ctx", CancellationToken.None);

        Assert.Equal("/Volumes/main/pz/staging/pz/t1/part-00000.parquet", path);
        Assert.Equal(new byte[] { 1, 2, 3 }, fake.Uploads[path]);

        await files.DeleteAllAsync([path], NullLogger.Instance);
        Assert.Empty(fake.Uploads);
        Assert.Contains(fake.Requests, r => r.Method == HttpMethod.Delete && r.Url.AbsolutePath == "/api/2.0/fs/directories/Volumes/main/pz/staging/pz/t1");
    }

    [Fact]
    public async Task Upload_failure_is_PZDB0305_keeping_transience()
    {
        var (fake, rest) = Setup();
        fake.RateLimitNextRequests = 1;
        var files = new DbxFilesApi(rest, new TableRef("main", "pz", "staging"), "t1");
        var local = Path.GetTempFileName();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => files.UploadAsync(local, 0, "output 'x'", CancellationToken.None));

        Assert.StartsWith("databricks: PZDB0305: output 'x': uploading part-00000.parquet", ex.Message);
        Assert.True(ex.IsTransient);
    }
}
