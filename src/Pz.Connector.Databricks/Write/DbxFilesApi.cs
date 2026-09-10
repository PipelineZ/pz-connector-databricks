using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>The session's corner of the staging volume: <c>&lt;volume&gt;/pz/&lt;tag&gt;/</c>, where
/// the tag is unique per session so concurrent sinks never share a directory. Cleanup is best
/// effort by construction: a leftover file costs storage, never correctness, because the target
/// statement names this directory alone.</summary>
internal sealed class DbxFilesApi(DbxRestClient rest, TableRef volume, string tag)
{
    public string Dir { get; } = $"{volume.VolumePath}/pz/{tag}";

    public async Task<string> UploadAsync(string localFile, int index, string context, CancellationToken ct)
    {
        var name = $"part-{index:D5}.parquet";
        var path = $"{Dir}/{name}";
        var uploadContext = $"{context}: uploading {name}";
        await using var stream = new FileStream(localFile, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024, FileOptions.Asynchronous);
        try
        {
            await rest.UploadFileAsync(path, stream, stream.Length, uploadContext, ct).ConfigureAwait(false);
        }
        catch (PzConnectorException ex)
        {
            throw new PzConnectorException(
                DbxCodes.Message(DbxCodes.Write_UploadFailed, rest.Redactor, $"{uploadContext} to {Dir} failed: {AfterCode(ex.Message)}"),
                ex.IsTransient, ex.RetryAfter, ex);
        }

        return path;
    }

    public async Task DeleteAllAsync(IReadOnlyList<string> uploaded, ILogger logger)
    {
        foreach (var path in uploaded)
        {
            try
            {
                await rest.DeleteFileAsync(path, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "databricks: could not delete staged file {Path}", path);
            }
        }

        try
        {
            await rest.DeleteDirectoryAsync(Dir, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "databricks: could not delete staging directory {Dir}", Dir);
        }
    }

    // The inner message is "databricks: PZDB####: <text>"; keep only the text so the code is not repeated.
    private static string AfterCode(string message)
    {
        const string prefix = "databricks: PZDB";
        if (!message.StartsWith(prefix, StringComparison.Ordinal))
        {
            return message;
        }

        var idx = message.IndexOf(": ", prefix.Length, StringComparison.Ordinal);
        return idx < 0 ? message : message[(idx + 2)..];
    }
}
