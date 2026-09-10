using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>Turns a Databricks failure -- a REST error envelope, a FAILED/CANCELED statement, or a
/// transport exception that never got an answer -- into the engine's exception, classified for retry.
/// Messages always pass the redactor: the service's own diagnostics can echo request context.</summary>
internal static class DbxErrors
{
    /// <summary>Statement error codes for a service-side condition that may clear on its own (a
    /// warehouse still starting, a busy cluster). Everything else is the SQL's or the caller's fault.</summary>
    public static bool IsTransientStatementError(string? errorCode) =>
        errorCode is "TEMPORARILY_UNAVAILABLE" or "RESOURCE_EXHAUSTED" or "DEADLINE_EXCEEDED" or "INTERNAL_ERROR";

    public static PzConnectorException FromHttp(int status, string? errorCode, string? message, TimeSpan? retryAfter,
        DbxRedactor redactor, string context)
    {
        var text = $"{context}: {Describe(status, errorCode, message)}";
        return status switch
        {
            401 or 403 => NonTransient(DbxCodes.Remote_Unauthorized, redactor,
                $"{text} -- check the token or the service principal's permissions on the warehouse and catalog"),
            429 or 502 or 503 or 504 => Transient(DbxCodes.Remote_Transient, redactor, text, retryAfter),
            _ => NonTransient(DbxCodes.Remote_Transient, redactor, text),
        };
    }

    /// <summary>A presigned-link failure. The link is served by cloud storage, not the workspace
    /// control plane: there a 429 or any 5xx is a storage-side condition that clears on its own, so
    /// every one of them is retryable -- unlike the control plane, where a 500 is the answer to the
    /// request and re-sending it changes nothing.</summary>
    public static PzConnectorException FromLink(int status, TimeSpan? retryAfter, DbxRedactor redactor, string context) =>
        Transient(DbxCodes.Remote_Transient, redactor, $"{context}: {Describe(status, null, "presigned chunk download failed")}", retryAfter);

    public static PzConnectorException FromStatement(string? errorCode, string? message, DbxRedactor redactor, string context, string code)
    {
        var text = $"{context}: statement failed{(errorCode is null ? "" : $" ({errorCode})")}{(message is null ? "" : $": {message}")}";
        return IsTransientStatementError(errorCode)
            ? Transient(DbxCodes.Remote_WarehouseUnavailable, redactor, text, retryAfter: null)
            : NonTransient(code, redactor, text);
    }

    /// <summary>A client-side failure before any response arrived -- DNS, refused connection, TLS, a
    /// client-side timeout. <paramref name="ex"/> must not be an <see cref="OperationCanceledException"/>:
    /// the engine's own cancellation is the caller's to rethrow unwrapped, never to publish as a
    /// connector failure, so handing one here is a caller bug and throws loudly.</summary>
    public static PzConnectorException Wrap(Exception ex, DbxRedactor redactor, string context)
    {
        if (ex is OperationCanceledException)
        {
            throw new InvalidOperationException(
                $"{nameof(DbxErrors)}.{nameof(Wrap)} must not be called with {nameof(OperationCanceledException)} -- the caller rethrows cancellation unwrapped.", ex);
        }

        return Transient(DbxCodes.Remote_Transient, redactor, $"{context}: {ex.Message}", retryAfter: null, ex);
    }

    private static string Describe(int status, string? errorCode, string? message) => (errorCode, message) switch
    {
        (null, null) => $"HTTP {status}",
        (not null, null) => $"HTTP {status} ({errorCode})",
        (null, not null) => $"HTTP {status}: {message}",
        _ => $"HTTP {status} ({errorCode}): {message}",
    };

    private static PzConnectorException Transient(string code, DbxRedactor redactor, string text, TimeSpan? retryAfter, Exception? inner = null) =>
        new(DbxCodes.Message(code, redactor, text), isTransient: true, retryAfter: retryAfter, innerException: inner);

    private static PzConnectorException NonTransient(string code, DbxRedactor redactor, string text, Exception? inner = null) =>
        new(DbxCodes.Message(code, redactor, text), isTransient: false, innerException: inner);
}
