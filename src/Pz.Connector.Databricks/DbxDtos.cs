namespace Pz.Connector.Databricks;

// The Databricks REST wire shapes this connector touches. Every member of a response is nullable:
// the service may omit any field, and a partial response must round-trip without a deserialization
// failure. A request record instead declares the fields the API requires as non-nullable and the
// optional ones as nullable, where DbxJsonContext's WhenWritingNull keeps an unset field off the
// wire (absence, never an explicit null, is what is sent).
// Property names are PascalCase; the context's snake_case policy maps each onto its wire name.

internal sealed record DbxParameter(string Name, string? Value, string? Type);

internal sealed record DbxStatementRequest(
    string WarehouseId, string Statement, string Disposition, string Format, string WaitTimeout, string OnWaitTimeout,
    string? Catalog, string? Schema, DbxParameter[]? Parameters);

internal sealed record DbxStatementError(string? ErrorCode, string? Message);

internal sealed record DbxStatus(string? State, DbxStatementError? Error);

internal sealed record DbxColumnInfo(string? Name, string? TypeText, string? TypeName, int? Position);

internal sealed record DbxResultSchema(int? ColumnCount, DbxColumnInfo[]? Columns);

internal sealed record DbxChunkInfo(long? ChunkIndex, long? RowOffset, long? RowCount, long? ByteCount);

internal sealed record DbxManifest(string? Format, DbxResultSchema? Schema, long? TotalChunkCount, DbxChunkInfo[]? Chunks,
    long? TotalRowCount, bool? Truncated);

internal sealed record DbxExternalLink(long? ChunkIndex, long? RowCount, long? ByteCount, string? ExternalLink, string? Expiration,
    Dictionary<string, string>? HttpHeaders);

/// <summary><c>DataArray</c> is set for an INLINE/JSON_ARRAY result, <c>ExternalLinks</c> for an
/// EXTERNAL_LINKS one; the same shape is returned by the chunk endpoint.</summary>
internal sealed record DbxResultData(long? ChunkIndex, long? RowCount, string?[][]? DataArray, DbxExternalLink[]? ExternalLinks);

internal sealed record DbxStatementResponse(string? StatementId, DbxStatus? Status, DbxManifest? Manifest, DbxResultData? Result);

internal sealed record DbxWarehouse(string? Id, string? Name, string? State);

internal sealed record DbxTokenResponse(string? AccessToken, string? TokenType, long? ExpiresIn);

internal sealed record DbxTableInfo(string? Name, string? FullName, string? TableType, DbxColumnInfo[]? Columns);

/// <summary>The envelope every Databricks REST error body uses: <c>{"error_code": ..., "message": ...}</c>.</summary>
internal sealed record DbxErrorEnvelope(string? ErrorCode, string? Message);
