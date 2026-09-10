using System.Text.Json.Serialization;

namespace Pz.Connector.Databricks;

// Every JSON call passes DbxJsonContext.Default.<Type> explicitly -- no reflection-based
// serialization survives Native AOT trimming. snake_case is the Databricks wire naming.
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.SnakeCaseLower, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(DbxParameter))]
[JsonSerializable(typeof(DbxStatementRequest))]
[JsonSerializable(typeof(DbxStatementError))]
[JsonSerializable(typeof(DbxStatus))]
[JsonSerializable(typeof(DbxColumnInfo))]
[JsonSerializable(typeof(DbxResultSchema))]
[JsonSerializable(typeof(DbxChunkInfo))]
[JsonSerializable(typeof(DbxManifest))]
[JsonSerializable(typeof(DbxExternalLink))]
[JsonSerializable(typeof(DbxResultData))]
[JsonSerializable(typeof(DbxStatementResponse))]
[JsonSerializable(typeof(DbxWarehouse))]
[JsonSerializable(typeof(DbxTokenResponse))]
[JsonSerializable(typeof(DbxTableInfo))]
[JsonSerializable(typeof(DbxErrorEnvelope))]
[JsonSerializable(typeof(string?[][]))]
internal sealed partial class DbxJsonContext : JsonSerializerContext;
