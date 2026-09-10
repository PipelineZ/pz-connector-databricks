namespace Pz.Connector.Databricks;

/// <summary>PZDB error codes. Every user-facing <c>PzConnectorException</c> message has the shape
/// <c>databricks: PZDB####: &lt;redacted text&gt;</c>. 01xx connection config (validated before any
/// network call), 02xx read, 03xx write, 04xx transport/remote.</summary>
internal static class DbxCodes
{
    // 01xx -- connection config.
    public const string Config_HostInvalid = "PZDB0101";
    public const string Config_WarehouseIdInvalid = "PZDB0102";
    public const string Config_AuthInvalid = "PZDB0103";
    public const string Config_EntityUnresolvable = "PZDB0104";
    public const string Config_BackticksRejected = "PZDB0105";
    public const string Config_StagingVolumeInvalid = "PZDB0106";
    public const string Config_Invalid = "PZDB0107";

    // 02xx -- read.
    public const string Read_EntityAndQuery = "PZDB0201";
    public const string Read_UnsupportedCursorType = "PZDB0202";
    public const string Read_StatementFailed = "PZDB0203";
    public const string Read_ChunkDownloadFailed = "PZDB0204";
    public const string Read_UnknownType = "PZDB0205";
    public const string Read_BadDatasetOption = "PZDB0206";

    // 03xx -- write.
    public const string Write_BadWriteMode = "PZDB0301";
    public const string Write_MergeKeys = "PZDB0302";
    public const string Write_UnsupportedArrowType = "PZDB0303";
    public const string Write_TargetColumnMissing = "PZDB0304";
    public const string Write_UploadFailed = "PZDB0305";
    public const string Write_TargetStatementFailed = "PZDB0306";
    public const string Write_BadOutputOption = "PZDB0307";

    // 04xx -- transport/remote.
    public const string Remote_Unauthorized = "PZDB0401";
    public const string Remote_Transient = "PZDB0402";
    public const string Remote_TokenRefused = "PZDB0403";
    public const string Remote_WarehouseUnavailable = "PZDB0404";

    /// <summary>Every code above, kept in sync by hand: reflection over the consts would need trimmer
    /// annotations to survive Native AOT, for a list that changes only when a code is added.</summary>
    internal static readonly IReadOnlyList<string> All =
    [
        Config_HostInvalid, Config_WarehouseIdInvalid, Config_AuthInvalid, Config_EntityUnresolvable, Config_BackticksRejected,
        Config_StagingVolumeInvalid, Config_Invalid,

        Read_EntityAndQuery, Read_UnsupportedCursorType, Read_StatementFailed, Read_ChunkDownloadFailed, Read_UnknownType,
        Read_BadDatasetOption,

        Write_BadWriteMode, Write_MergeKeys, Write_UnsupportedArrowType, Write_TargetColumnMissing, Write_UploadFailed,
        Write_TargetStatementFailed, Write_BadOutputOption,

        Remote_Unauthorized, Remote_Transient, Remote_TokenRefused, Remote_WarehouseUnavailable,
    ];

    /// <summary>Redaction runs before the prefix goes on, so a secret that happens to contain
    /// "databricks:" cannot shred the one part of the message that is ours.</summary>
    public static string Message(string code, DbxRedactor redactor, string text) => $"databricks: {code}: {redactor.Redact(text)}";
}
