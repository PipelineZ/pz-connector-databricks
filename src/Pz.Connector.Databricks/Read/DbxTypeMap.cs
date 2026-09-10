using System.Globalization;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>Databricks SQL types (as the Statement API's manifest names them) to Arrow, and the
/// parameter type name a cursor column's Arrow type takes. The manifest's <c>type_text</c> is the
/// authority: <c>type_name</c> collapses <c>TIMESTAMP</c>/<c>TIMESTAMP_NTZ</c> and carries no
/// decimal precision. Complex types arrive from the Statement API as JSON strings, so they map to
/// utf8 rather than to Arrow list/map/struct.</summary>
internal static partial class DbxTypeMap
{
    public static Schema ToArrowSchema(DbxResultSchema schema)
    {
        var columns = schema.Columns ?? [];
        var fields = columns
            .OrderBy(c => c.Position ?? 0)
            .Select(c => new Field(c.Name ?? "", ToArrow(c.TypeText ?? "", c.TypeName ?? "", c.Name ?? ""), true))
            .ToList();
        return new Schema(fields, null);
    }

    public static IArrowType ToArrow(string typeText, string typeName, string column)
    {
        var text = typeText.Trim().ToUpperInvariant();
        var head = text.Split('(', '<', ' ')[0];
        switch (head)
        {
            case "BIGINT" or "LONG": return Int64Type.Default;
            case "INT" or "INTEGER": return Int32Type.Default;
            case "SMALLINT" or "SHORT": return Int16Type.Default;
            case "TINYINT" or "BYTE": return Int8Type.Default;
            case "BOOLEAN": return BooleanType.Default;
            case "FLOAT" or "REAL": return FloatType.Default;
            case "DOUBLE": return DoubleType.Default;
            case "STRING" or "CHAR" or "VARCHAR" or "VARIANT" or "INTERVAL" or "ARRAY" or "MAP" or "STRUCT": return StringType.Default;
            case "BINARY": return BinaryType.Default;
            case "DATE": return Date32Type.Default;
            case "TIMESTAMP": return new TimestampType(TimeUnit.Microsecond, "UTC");
            case "TIMESTAMP_NTZ": return new TimestampType(TimeUnit.Microsecond, (string?)null);
            case "DECIMAL" or "DEC" or "NUMERIC":
                var m = DecimalPattern().Match(text);
                var precision = m.Success ? int.Parse(m.Groups["p"].Value, CultureInfo.InvariantCulture) : 10;
                var scale = m.Success && m.Groups["s"].Success ? int.Parse(m.Groups["s"].Value, CultureInfo.InvariantCulture) : 0;
                return new Decimal128Type(precision, scale);
            default:
                throw new PzConnectorException(
                    DbxCodes.Message(DbxCodes.Read_UnknownType, DbxRedactor.None,
                        $"column '{column}' has Databricks type '{typeText}' ({typeName}), which this connector cannot map to Arrow -- cast it in a query: read"),
                    isTransient: false);
        }
    }

    public static string? ParameterType(IArrowType type) => type switch
    {
        Int64Type => "BIGINT",
        Int32Type => "INT",
        Int16Type => "SMALLINT",
        Int8Type => "TINYINT",
        Decimal128Type d => $"DECIMAL({d.Precision},{d.Scale})",
        Date32Type or Date64Type => "DATE",
        TimestampType t => string.IsNullOrEmpty(t.Timezone) ? "TIMESTAMP_NTZ" : "TIMESTAMP",
        StringType => "STRING",
        _ => null,
    };

    [GeneratedRegex(@"^(?:DECIMAL|DEC|NUMERIC)\s*\(\s*(?<p>\d+)\s*(?:,\s*(?<s>\d+)\s*)?\)$")]
    private static partial Regex DecimalPattern();
}
