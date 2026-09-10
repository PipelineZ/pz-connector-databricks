using System.Globalization;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>Databricks SQL types (as the Statement API's manifest names them) to Arrow, and the
/// parameter type name a cursor column's Arrow type takes. The manifest's <c>type_text</c> is the
/// authority: <c>type_name</c> collapses <c>TIMESTAMP</c>/<c>TIMESTAMP_NTZ</c> and carries no
/// decimal precision. The service returns native Arrow nested types for <c>ARRAY</c>/<c>MAP</c>/
/// <c>STRUCT</c> and a native duration/interval for <c>INTERVAL</c>, not the utf8 this connector
/// declares for them, so those columns must be serialized in the statement the connector runs --
/// see <see cref="SerializeExpression"/> and <see cref="ToReadSchema"/>.</summary>
internal static partial class DbxTypeMap
{
    public static Schema ToArrowSchema(DbxResultSchema schema) => ToReadSchema(schema).Schema;

    /// <summary>The Arrow schema a read declares, plus each column's serialize expression (or
    /// <see langword="null"/>) in manifest order.</summary>
    public static DbxReadSchema ToReadSchema(DbxResultSchema schema)
    {
        var columns = (schema.Columns ?? []).OrderBy(c => c.Position ?? 0).ToList();
        var fields = new List<Field>(columns.Count);
        var readColumns = new List<DbxReadColumn>(columns.Count);
        foreach (var c in columns)
        {
            var name = c.Name ?? "";
            var typeText = c.TypeText ?? "";
            var typeName = c.TypeName ?? "";
            fields.Add(new Field(name, ToArrow(typeText, typeName, name), true));
            readColumns.Add(new DbxReadColumn(name, SerializeExpression(typeText, typeName, DbxSql.Q(name))));
        }

        return new DbxReadSchema(new Schema(fields, null), readColumns);
    }

    /// <summary>The projection this column's declared type needs so the statement's result agrees
    /// with the utf8 this connector declares for it: <c>to_json(...)</c> for <c>ARRAY</c>/<c>MAP</c>/
    /// <c>STRUCT</c>, <c>cast(... as string)</c> for <c>INTERVAL</c>, <see langword="null"/> for
    /// everything else (a bare column reference is enough).</summary>
    public static string? SerializeExpression(string typeText, string typeName, string quotedColumn)
    {
        var text = typeText.Trim();
        var source = text.Length > 0 ? text : typeName.Trim();
        var head = source.ToUpperInvariant().Split('(', '<', ' ')[0];
        return head switch
        {
            "ARRAY" or "MAP" or "STRUCT" => $"to_json({quotedColumn})",
            "INTERVAL" => $"cast({quotedColumn} as string)",
            _ => null,
        };
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
            case "TIMESTAMP": return new TimestampType(TimeUnit.Microsecond, "Etc/UTC");
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

/// <summary>The Arrow schema a read declares, plus per-column the expression (if any) the statement
/// must project that column through so the wire result agrees with the declared utf8 type.</summary>
internal sealed record DbxReadSchema(Schema Schema, IReadOnlyList<DbxReadColumn> Columns)
{
    public bool HasSerializedColumns => Columns.Any(c => c.SerializeExpression is not null);
}

internal sealed record DbxReadColumn(string Name, string? SerializeExpression);
