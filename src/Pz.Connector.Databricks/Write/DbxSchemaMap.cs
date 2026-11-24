using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet.Schema;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>One write column: how it is spooled to Parquet, the Databricks type a created target
/// gets, and the cast the target statement applies when the spooled representation is a string.
///
/// <para>A nested column (<see cref="FromJson"/>) is spooled as JSON text and rebuilt in the target
/// statement with <c>from_json</c> against its declared type, so Parquet's repetition-level encoding is
/// never involved.</para></summary>
internal sealed record DbxColumnPlan(string Name, DataField SpoolField, string DatabricksType, string? CastTo, bool FromJson = false);

/// <summary>Arrow to Parquet spool to Databricks. Parquet's own logical types carry every plain
/// type exactly; decimals cannot, because the Parquet writer routes them through
/// <see cref="decimal"/> (28 digits), so decimal128 and uint64 are spooled as digit strings and
/// cast back in SQL. Every spool column is nullable: a non-null Arrow column still lands through a
/// nullable Parquet column, and the target's own nullability is its business.</summary>
internal static class DbxSchemaMap
{
    public const string SequenceColumn = "_pz_seq";

    /// <summary>Databricks reads a Parquet TIMESTAMP(MICROS, isAdjustedToUTC=false) column back as
    /// plain TIMESTAMP, indistinguishable from a UTC column -- a timezone-less Arrow timestamp
    /// cannot rely on Parquet's own logical type to keep "naive" meaning and instead spools as an
    /// ISO 8601 string, cast back to TIMESTAMP_NTZ by the target statement.</summary>
    public const bool NaiveTimestampAsString = true;

    public static DataField SequenceField { get; } = new(SequenceColumn, typeof(long), isNullable: false);

    public static IReadOnlyList<DbxColumnPlan> Plan(Schema arrow, string output) =>
        arrow.FieldsList.Select(f => PlanField(f, output)).ToList();

    public static string SelectExpression(DbxColumnPlan c) =>
        c.FromJson ? $"from_json({DbxSql.Q(c.Name)}, '{EscapeLiteral(c.DatabricksType)}') as {DbxSql.Q(c.Name)}"
        : c.CastTo is null ? DbxSql.Q(c.Name) : $"cast({DbxSql.Q(c.Name)} as {c.CastTo}) as {DbxSql.Q(c.Name)}";

    private static DbxColumnPlan PlanField(Apache.Arrow.Field f, string output)
    {
        var n = f.Name;
        switch (f.DataType)
        {
            case Int8Type or Int16Type or Int32Type or UInt8Type or UInt16Type:
                return new DbxColumnPlan(n, new DataField(n, typeof(int), true), "INT", null);
            case Int64Type or UInt32Type:
                return new DbxColumnPlan(n, new DataField(n, typeof(long), true), "BIGINT", null);
            case UInt64Type:
                return new DbxColumnPlan(n, new DataField(n, typeof(string), true), "DECIMAL(20,0)", "DECIMAL(20,0)");
            case FloatType:
                return new DbxColumnPlan(n, new DataField(n, typeof(float), true), "FLOAT", null);
            case DoubleType:
                return new DbxColumnPlan(n, new DataField(n, typeof(double), true), "DOUBLE", null);
            // Decimal256Type/Decimal32Type/Decimal64Type all derive from FixedSizeBinaryType, same as
            // Decimal128Type -- they must be rejected before the generic binary arm below would
            // otherwise treat them as opaque bytes.
            case Decimal256Type or Decimal32Type or Decimal64Type:
                throw Unsupported(output, n, f.DataType);
            case Decimal128Type d:
                var decimalType = $"DECIMAL({d.Precision.ToString(CultureInfo.InvariantCulture)},{d.Scale.ToString(CultureInfo.InvariantCulture)})";
                return new DbxColumnPlan(n, new DataField(n, typeof(string), true), decimalType, decimalType);
            case StringType or LargeStringType:
                return new DbxColumnPlan(n, new DataField(n, typeof(string), true), "STRING", null);
            case BinaryType or LargeBinaryType or FixedSizeBinaryType:
                return new DbxColumnPlan(n, new DataField(n, typeof(byte[]), true), "BINARY", null);
            case BooleanType:
                return new DbxColumnPlan(n, new DataField(n, typeof(bool), true), "BOOLEAN", null);
            case Date32Type or Date64Type:
                return new DbxColumnPlan(n, new DateTimeDataField(n, DateTimeFormat.Date, isNullable: true), "DATE", null);
            case TimestampType t when !string.IsNullOrEmpty(t.Timezone):
                // DateAndTime is Parquet's legacy millisecond format; Parquet.Net forces its Unit to
                // Millis regardless of what is passed for `unit` -- DateAndTimeMicros is the format
                // that actually keeps microsecond precision.
                return new DbxColumnPlan(n,
                    new DateTimeDataField(n, DateTimeFormat.DateAndTimeMicros, isAdjustedToUTC: true, isNullable: true),
                    "TIMESTAMP", null);
            case TimestampType:
                return NaiveTimestampAsString
                    ? new DbxColumnPlan(n, new DataField(n, typeof(string), true), "TIMESTAMP_NTZ", "TIMESTAMP_NTZ")
                    : new DbxColumnPlan(n,
                        new DateTimeDataField(n, DateTimeFormat.DateAndTimeMicros, isAdjustedToUTC: false, isNullable: true),
                        "TIMESTAMP_NTZ", null);
            case ListType or FixedSizeListType or StructType or MapType:
                return new DbxColumnPlan(n, new DataField(n, typeof(string), true), NestedTypeName(f.DataType, output, n), null, FromJson: true);
            default:
                throw Unsupported(output, n, f.DataType);
        }
    }

    /// <summary>The Databricks DDL spelling of a nested Arrow type. <c>from_json</c> reads a map only with
    /// string keys, and the engine carries only the flat leaf types below; anything else is refused here,
    /// before a byte is spooled.</summary>
    private static string NestedTypeName(IArrowType type, string output, string column) => type switch
    {
        Int32Type => "INT",
        Int64Type => "BIGINT",
        DoubleType => "DOUBLE",
        Decimal128Type d => $"DECIMAL({d.Precision.ToString(CultureInfo.InvariantCulture)},{d.Scale.ToString(CultureInfo.InvariantCulture)})",
        StringType => "STRING",
        BooleanType => "BOOLEAN",
        Date32Type => "DATE",
        TimestampType t when !string.IsNullOrEmpty(t.Timezone) => "TIMESTAMP",
        ListType l => $"ARRAY<{NestedTypeName(l.ValueDataType, output, column)}>",
        FixedSizeListType l => $"ARRAY<{NestedTypeName(l.ValueDataType, output, column)}>",
        MapType m when m.KeyField.DataType is StringType =>
            $"MAP<STRING, {NestedTypeName(m.ValueField.DataType, output, column)}>",
        StructType s => $"STRUCT<{string.Join(", ", s.Fields.Select(f => $"{DbxSql.Q(f.Name)}: {NestedTypeName(f.DataType, output, column)}"))}>",
        _ => throw new PzConnectorException(
            DbxCodes.Message(DbxCodes.Write_UnsupportedArrowType, DbxRedactor.None,
                $"{output}: column '{column}' has a nested type containing '{type.Name}', which Databricks cannot take here (map keys must be varchar; leaves are int, bigint, double, decimal, varchar, boolean, date, timestamp)"),
            isTransient: false),
    };

    /// <summary>A type string as the body of a single-quoted Databricks literal.</summary>
    private static string EscapeLiteral(string text) => text.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("'", "\\'", StringComparison.Ordinal);

    private static PzConnectorException Unsupported(string output, string column, IArrowType type) =>
        new(DbxCodes.Message(DbxCodes.Write_UnsupportedArrowType, DbxRedactor.None,
                $"{output}: column '{column}' has unsupported Arrow type '{type.Name}'"),
            isTransient: false);
}
