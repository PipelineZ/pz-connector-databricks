using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Pz.Connector.Databricks.Tests;

/// <summary>An in-memory table the fake workspace serves. Rows are object arrays positionally
/// matching <see cref="Columns"/>; only the handful of Arrow types the suites use are encodable.</summary>
internal sealed class FakeTable(params (string Name, IArrowType Type)[] columns)
{
    private readonly Dictionary<int, (string TypeText, string TypeName)> _databricksTypes = [];

    public (string Name, IArrowType Type)[] Columns { get; } = columns;

    public List<object?[]> Rows { get; } = [];

    /// <summary>When set, every presigned-link GET for a chunk of this table answers with this status.</summary>
    public int? LinkStatus { get; set; }

    /// <summary>Overrides the fake workspace's default chunk size for reads of this table, so one
    /// suite can hold a small table to one chunk and still split a large one many ways.</summary>
    public int? RowsPerChunk { get; set; }

    /// <summary>Declares <paramref name="column"/>'s manifest type as <paramref name="typeText"/>/
    /// <paramref name="typeName"/> instead of the one inferred from its Arrow storage type -- a
    /// complex/interval column is declared with its real Databricks type while its rows hold the
    /// string the connector's own serializing projection would produce.</summary>
    public FakeTable WithDatabricksType(string column, string typeText, string typeName)
    {
        var index = System.Array.FindIndex(Columns, c => c.Name == column);
        if (index < 0)
        {
            throw new ArgumentException($"no such column '{column}'", nameof(column));
        }

        _databricksTypes[index] = (typeText, typeName);
        return this;
    }

    public string TypeText(int column) => _databricksTypes.TryGetValue(column, out var t) ? t.TypeText : Columns[column].Type switch
    {
        Int64Type => "BIGINT",
        Int32Type => "INT",
        StringType => "STRING",
        DoubleType => "DOUBLE",
        BooleanType => "BOOLEAN",
        TimestampType ts => string.IsNullOrEmpty(ts.Timezone) ? "TIMESTAMP_NTZ" : "TIMESTAMP",
        Date32Type => "DATE",
        _ => throw new NotSupportedException(Columns[column].Type.Name),
    };

    public string TypeName(int column) => _databricksTypes.TryGetValue(column, out var t) ? t.TypeName : Columns[column].Type switch
    {
        Int64Type => "LONG",
        Int32Type => "INT",
        StringType => "STRING",
        DoubleType => "DOUBLE",
        BooleanType => "BOOLEAN",
        TimestampType => "TIMESTAMP",
        Date32Type => "DATE",
        _ => throw new NotSupportedException(Columns[column].Type.Name),
    };

    /// <summary>Encodes only <paramref name="projection"/>'s columns, in that order, as an Arrow IPC
    /// stream -- <see langword="null"/> serves every column in declared order (a <c>select *</c>).
    /// A timestamp with a timezone always goes out as <c>Etc/UTC</c>, matching what the real service
    /// puts on the wire regardless of the zone name a test constructed the column with.</summary>
    public byte[] ToArrowStream(IReadOnlyList<object?[]> rows, IReadOnlyList<int>? projection = null)
    {
        var cols = projection ?? Enumerable.Range(0, Columns.Length).ToList();
        var fields = cols.Select(c => new Field(Columns[c].Name, WireType(Columns[c].Type), true)).ToList();
        var schema = new Schema(fields, null);
        var arrays = cols.Select(c => BuildColumn(c, rows)).ToArray();

        using var batch = new RecordBatch(schema, arrays, rows.Count);
        using var ms = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, schema, leaveOpen: true))
        {
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }

        return ms.ToArray();
    }

    private static IArrowType WireType(IArrowType type) =>
        type is TimestampType t && !string.IsNullOrEmpty(t.Timezone) ? new TimestampType(t.Unit, "Etc/UTC") : type;

    private IArrowArray BuildColumn(int c, IReadOnlyList<object?[]> rows)
    {
        switch (Columns[c].Type)
        {
            case Int64Type:
            {
                var b = new Int64Array.Builder();
                foreach (var r in rows) { if (r[c] is null) b.AppendNull(); else b.Append(Convert.ToInt64(r[c])); }
                return b.Build();
            }
            case Int32Type:
            {
                var b = new Int32Array.Builder();
                foreach (var r in rows) { if (r[c] is null) b.AppendNull(); else b.Append(Convert.ToInt32(r[c])); }
                return b.Build();
            }
            case StringType:
            {
                var b = new StringArray.Builder();
                foreach (var r in rows) { if (r[c] is null) b.AppendNull(); else b.Append((string)r[c]!); }
                return b.Build();
            }
            case DoubleType:
            {
                var b = new DoubleArray.Builder();
                foreach (var r in rows) { if (r[c] is null) b.AppendNull(); else b.Append(Convert.ToDouble(r[c])); }
                return b.Build();
            }
            case BooleanType:
            {
                var b = new BooleanArray.Builder();
                foreach (var r in rows) { if (r[c] is null) b.AppendNull(); else b.Append((bool)r[c]!); }
                return b.Build();
            }
            case TimestampType t:
            {
                var b = new TimestampArray.Builder((TimestampType)WireType(t));
                foreach (var r in rows) { if (r[c] is null) b.AppendNull(); else b.Append((DateTimeOffset)r[c]!); }
                return b.Build();
            }
            case Date32Type:
            {
                var b = new Date32Array.Builder();
                foreach (var r in rows) { if (r[c] is null) b.AppendNull(); else b.Append((DateTime)r[c]!); }
                return b.Build();
            }
            default:
                throw new NotSupportedException(Columns[c].Type.Name);
        }
    }
}
