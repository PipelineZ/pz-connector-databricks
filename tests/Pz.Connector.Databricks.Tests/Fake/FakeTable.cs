using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Pz.Connector.Databricks.Tests;

/// <summary>An in-memory table the fake workspace serves. Rows are object arrays positionally
/// matching <see cref="Columns"/>; only the handful of Arrow types the suites use are encodable.</summary>
internal sealed class FakeTable(params (string Name, IArrowType Type)[] columns)
{
    public (string Name, IArrowType Type)[] Columns { get; } = columns;

    public List<object?[]> Rows { get; } = [];

    /// <summary>When set, every presigned-link GET for a chunk of this table answers with this status.</summary>
    public int? LinkStatus { get; set; }

    /// <summary>Overrides the fake workspace's default chunk size for reads of this table, so one
    /// suite can hold a small table to one chunk and still split a large one many ways.</summary>
    public int? RowsPerChunk { get; set; }

    public string TypeText(int column) => Columns[column].Type switch
    {
        Int64Type => "BIGINT",
        Int32Type => "INT",
        StringType => "STRING",
        DoubleType => "DOUBLE",
        BooleanType => "BOOLEAN",
        TimestampType t => string.IsNullOrEmpty(t.Timezone) ? "TIMESTAMP_NTZ" : "TIMESTAMP",
        Date32Type => "DATE",
        _ => throw new NotSupportedException(Columns[column].Type.Name),
    };

    public string TypeName(int column) => Columns[column].Type switch
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

    public Schema ArrowSchema => new(Columns.Select(c => new Field(c.Name, c.Type, true)).ToList(), null);

    public byte[] ToArrowStream(IReadOnlyList<object?[]> rows)
    {
        var arrays = new IArrowArray[Columns.Length];
        for (var c = 0; c < Columns.Length; c++)
        {
            arrays[c] = BuildColumn(c, rows);
        }

        using var batch = new RecordBatch(ArrowSchema, arrays, rows.Count);
        using var ms = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, ArrowSchema, leaveOpen: true))
        {
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }

        return ms.ToArray();
    }

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
                var b = new TimestampArray.Builder(t);
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
