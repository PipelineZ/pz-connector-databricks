using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Abstractions.Formats;

namespace Pz.Connector.Databricks;

/// <summary>Turns the JSON text the read statement projects for an <c>ARRAY</c>/<c>MAP</c>/<c>STRUCT</c>
/// column (<c>to_json</c>) into the Arrow nested column the dataset schema declares. Columns are matched
/// by name against the batch the statement returned, so a pruned projection converts only what it
/// kept. Every other column moves into the result batch untouched.</summary>
internal static class DbxNestedColumns
{
    public static RecordBatch Convert(RecordBatch batch, IReadOnlyDictionary<string, IArrowType> nested, string context)
    {
        if (nested.Count == 0 || batch.Length == 0)
        {
            return batch;
        }

        var fields = new List<Field>(batch.ColumnCount);
        var arrays = new List<IArrowArray>(batch.ColumnCount);
        var replaced = new List<IArrowArray>();
        for (var i = 0; i < batch.ColumnCount; i++)
        {
            var field = batch.Schema.GetFieldByIndex(i);
            var source = batch.Column(i);
            if (nested.TryGetValue(field.Name, out var type) && source is StringArray text)
            {
                fields.Add(new Field(field.Name, type, true));
                arrays.Add(Parse(text, field.Name, type, context));
                replaced.Add(source);
            }
            else
            {
                fields.Add(field);
                arrays.Add(source);
            }
        }

        if (replaced.Count == 0)
        {
            return batch;
        }

        var result = new RecordBatch(new Schema(fields, null), arrays, batch.Length);
        foreach (var array in replaced)
        {
            (array as IDisposable)?.Dispose();
        }

        return result;
    }

    private static IArrowArray Parse(StringArray text, string column, IArrowType type, string context)
    {
        var builder = new ArrowBatchBuilder(new Schema([new Field(column, type, true)], null), maxRowsPerBatch: int.MaxValue);
        for (var row = 0; row < text.Length; row++)
        {
            object? value;
            try
            {
                value = text.IsNull(row) ? null : ColumnContractType.ValueFromJson(text.GetString(row), type);
            }
            catch (Exception ex) when (ex is FormatException or InvalidOperationException or OverflowException)
            {
                throw new PzConnectorException(
                    DbxCodes.Message(DbxCodes.Read_NestedValue, DbxRedactor.None,
                        $"{context}: column '{column}' row {row + 1} is not a valid {type.Name} value ({ex.Message}) -- cast the column to STRING in a query: read to keep its JSON text"),
                    isTransient: false, innerException: ex);
            }

            builder.AppendRow([value]);
        }

        // Convert never reaches here with an empty batch, so Flush always has rows to build. The
        // batch is not disposed: disposing it would dispose the column handed to the caller.
        return builder.Flush()!.Column(0);
    }
}
