using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>The statement a read runs and the parameters it carries. Table mode pushes the engine's
/// column list and predicate into the SELECT and turns the watermark bounds into named parameters
/// typed from the cursor column; query mode runs the user's SQL verbatim with no pushdown of any
/// kind. Every WHERE term is self-parenthesized so a disjunctive predicate cannot bind into the
/// watermark's AND.
///
/// <para>A dataset with no serialized column (see <see cref="DbxReadSchema.HasSerializedColumns"/>)
/// renders exactly the SQL it always has -- <c>select *</c>, or the user's query verbatim. Only when
/// the read schema has at least one <c>ARRAY</c>/<c>MAP</c>/<c>STRUCT</c>/<c>INTERVAL</c> column does
/// the projection become explicit (table mode) or the query get wrapped as a derived table (query
/// mode), because only then is there something to serialize.</para></summary>
internal sealed record DbxReadPlan(string Sql, DbxParameter[]? Parameters)
{
    public const string LowerParameter = "pz_lower";
    public const string UpperParameter = "pz_upper";

    public static string ProbeSql(DbxReadConfig config)
    {
        var inner = config.Query is { } query ? Inner(query) : $"select * from {config.Table!.Value.Quoted}";
        return $"select * from ({inner}) as pz_probe limit 0";
    }

    /// <summary>A user's query as it can appear inside a derived table. A statement terminator is
    /// legal where the query runs on its own but a parse error once the query is parenthesized, and
    /// the error names the wrapper rather than anything the user wrote.</summary>
    private static string Inner(string query) => query.Trim().TrimEnd(';').TrimEnd();

    public static DbxReadPlan Build(DbxReadConfig config, DatasetSpec spec, ReadHints hints, DbxReadSchema schema, DbxRedactor redactor)
    {
        if (config.Query is { } query)
        {
            if (!schema.HasSerializedColumns)
            {
                return new DbxReadPlan(query, null);
            }

            var wrapped = string.Join(", ", schema.Columns.Select(c => DbxSql.Projection(c.Name, c.SerializeExpression)));
            return new DbxReadPlan($"select {wrapped} from ({Inner(query)}) as pz_query", null);
        }

        var terms = new List<string>();
        var parameters = new List<DbxParameter>();

        // Databricks SQL reads a double-quoted token as a string literal, not the quoted identifier
        // DuckDB's parser emits it as; pushing such a predicate would silently compare the wrong
        // thing. Omitting it is always safe: the engine still filters locally.
        if (!string.IsNullOrEmpty(hints.PredicateSql) && !hints.PredicateSql.Contains('"'))
        {
            terms.Add($"({hints.PredicateSql})");
        }

        if (spec.WatermarkCursor is { } cursor && (spec.WatermarkValue is not null || spec.WatermarkUpperBound is not null))
        {
            var field = schema.Schema.GetFieldByName(cursor)
                ?? throw Unsupported(redactor, $"watermark cursor column '{cursor}' is not in the read schema");

            // The declared Arrow type of a serialized column is utf8 like any ordinary string column,
            // so ParameterType alone would accept it; its values are computed by the projection, not
            // stored, and cannot be compared against as a statement parameter.
            var column = schema.Columns.FirstOrDefault(c => c.Name == cursor);
            if (column?.SerializeExpression is not null)
            {
                throw Unsupported(redactor, $"watermark cursor column '{cursor}' is serialized in the read statement and cannot be a statement parameter");
            }

            var type = DbxTypeMap.ParameterType(field.DataType)
                ?? throw Unsupported(redactor, $"watermark cursor column '{cursor}' has Arrow type '{field.DataType.Name}', which cannot be a statement parameter");

            if (spec.WatermarkValue is { } lower)
            {
                var op = spec.WatermarkLowerInclusive ? ">=" : ">";
                terms.Add($"({DbxSql.Q(cursor)} {op} :{LowerParameter})");
                parameters.Add(new DbxParameter(LowerParameter, lower, type));
            }

            if (spec.WatermarkUpperBound is { } upper)
            {
                terms.Add($"({DbxSql.Q(cursor)} <= :{UpperParameter})");
                parameters.Add(new DbxParameter(UpperParameter, upper, type));
            }
        }

        var projection = BuildProjection(hints, schema);
        var sql = DbxSql.Select(config.Table!.Value, projection, terms);
        return new DbxReadPlan(sql, parameters.Count == 0 ? null : [.. parameters]);
    }

    /// <summary>The engine's pruned column list wins when it is non-empty; otherwise every schema
    /// column is projected explicitly, but only when at least one needs serializing -- an empty list
    /// here is what makes <see cref="DbxSql.Select"/> fall back to <c>*</c>, matching the SQL this
    /// connector has always emitted for a dataset with nothing to serialize.</summary>
    private static List<string> BuildProjection(ReadHints hints, DbxReadSchema schema)
    {
        if (hints.Columns is { Count: > 0 })
        {
            var byName = schema.Columns.ToDictionary(c => c.Name, c => c.SerializeExpression, StringComparer.Ordinal);
            return hints.Columns.Select(c => DbxSql.Projection(c, byName.GetValueOrDefault(c))).ToList();
        }

        return schema.HasSerializedColumns
            ? schema.Columns.Select(c => DbxSql.Projection(c.Name, c.SerializeExpression)).ToList()
            : [];
    }

    private static PzConnectorException Unsupported(DbxRedactor redactor, string text) =>
        new(DbxCodes.Message(DbxCodes.Read_UnsupportedCursorType, redactor, text), isTransient: false);
}
