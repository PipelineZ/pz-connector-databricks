using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks;

/// <summary>The statement a read runs and the parameters it carries. Table mode pushes the engine's
/// column list and predicate into the SELECT and turns the watermark bounds into named parameters
/// typed from the cursor column; query mode runs the user's SQL verbatim with no pushdown of any
/// kind. Every WHERE term is self-parenthesized so a disjunctive predicate cannot bind into the
/// watermark's AND.</summary>
internal sealed record DbxReadPlan(string Sql, DbxParameter[]? Parameters)
{
    public const string LowerParameter = "pz_lower";
    public const string UpperParameter = "pz_upper";

    public static string ProbeSql(DbxReadConfig config)
    {
        var inner = config.Query ?? $"select * from {config.Table!.Value.Quoted}";
        return $"select * from ({inner}) as pz_probe limit 0";
    }

    public static DbxReadPlan Build(DbxReadConfig config, DatasetSpec spec, ReadHints hints, Schema schema, DbxRedactor redactor)
    {
        if (config.Query is { } query)
        {
            return new DbxReadPlan(query, null);
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
            var field = schema.GetFieldByName(cursor)
                ?? throw Unsupported(redactor, $"watermark cursor column '{cursor}' is not in the read schema");
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

        var sql = DbxSql.Select(config.Table!.Value, hints.Columns, terms);
        return new DbxReadPlan(sql, parameters.Count == 0 ? null : [.. parameters]);
    }

    private static PzConnectorException Unsupported(DbxRedactor redactor, string text) =>
        new(DbxCodes.Message(DbxCodes.Read_UnsupportedCursorType, redactor, text), isTransient: false);
}
