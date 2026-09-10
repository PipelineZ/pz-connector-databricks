namespace Pz.Connector.Databricks;

/// <summary>Every SQL statement this connector generates. Identifiers are backtick-quoted through
/// <see cref="Q"/> so generated text is never reparsed against a caller's quoting assumptions; values
/// never appear as literals -- watermark bounds travel as statement parameters.</summary>
internal static partial class DbxSql
{
    public static string Q(string identifier) => TableRef.QuoteIdentifier(identifier);

    /// <summary>A column's rendered projection entry: its serialize expression aliased back to its
    /// own name, or a bare quoted reference when it needs no serialization.</summary>
    public static string Projection(string column, string? serializeExpression) =>
        serializeExpression is null ? Q(column) : $"{serializeExpression} as {Q(column)}";

    /// <summary><paramref name="projection"/> entries are already-rendered column expressions (see
    /// <see cref="Projection"/>); an empty list selects every column with <c>*</c>.</summary>
    public static string Select(TableRef table, IReadOnlyList<string> projection, IReadOnlyList<string> whereTerms)
    {
        var cols = projection.Count > 0 ? string.Join(", ", projection) : "*";
        var sql = $"select {cols} from {table.Quoted}";
        return whereTerms.Count == 0 ? sql : $"{sql} where {string.Join(" and ", whereTerms)}";
    }
}
