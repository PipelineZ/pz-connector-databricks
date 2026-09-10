namespace Pz.Connector.Databricks;

/// <summary>Every SQL statement this connector generates. Identifiers are backtick-quoted through
/// <see cref="Q"/> so generated text is never reparsed against a caller's quoting assumptions; values
/// never appear as literals -- watermark bounds travel as statement parameters.</summary>
internal static partial class DbxSql
{
    public static string Q(string identifier) => TableRef.QuoteIdentifier(identifier);

    public static string Select(TableRef table, IReadOnlyList<string>? columns, IReadOnlyList<string> whereTerms)
    {
        var projection = columns is { Count: > 0 } ? string.Join(", ", columns.Select(Q)) : "*";
        var sql = $"select {projection} from {table.Quoted}";
        return whereTerms.Count == 0 ? sql : $"{sql} where {string.Join(" and ", whereTerms)}";
    }
}
