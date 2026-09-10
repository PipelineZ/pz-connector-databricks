using System.Text;

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

    /// <summary>The uploaded Parquet directory as a path table. Only the directory is named, so
    /// every part file the session uploaded is read and nothing else under the volume is.</summary>
    public static string ParquetSource(string volumeDir) => $"parquet.{Q(volumeDir + "/")}";

    public static string CreateIfNotExists(TableRef target, IReadOnlyList<DbxColumnPlan> columns) =>
        $"create table if not exists {target.Quoted} ({string.Join(", ", columns.Select(c => $"{Q(c.Name)} {c.DatabricksType}"))})";

    public static string Append(TableRef target, IReadOnlyList<DbxColumnPlan> columns, string volumeDir) =>
        $"insert into {target.Quoted} ({ColumnList(columns)}) select {Selection(columns)} from {ParquetSource(volumeDir)}";

    /// <summary>Atomic in Delta; replaces the target's schema with the write's own.</summary>
    public static string Replace(TableRef target, IReadOnlyList<DbxColumnPlan> columns, string volumeDir) =>
        $"create or replace table {target.Quoted} as select {Selection(columns)} from {ParquetSource(volumeDir)}";

    /// <summary>Deduplicates staged rows on the merge keys (last <c>_pz_seq</c> within the session
    /// wins), then upserts. <c>&lt;=&gt;</c> makes null keys match null keys. <c>when matched</c> is
    /// omitted when every column is a key -- nothing is left to update.</summary>
    public static string Merge(TableRef target, IReadOnlyList<DbxColumnPlan> columns, IReadOnlyList<string> keys, string volumeDir)
    {
        var colList = ColumnList(columns);
        var keyList = string.Join(", ", keys.Select(Q));
        var nonKeys = columns.Where(c => !keys.Contains(c.Name, StringComparer.Ordinal)).ToList();

        var sql = new StringBuilder();
        sql.Append("merge into ").Append(target.Quoted).Append(" t\n");
        sql.Append("using (\n");
        sql.Append("  select ").Append(colList).Append(" from (\n");
        sql.Append("    select ").Append(Selection(columns))
            .Append(", row_number() over (partition by ").Append(keyList).Append(" order by `_pz_seq` desc) as `_pz_rn`\n");
        sql.Append("    from ").Append(ParquetSource(volumeDir)).Append('\n');
        sql.Append("  ) where `_pz_rn` = 1\n");
        sql.Append(") s\n");
        sql.Append("on ").Append(string.Join(" and ", keys.Select(k => $"t.{Q(k)} <=> s.{Q(k)}"))).Append('\n');
        if (nonKeys.Count > 0)
        {
            sql.Append("when matched then update set ").Append(string.Join(", ", nonKeys.Select(c => $"t.{Q(c.Name)} = s.{Q(c.Name)}"))).Append('\n');
        }

        sql.Append("when not matched then insert (").Append(colList).Append(") values (")
            .Append(string.Join(", ", columns.Select(c => $"s.{Q(c.Name)}"))).Append(')');
        return sql.ToString();
    }

    private static string ColumnList(IReadOnlyList<DbxColumnPlan> columns) => string.Join(", ", columns.Select(c => Q(c.Name)));

    private static string Selection(IReadOnlyList<DbxColumnPlan> columns) => string.Join(", ", columns.Select(DbxSchemaMap.SelectExpression));
}
