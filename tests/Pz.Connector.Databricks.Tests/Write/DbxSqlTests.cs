using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxSqlTests
{
    private static readonly TableRef Target = new("main", "sales", "orders");
    private const string Dir = "/Volumes/main/pz/staging/pz/t1";

    private static IReadOnlyList<DbxColumnPlan> Columns() => DbxSchemaMap.Plan(new Schema([
        new Field("id", Int64Type.Default, true),
        new Field("amount", new Decimal128Type(18, 2), true),
        new Field("name", StringType.Default, true),
    ], null), "out");

    [Fact]
    public void ParquetSource_is_a_backticked_directory_path()
    {
        Assert.Equal("parquet.`/Volumes/main/pz/staging/pz/t1/`", DbxSql.ParquetSource(Dir));
    }

    [Fact]
    public void CreateIfNotExists_types_every_column()
    {
        Assert.Equal("create table if not exists `main`.`sales`.`orders` (`id` BIGINT, `amount` DECIMAL(18,2), `name` STRING)",
            DbxSql.CreateIfNotExists(Target, Columns()));
    }

    [Fact]
    public void Append_inserts_the_cast_selection()
    {
        Assert.Equal(
            "insert into `main`.`sales`.`orders` (`id`, `amount`, `name`) select `id`, cast(`amount` as DECIMAL(18,2)) as `amount`, `name` from parquet.`/Volumes/main/pz/staging/pz/t1/`",
            DbxSql.Append(Target, Columns(), Dir));
    }

    [Fact]
    public void Replace_creates_or_replaces_from_the_selection()
    {
        Assert.Equal(
            "create or replace table `main`.`sales`.`orders` as select `id`, cast(`amount` as DECIMAL(18,2)) as `amount`, `name` from parquet.`/Volumes/main/pz/staging/pz/t1/`",
            DbxSql.Replace(Target, Columns(), Dir));
    }

    [Fact]
    public void Merge_dedups_last_sequence_wins_and_matches_null_safely()
    {
        var sql = DbxSql.Merge(Target, Columns(), ["id"], Dir);
        Assert.Equal("""
            merge into `main`.`sales`.`orders` t
            using (
              select `id`, `amount`, `name` from (
                select `id`, cast(`amount` as DECIMAL(18,2)) as `amount`, `name`, row_number() over (partition by `id` order by `_pz_seq` desc) as `_pz_rn`
                from parquet.`/Volumes/main/pz/staging/pz/t1/`
              ) where `_pz_rn` = 1
            ) s
            on t.`id` <=> s.`id`
            when matched then update set t.`amount` = s.`amount`, t.`name` = s.`name`
            when not matched then insert (`id`, `amount`, `name`) values (s.`id`, s.`amount`, s.`name`)
            """.ReplaceLineEndings("\n"), sql);
    }

    [Fact]
    public void Merge_with_every_column_a_key_has_no_update_clause()
    {
        var cols = DbxSchemaMap.Plan(new Schema([new Field("id", Int64Type.Default, true)], null), "out");
        var sql = DbxSql.Merge(Target, cols, ["id"], Dir);
        Assert.DoesNotContain("when matched", sql);
        Assert.Contains("when not matched then insert (`id`) values (s.`id`)", sql);
    }
}
