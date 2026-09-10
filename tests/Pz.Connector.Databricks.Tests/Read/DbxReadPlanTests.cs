using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxReadPlanTests
{
    private static readonly TableRef Orders = new("main", "sales", "orders");

    private static readonly DbxReadSchema Schema = new(
        new Schema([
            new Field("id", Int64Type.Default, true),
            new Field("updated_at", new TimestampType(TimeUnit.Microsecond, "Etc/UTC"), true),
            new Field("name", StringType.Default, true),
        ], null),
        [
            new DbxReadColumn("id", null),
            new DbxReadColumn("updated_at", null),
            new DbxReadColumn("name", null),
        ]);

    private static readonly DbxReadSchema ComplexSchema = new(
        new Schema([
            new Field("id", Int64Type.Default, true),
            new Field("tags", StringType.Default, true),
            new Field("span", StringType.Default, true),
        ], null),
        [
            new DbxReadColumn("id", null),
            new DbxReadColumn("tags", "to_json(`tags`)"),
            new DbxReadColumn("span", "cast(`span` as string)"),
        ]);

    private static DatasetSpec Spec() => new("dbx", "orders", new Dictionary<string, object?>());

    [Fact]
    public void Table_mode_selects_star_without_hints()
    {
        var plan = DbxReadPlan.Build(new DbxReadConfig(Orders, null), Spec(), ReadHints.None, Schema, DbxRedactor.None);
        Assert.Equal("select * from `main`.`sales`.`orders`", plan.Sql);
        Assert.Null(plan.Parameters);
    }

    [Fact]
    public void Column_pruning_and_predicate_are_pushed_down()
    {
        var hints = new ReadHints(Columns: ["id", "name"], PredicateSql: "name = 'x' or id < 5");
        var plan = DbxReadPlan.Build(new DbxReadConfig(Orders, null), Spec(), hints, Schema, DbxRedactor.None);
        Assert.Equal("select `id`, `name` from `main`.`sales`.`orders` where (name = 'x' or id < 5)", plan.Sql);
    }

    [Fact]
    public void A_double_quoted_predicate_is_not_pushed_down()
    {
        var hints = new ReadHints(PredicateSql: "\"name\" = 'x'");
        var plan = DbxReadPlan.Build(new DbxReadConfig(Orders, null), Spec(), hints, Schema, DbxRedactor.None);
        Assert.Equal("select * from `main`.`sales`.`orders`", plan.Sql);
    }

    [Fact]
    public void Watermark_bounds_become_typed_parameters()
    {
        var spec = Spec() with { WatermarkCursor = "updated_at", WatermarkValue = "2026-09-10T00:00:00", WatermarkUpperBound = "2026-09-11T00:00:00" };
        var plan = DbxReadPlan.Build(new DbxReadConfig(Orders, null), spec, new ReadHints(PredicateSql: "id > 0"), Schema, DbxRedactor.None);

        Assert.Equal("select * from `main`.`sales`.`orders` where (id > 0) and (`updated_at` > :pz_lower) and (`updated_at` <= :pz_upper)", plan.Sql);
        Assert.Equal(2, plan.Parameters!.Length);
        Assert.Equal(new DbxParameter("pz_lower", "2026-09-10T00:00:00", "TIMESTAMP"), plan.Parameters[0]);
        Assert.Equal(new DbxParameter("pz_upper", "2026-09-11T00:00:00", "TIMESTAMP"), plan.Parameters[1]);
    }

    [Fact]
    public void Inclusive_lower_bound_uses_gte()
    {
        var spec = Spec() with { WatermarkCursor = "id", WatermarkValue = "3", WatermarkLowerInclusive = true };
        var plan = DbxReadPlan.Build(new DbxReadConfig(Orders, null), spec, ReadHints.None, Schema, DbxRedactor.None);
        Assert.Equal("select * from `main`.`sales`.`orders` where (`id` >= :pz_lower)", plan.Sql);
        Assert.Equal("BIGINT", plan.Parameters![0].Type);
    }

    [Fact]
    public void Unsupported_or_missing_cursor_is_PZDB0202()
    {
        var missing = Spec() with { WatermarkCursor = "nope", WatermarkValue = "1" };
        var ex = Assert.Throws<PzConnectorException>(() => DbxReadPlan.Build(new DbxReadConfig(Orders, null), missing, ReadHints.None, Schema, DbxRedactor.None));
        Assert.StartsWith("databricks: PZDB0202: ", ex.Message);

        var boolSchema = new DbxReadSchema(new Schema([new Field("flag", BooleanType.Default, true)], null), [new DbxReadColumn("flag", null)]);
        var unsupported = Spec() with { WatermarkCursor = "flag", WatermarkValue = "true" };
        ex = Assert.Throws<PzConnectorException>(() => DbxReadPlan.Build(new DbxReadConfig(Orders, null), unsupported, ReadHints.None, boolSchema, DbxRedactor.None));
        Assert.StartsWith("databricks: PZDB0202: ", ex.Message);
    }

    [Fact]
    public void Query_mode_runs_verbatim_with_no_pushdown()
    {
        var spec = Spec() with { WatermarkCursor = "id", WatermarkValue = "3" };
        var plan = DbxReadPlan.Build(new DbxReadConfig(null, "select id from t where x = 1"), spec, new ReadHints(Columns: ["id"], PredicateSql: "id > 1"), Schema, DbxRedactor.None);
        Assert.Equal("select id from t where x = 1", plan.Sql);
        Assert.Null(plan.Parameters);
    }

    [Fact]
    public void ProbeSql_wraps_table_and_query()
    {
        Assert.Equal("select * from (select * from `main`.`sales`.`orders`) as pz_probe limit 0", DbxReadPlan.ProbeSql(new DbxReadConfig(Orders, null)));
        Assert.Equal("select * from (select 1 as x) as pz_probe limit 0", DbxReadPlan.ProbeSql(new DbxReadConfig(null, "select 1 as x")));
    }

    [Theory]
    [InlineData("select 1;")]
    [InlineData("  select 1 ;  ")]
    [InlineData("select 1;\n")]
    public void ProbeSql_trims_whitespace_and_a_trailing_terminator(string query)
    {
        Assert.Equal("select * from (select 1) as pz_probe limit 0", DbxReadPlan.ProbeSql(new DbxReadConfig(null, query)));
    }

    [Fact]
    public void A_wrapped_query_is_trimmed_of_its_terminator_too()
    {
        var plan = DbxReadPlan.Build(new DbxReadConfig(null, "select id, tags, span from t;\n"), Spec(), ReadHints.None, ComplexSchema, DbxRedactor.None);
        Assert.Equal(
            "select `id`, to_json(`tags`) as `tags`, cast(`span` as string) as `span` from (select id, tags, span from t) as pz_query",
            plan.Sql);
    }

    [Fact]
    public void Table_mode_with_a_serialized_column_and_no_hints_projects_every_column_explicitly()
    {
        var plan = DbxReadPlan.Build(new DbxReadConfig(Orders, null), Spec(), ReadHints.None, ComplexSchema, DbxRedactor.None);
        Assert.Equal("select `id`, to_json(`tags`) as `tags`, cast(`span` as string) as `span` from `main`.`sales`.`orders`", plan.Sql);
    }

    [Fact]
    public void Table_mode_with_hints_columns_projects_only_those_serialized_where_needed()
    {
        var hints = new ReadHints(Columns: ["tags", "id"]);
        var plan = DbxReadPlan.Build(new DbxReadConfig(Orders, null), Spec(), hints, ComplexSchema, DbxRedactor.None);
        Assert.Equal("select to_json(`tags`) as `tags`, `id` from `main`.`sales`.`orders`", plan.Sql);
    }

    [Fact]
    public void Table_mode_with_no_serialized_column_is_byte_identical_to_before()
    {
        var plan = DbxReadPlan.Build(new DbxReadConfig(Orders, null), Spec(), ReadHints.None, Schema, DbxRedactor.None);
        Assert.Equal("select * from `main`.`sales`.`orders`", plan.Sql);
    }

    [Fact]
    public void Query_mode_with_a_serialized_column_wraps_the_query()
    {
        var plan = DbxReadPlan.Build(new DbxReadConfig(null, "select id, tags, span from t"), Spec(), ReadHints.None, ComplexSchema, DbxRedactor.None);
        Assert.Equal(
            "select `id`, to_json(`tags`) as `tags`, cast(`span` as string) as `span` from (select id, tags, span from t) as pz_query",
            plan.Sql);
        Assert.Null(plan.Parameters);
    }

    [Fact]
    public void Query_mode_without_a_serialized_column_stays_verbatim()
    {
        var plan = DbxReadPlan.Build(new DbxReadConfig(null, "select id, name from t"), Spec(), ReadHints.None, Schema, DbxRedactor.None);
        Assert.Equal("select id, name from t", plan.Sql);
    }

    [Fact]
    public void Watermark_on_a_serialized_column_is_PZDB0202_naming_the_column()
    {
        var spec = Spec() with { WatermarkCursor = "tags", WatermarkValue = "x" };
        var ex = Assert.Throws<PzConnectorException>(() =>
            DbxReadPlan.Build(new DbxReadConfig(Orders, null), spec, ReadHints.None, ComplexSchema, DbxRedactor.None));
        Assert.StartsWith("databricks: PZDB0202: ", ex.Message);
        Assert.Contains("'tags'", ex.Message);
    }
}
