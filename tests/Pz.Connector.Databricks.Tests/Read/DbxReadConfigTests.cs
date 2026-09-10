using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxReadConfigTests
{
    private static readonly DbxConnectionConfig Cfg =
        new(new Uri("https://ws.example/"), "wh1", DbxAuthKind.Token, "t", null, null, "main", "sales", null, DbxRedactor.None);

    private static DatasetSpec Spec(string dataset, params (string, object?)[] options) =>
        new("dbx", dataset, options.ToDictionary(o => o.Item1, o => o.Item2, StringComparer.Ordinal));

    [Fact]
    public void Dataset_name_is_the_entity_by_default()
    {
        var errors = new List<string>();
        var cfg = DbxReadConfig.Parse(Spec("orders"), Cfg, errors);
        Assert.Empty(errors);
        Assert.Equal(new TableRef("main", "sales", "orders"), cfg!.Table);
        Assert.Null(cfg.Query);
    }

    [Fact]
    public void Entity_option_overrides_the_dataset_name()
    {
        var errors = new List<string>();
        var cfg = DbxReadConfig.Parse(Spec("orders", ("entity", "prod.raw.orders")), Cfg, errors);
        Assert.Equal(new TableRef("prod", "raw", "orders"), cfg!.Table);
    }

    [Fact]
    public void Query_mode_has_no_table()
    {
        var errors = new List<string>();
        var cfg = DbxReadConfig.Parse(Spec("recent", ("query", "select 1")), Cfg, errors);
        Assert.Null(cfg!.Table);
        Assert.Equal("select 1", cfg.Query);
    }

    [Fact]
    public void Entity_and_query_together_are_refused()
    {
        var errors = new List<string>();
        Assert.Null(DbxReadConfig.Parse(Spec("x", ("entity", "a"), ("query", "select 1")), Cfg, errors));
        Assert.Contains(errors, e => e.Contains(DbxCodes.Read_EntityAndQuery));
    }

    [Fact]
    public void Unknown_options_and_bad_entities_are_reported()
    {
        var errors = new List<string>();
        Assert.Null(DbxReadConfig.Parse(Spec("a.b.c.d", ("streams", 4)), Cfg, errors));
        Assert.Contains(errors, e => e.StartsWith("databricks: PZDB0206: ", StringComparison.Ordinal) && e.Contains("unknown dataset option 'streams'"));
        Assert.Contains(errors, e => e.Contains("'a.b.c.d'"));
    }
}
