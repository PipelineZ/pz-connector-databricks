using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxWriteConfigTests
{
    private static readonly DbxConnectionConfig Cfg =
        new(new Uri("https://ws.example/"), "wh1", DbxAuthKind.Token, "t", null, null, "main", "sales", null, DbxRedactor.None);

    private static OutputSpec Spec(string output, params (string, object?)[] options) =>
        new("dbx", output, "append", "fail_on_change", options.ToDictionary(o => o.Item1, o => o.Item2, StringComparer.Ordinal));

    [Fact]
    public void Output_name_is_the_target_by_default()
    {
        var errors = new List<string>();
        Assert.Equal(new TableRef("main", "sales", "orders_out"), DbxWriteConfig.Parse(Spec("orders_out"), Cfg, errors)!.Target);
        Assert.Empty(errors);
    }

    [Fact]
    public void Entity_option_overrides()
    {
        var errors = new List<string>();
        Assert.Equal(new TableRef("prod", "marts", "orders"), DbxWriteConfig.Parse(Spec("x", ("entity", "prod.marts.orders")), Cfg, errors)!.Target);
    }

    [Fact]
    public void Unknown_options_and_bad_names_are_reported()
    {
        var errors = new List<string>();
        Assert.Null(DbxWriteConfig.Parse(Spec("a.b.c.d", ("mode", "x")), Cfg, errors));
        Assert.Contains(errors, e => e.StartsWith("databricks: PZDB0307: ", StringComparison.Ordinal) && e.Contains("unknown output option 'mode'"));
        Assert.Contains(errors, e => e.Contains("'a.b.c.d'"));
    }
}
