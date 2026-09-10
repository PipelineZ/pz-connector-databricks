namespace Pz.Connector.Databricks.Tests;

public sealed class TableRefTests
{
    [Theory]
    [InlineData("orders", "main", "sales", "main", "sales", "orders")]
    [InlineData("sales.orders", "main", null, "main", "sales", "orders")]
    [InlineData("prod.sales.orders", null, null, "prod", "sales", "orders")]
    public void TryParse_fills_missing_parts_from_the_connection_defaults(
        string entity, string? catalog, string? schema, string c, string s, string t)
    {
        Assert.True(TableRef.TryParse(entity, catalog, schema, out var r, out var error, out var code));
        Assert.Null(error);
        Assert.Null(code);
        Assert.Equal(new TableRef(c, s, t), r);
    }

    [Theory]
    [InlineData("orders", "main", null)]
    [InlineData("orders", null, "sales")]
    [InlineData("sales.orders", null, null)]
    public void TryParse_refuses_a_name_the_defaults_cannot_complete(string entity, string? catalog, string? schema)
    {
        Assert.False(TableRef.TryParse(entity, catalog, schema, out _, out var error, out var code));
        Assert.Equal(DbxCodes.Config_EntityUnresolvable, code);
        Assert.Contains(entity, error);
    }

    [Theory]
    [InlineData("a.b.c.d")]
    [InlineData("")]
    [InlineData("a..b")]
    public void TryParse_refuses_the_wrong_number_of_parts(string entity)
    {
        Assert.False(TableRef.TryParse(entity, "c", "s", out _, out _, out var code));
        Assert.Equal(DbxCodes.Config_EntityUnresolvable, code);
    }

    [Fact]
    public void TryParse_refuses_backticks()
    {
        Assert.False(TableRef.TryParse("`sales`.orders", "c", "s", out _, out var error, out var code));
        Assert.Equal(DbxCodes.Config_BackticksRejected, code);
        Assert.Contains("backtick", error);
    }

    [Fact]
    public void Quoted_backtick_quotes_every_part_and_doubles_inner_backticks()
    {
        Assert.Equal("`c`.`s`.`t`", new TableRef("c", "s", "t").Quoted);
        Assert.Equal("`we``ird`", TableRef.QuoteIdentifier("we`ird"));
    }

    [Fact]
    public void FullName_and_VolumePath_are_unquoted()
    {
        var r = new TableRef("main", "pz", "staging");
        Assert.Equal("main.pz.staging", r.FullName);
        Assert.Equal("/Volumes/main/pz/staging", r.VolumePath);
    }

    [Theory]
    [InlineData("main.pz.staging", true)]
    [InlineData("pz.staging", false)]
    [InlineData("main.pz.stag`ing", false)]
    public void TryParseVolume_requires_exactly_three_parts(string text, bool ok)
    {
        Assert.Equal(ok, TableRef.TryParseVolume(text, out _, out var error));
        Assert.Equal(ok, error is null);
    }
}
