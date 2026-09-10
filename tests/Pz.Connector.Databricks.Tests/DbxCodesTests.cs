using System.Text.RegularExpressions;

namespace Pz.Connector.Databricks.Tests;

public sealed partial class DbxCodesTests
{
    // 7 (01xx) + 5 (02xx) + 7 (03xx) + 4 (04xx) consts declared in DbxCodes -- counted by hand and kept
    // in sync with DbxCodes.All whenever a code is added or removed.
    private const int ExpectedCodeCount = 23;

    [Fact]
    public void All_codes_are_unique_and_match_the_PZDB_shape()
    {
        Assert.Equal(ExpectedCodeCount, DbxCodes.All.Count);
        Assert.Equal(DbxCodes.All.Count, DbxCodes.All.Distinct(StringComparer.Ordinal).Count());
        Assert.All(DbxCodes.All, code => Assert.Matches(CodeShape(), code));
    }

    [Fact]
    public void Message_redacts_then_prefixes_with_the_code()
    {
        var redactor = new DbxRedactor(["s3cret"]);

        Assert.Equal("databricks: PZDB0101: host *** required",
            DbxCodes.Message(DbxCodes.Config_HostInvalid, redactor, "host s3cret required"));
    }

    [Fact]
    public void Message_still_masks_a_secret_that_contains_the_connector_prefix()
    {
        var redactor = new DbxRedactor(["databricks:secret"]);

        Assert.Equal("databricks: PZDB0101: token *** leaked",
            DbxCodes.Message(DbxCodes.Config_HostInvalid, redactor, "token databricks:secret leaked"));
    }

    [GeneratedRegex(@"^PZDB0[1-4]\d\d$")]
    private static partial Regex CodeShape();
}
