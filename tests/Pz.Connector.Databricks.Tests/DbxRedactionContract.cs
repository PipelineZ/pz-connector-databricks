using Pz.Connectors.TestKit;

namespace Pz.Connector.Databricks.Tests;

/// <summary>The TestKit's credential shapes through this connector's redactor, seeded with the same
/// synthetic secret the suite embeds, exactly as a real config seeds it with the token.</summary>
public sealed class DbxRedactionContract : ErrorRedactionContractTests
{
    protected override string RedactErrorText(string thirdPartyMessage) =>
        new DbxRedactor(["pz-testkit-secret-value"]).Redact(thirdPartyMessage);
}
