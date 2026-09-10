using Pz.Connectors.Abstractions;

namespace Pz.Connector.Databricks.Tests;

public sealed class DbxErrorsTests
{
    private static readonly DbxRedactor R = new(["s3cret"]);

    [Theory]
    [InlineData(401, "PZDB0401", false)]
    [InlineData(403, "PZDB0401", false)]
    [InlineData(429, "PZDB0402", true)]
    [InlineData(502, "PZDB0402", true)]
    [InlineData(503, "PZDB0402", true)]
    [InlineData(504, "PZDB0402", true)]
    [InlineData(404, "PZDB0402", false)]
    [InlineData(400, "PZDB0402", false)]
    public void FromHttp_classifies_by_status(int status, string code, bool transient)
    {
        var ex = DbxErrors.FromHttp(status, "SOME_CODE", "message s3cret", null, R, "submitting statement for orders");

        Assert.StartsWith($"databricks: {code}: submitting statement for orders: HTTP {status} (SOME_CODE): message ***", ex.Message);
        Assert.Equal(transient, ex.IsTransient);
    }

    [Fact]
    public void FromHttp_carries_retry_after()
    {
        var ex = DbxErrors.FromHttp(429, null, null, TimeSpan.FromSeconds(7), R, "ctx");

        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(7), ex.RetryAfter);
        Assert.Equal("databricks: PZDB0402: ctx: HTTP 429", ex.Message);
    }

    [Theory]
    [InlineData("TEMPORARILY_UNAVAILABLE", true)]
    [InlineData("RESOURCE_EXHAUSTED", true)]
    [InlineData("DEADLINE_EXCEEDED", true)]
    [InlineData("INTERNAL_ERROR", true)]
    [InlineData("BAD_REQUEST", false)]
    [InlineData(null, false)]
    public void IsTransientStatementError_recognizes_the_service_side_conditions(string? code, bool transient)
    {
        Assert.Equal(transient, DbxErrors.IsTransientStatementError(code));
    }

    [Fact]
    public void FromStatement_uses_the_caller_code_for_a_permanent_failure()
    {
        var ex = DbxErrors.FromStatement("BAD_REQUEST", "[TABLE_OR_VIEW_NOT_FOUND] x s3cret", R, "reading sales.orders", DbxCodes.Read_StatementFailed);

        Assert.Equal("databricks: PZDB0203: reading sales.orders: statement failed (BAD_REQUEST): [TABLE_OR_VIEW_NOT_FOUND] x ***", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void FromStatement_maps_a_transient_failure_to_PZDB0404()
    {
        var ex = DbxErrors.FromStatement("TEMPORARILY_UNAVAILABLE", "warehouse starting", R, "reading x", DbxCodes.Read_StatementFailed);

        Assert.StartsWith("databricks: PZDB0404: reading x: statement failed (TEMPORARILY_UNAVAILABLE)", ex.Message);
        Assert.True(ex.IsTransient);
    }

    [Fact]
    public void Wrap_is_transient_and_refuses_cancellation()
    {
        var ex = DbxErrors.Wrap(new HttpRequestException("boom s3cret"), R, "ctx");
        Assert.Equal("databricks: PZDB0402: ctx: boom ***", ex.Message);
        Assert.True(ex.IsTransient);

        Assert.Throws<InvalidOperationException>(() => DbxErrors.Wrap(new OperationCanceledException(), R, "ctx"));
    }
}
