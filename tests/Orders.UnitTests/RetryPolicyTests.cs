using Orders.Worker;

namespace Orders.UnitTests;

public class RetryPolicyTests
{
    [Theory]
    [InlineData(1, 3)]
    [InlineData(2, 3)]
    public void Decide_AttemptBelowLimit_ReturnsRetry(long attempt, int maxAttempts)
    {
        var decision = RetryPolicy.Decide(attempt, maxAttempts);

        Assert.Equal(RetryDecision.Retry, decision);
    }

    [Theory]
    [InlineData(3, 3)]
    [InlineData(4, 3)]
    public void Decide_AttemptAtOrAboveLimit_ReturnsPark(long attempt, int maxAttempts)
    {
        var decision = RetryPolicy.Decide(attempt, maxAttempts);

        Assert.Equal(RetryDecision.Park, decision);
    }
}