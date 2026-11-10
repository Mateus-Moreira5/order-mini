namespace Orders.Worker;

public enum RetryDecision
{
    Retry,
    Park
}

public static class RetryPolicy
{
    public static RetryDecision Decide(long attempt, int maxAttempts) =>
        attempt >= maxAttempts ? RetryDecision.Park : RetryDecision.Retry;
}