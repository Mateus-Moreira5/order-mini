namespace Orders.Contracts;

public sealed record OrderCreated(
    Guid OrderId,
    Guid MessageId,
    string Customer,
    decimal Amount,
    string FailureMode,
    DateTime CreatedAtUtc);