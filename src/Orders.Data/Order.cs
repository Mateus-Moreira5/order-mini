namespace Orders.Data;

public sealed class Order
{
    public Guid Id { get; set; }
    public string Customer { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public OrderStatus Status { get; set; }
    public string FailureMode { get; set; } = "None";
    public int Attempts { get; set; }
    public string? LastError { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? UpdatedAtUtc { get; set; }
}