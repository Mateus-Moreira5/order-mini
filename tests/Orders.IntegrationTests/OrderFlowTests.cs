using System.Net;
using System.Net.Http.Json;
using Orders.Data;
using Xunit;

namespace Orders.IntegrationTests;

[Collection(InfraCollection.Name)]
public class OrderFlowTests(InfraFixture infra)
{
    private sealed record AcceptedResponse(Guid OrderId);

    private async Task<Guid> PostOrderAsync(
        string customer,
        string failureMode,
        string? correlationId = null,
        string? idempotencyKey = null)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/orders")
        {
            Content = JsonContent.Create(new { customer, amount = 10.50m, failureMode })
        };

        if (correlationId is not null)
        {
            request.Headers.Add("X-Correlation-Id", correlationId);
        }

        if (idempotencyKey is not null)
        {
            request.Headers.Add("Idempotency-Key", idempotencyKey);
        }

        using var response = await infra.Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        var body = await response.Content.ReadFromJsonAsync<AcceptedResponse>();
        return body!.OrderId;
    }

    [Fact]
    public async Task PermanentFailure_EndsFailedAndLandsInParkingWithCorrelationId()
    {
        var correlationId = $"it-{Guid.NewGuid():N}";

        var orderId = await PostOrderAsync("perm", "Permanent", correlationId: correlationId);

        var order = await infra.WaitForOrderAsync(
            orderId,
            o => o.Status == OrderStatus.Failed,
            TimeSpan.FromSeconds(90));

        Assert.Equal(3, order.Attempts);
        Assert.False(string.IsNullOrEmpty(order.LastError));

        var parked = await infra.FindInParkingAsync(correlationId, TimeSpan.FromSeconds(10));

        Assert.NotNull(parked);
        Assert.Equal(correlationId, parked.CorrelationId);
        Assert.Equal(3L, parked.Headers["x-attempts"]);
        Assert.True(parked.Headers.ContainsKey("x-last-error"));
    }

    [Fact]
    public async Task SameIdempotencyKey_CreatesOneOrderAndProcessesItOnce()
    {
        var customer = $"idem-{Guid.NewGuid():N}";
        var key = Guid.NewGuid().ToString();

        var first = await PostOrderAsync(customer, "None", idempotencyKey: key);
        var second = await PostOrderAsync(customer, "None", idempotencyKey: key);

        Assert.Equal(first, second);

        var order = await infra.WaitForOrderAsync(
            first,
            o => o.Status == OrderStatus.Completed,
            TimeSpan.FromSeconds(30));

        Assert.Equal(1, order.Attempts);

        await using var db = infra.NewDbContext();
        var count = db.Orders.Count(o => o.Customer == customer);

        Assert.Equal(1, count);
    }
}