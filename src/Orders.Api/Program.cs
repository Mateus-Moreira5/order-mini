using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Orders.Contracts;
using Orders.Data;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<OrdersDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Orders")));

var rabbitOptions = builder.Configuration
    .GetSection(RabbitMqOptions.SectionName)
    .Get<RabbitMqOptions>() ?? new RabbitMqOptions();

var connection = await rabbitOptions.CreateFactory().CreateConnectionAsync();

await using (var setupChannel = await connection.CreateChannelAsync())
{
    await RabbitTopology.DeclareAsync(setupChannel);
}

builder.Services.AddSingleton(connection);

var app = builder.Build();

app.MapPost("/orders", async (CreateOrderRequest request, IConnection conn, OrdersDbContext db) =>
{
    var order = new Order
    {
        Id = Guid.NewGuid(),
        Customer = request.Customer,
        Amount = request.Amount,
        Status = OrderStatus.Pending,
        FailureMode = request.FailureMode ?? "None",
        CreatedAtUtc = DateTime.UtcNow
    };

    db.Orders.Add(order);
    await db.SaveChangesAsync();

    var message = new OrderCreated(
        OrderId: order.Id,
        MessageId: Guid.NewGuid(),
        Customer: order.Customer,
        Amount: order.Amount,
        FailureMode: order.FailureMode,
        CreatedAtUtc: order.CreatedAtUtc);

    var body = JsonSerializer.SerializeToUtf8Bytes(message);

    var props = new BasicProperties
    {
        Persistent = true,
        MessageId = message.MessageId.ToString(),
        ContentType = "application/json"
    };

    await using var channel = await conn.CreateChannelAsync();

    await channel.BasicPublishAsync(
        exchange: RabbitTopology.Exchange,
        routingKey: RabbitTopology.OrderCreatedKey,
        mandatory: false,
        basicProperties: props,
        body: body);

    return Results.Accepted($"/orders/{order.Id}", new { OrderId = order.Id });
});

app.Run();

public sealed record CreateOrderRequest(string Customer, decimal Amount, string? FailureMode);