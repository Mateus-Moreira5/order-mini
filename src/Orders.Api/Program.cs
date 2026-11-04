using System.Text.Json;
using Orders.Contracts;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

var factory = new ConnectionFactory
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};

var connection = await factory.CreateConnectionAsync();

await using (var setupChannel = await connection.CreateChannelAsync())
{
    await RabbitTopology.DeclareAsync(setupChannel);
}

builder.Services.AddSingleton(connection);

var app = builder.Build();

app.MapPost("/orders", async (CreateOrderRequest request, IConnection conn) =>
{
    var message = new OrderCreated(
        OrderId: Guid.NewGuid(),
        MessageId: Guid.NewGuid(),
        Customer: request.Customer,
        Amount: request.Amount,
        FailureMode: request.FailureMode ?? "None",
        CreatedAtUtc: DateTime.UtcNow);

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

    return Results.Accepted($"/orders/{message.OrderId}", new { message.OrderId });
});

app.Run();

public sealed record CreateOrderRequest(string Customer, decimal Amount, string? FailureMode);