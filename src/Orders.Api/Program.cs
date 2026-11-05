using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Orders.Contracts;
using Orders.Data;
using RabbitMQ.Client;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<OrdersDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Orders")));

builder.Services.AddSingleton<IConnectionMultiplexer>(
    ConnectionMultiplexer.Connect(builder.Configuration.GetConnectionString("Redis")!));

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
    var order = new Orders.Data.Order
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

app.MapGet("/orders/{id:guid}", async (
    Guid id,
    OrdersDbContext db,
    IConnectionMultiplexer redis,
    ILogger<Program> logger) =>
{
    var cache = redis.GetDatabase();
    var key = CacheKeys.Order(id);

    var cached = await cache.StringGetAsync(key);
    if (cached.HasValue)
    {
        logger.LogInformation("Cache HIT para o pedido {OrderId}", id);
        return Results.Content(cached.ToString(), "application/json");
    }

    logger.LogInformation("Cache MISS para o pedido {OrderId}", id);

    var order = await db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.Id == id);
    if (order is null)
    {
        return Results.NotFound();
    }

    var response = new OrderResponse(
        order.Id,
        order.Customer,
        order.Amount,
        order.Status.ToString(),
        order.Attempts,
        order.LastError,
        order.CreatedAtUtc,
        order.UpdatedAtUtc);

    var json = JsonSerializer.Serialize(response, JsonSerializerOptions.Web);
    await cache.StringSetAsync(key, json, TimeSpan.FromSeconds(60));

    return Results.Content(json, "application/json");
});

app.Run();

public sealed record CreateOrderRequest(string Customer, decimal Amount, string? FailureMode);

public sealed record OrderResponse(
    Guid Id,
    string Customer,
    decimal Amount,
    string Status,
    int Attempts,
    string? LastError,
    DateTime CreatedAtUtc,
    DateTime? UpdatedAtUtc);