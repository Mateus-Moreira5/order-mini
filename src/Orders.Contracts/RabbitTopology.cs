using System;
using RabbitMQ.Client;

namespace Orders.Contracts;

public static class RabbitTopology
{
    public const string Exchange = "orders";
    public const string ProcessQueue = "orders.process";
    public const string OrderCreatedKey = "order.created";

    public static async Task DeclareAsync(IChannel channel, CancellationToken ct = default)
    {
        await channel.ExchangeDeclareAsync(
            Exchange, ExchangeType.Direct, durable: true, autoDelete: false, cancellationToken: ct
        );
        await channel.QueueDeclareAsync(
            ProcessQueue, durable: true, exclusive: true, autoDelete: false, cancellationToken: ct
        );
        await channel.QueueBindAsync(
            ProcessQueue, Exchange, OrderCreatedKey, cancellationToken: ct
        );
    }
}
