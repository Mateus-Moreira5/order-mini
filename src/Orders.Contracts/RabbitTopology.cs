using RabbitMQ.Client;

namespace Orders.Contracts;

public static class RabbitTopology
{
    public const string Exchange = "orders";
    public const string ProcessQueue = "orders.process";
    public const string OrderCreatedKey = "order.created";

    public const string DeadLetterExchange = "orders.dlx";
    public const string RetryQueue = "orders.retry";
    public const string RetryKey = "order.retry";
    public const int RetryDelayMs = 10000;

    public const string ParkingQueue = "orders.parking";
    public const string ParkingKey = "order.parking";

    public static async Task DeclareAsync(IChannel channel, CancellationToken ct = default)
    {
        await channel.ExchangeDeclareAsync(
            Exchange, ExchangeType.Direct, durable: true, autoDelete: false, cancellationToken: ct);

        await channel.ExchangeDeclareAsync(
            DeadLetterExchange, ExchangeType.Direct, durable: true, autoDelete: false, cancellationToken: ct);

        await channel.QueueDeclareAsync(
            ParkingQueue, durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);

        await channel.QueueBindAsync(ParkingQueue, DeadLetterExchange, ParkingKey, cancellationToken: ct);

        var retryArguments = new Dictionary<string, object?>
        {
            ["x-message-ttl"] = RetryDelayMs,
            ["x-dead-letter-exchange"] = Exchange,
            ["x-dead-letter-routing-key"] = OrderCreatedKey
        };

        await channel.QueueDeclareAsync(
            RetryQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: retryArguments,
            cancellationToken: ct);

        await channel.QueueBindAsync(RetryQueue, DeadLetterExchange, RetryKey, cancellationToken: ct);

        var processArguments = new Dictionary<string, object?>
        {
            ["x-dead-letter-exchange"] = DeadLetterExchange,
            ["x-dead-letter-routing-key"] = RetryKey
        };

        await channel.QueueDeclareAsync(
            ProcessQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: processArguments,
            cancellationToken: ct);

        await channel.QueueBindAsync(ProcessQueue, Exchange, OrderCreatedKey, cancellationToken: ct);
    }
}