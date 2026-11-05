using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Orders.Contracts;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Orders.Worker;

public sealed class OrderConsumer(
    IOptions<RabbitMqOptions> options,
    ILogger<OrderConsumer> logger) : BackgroundService
{
    private const int MaxAttempts = 3;
    private readonly RabbitMqOptions _rabbit = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using var connection = await _rabbit.CreateFactory().CreateConnectionAsync(stoppingToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        await RabbitTopology.DeclareAsync(channel, stoppingToken);

        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: 1,
            global: false,
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) => await HandleAsync(channel, ea, stoppingToken);

        await channel.BasicConsumeAsync(
            queue: RabbitTopology.ProcessQueue,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        logger.LogInformation("Worker aguardando mensagens na fila {Queue}", RabbitTopology.ProcessQueue);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async Task HandleAsync(IChannel channel, BasicDeliverEventArgs ea, CancellationToken ct)
    {
        var order = JsonSerializer.Deserialize<OrderCreated>(ea.Body.Span)!;
        var failures = GetFailureCount(ea.BasicProperties);
        var attempt = failures + 1;

        logger.LogInformation(
            "Recebido pedido {OrderId} (Attempt={Attempt}, MessageId={MessageId}, FailureMode={FailureMode})",
            order.OrderId, attempt, order.MessageId, order.FailureMode);

        try
        {
            await ProcessAsync(order, failures, ct);

            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false, cancellationToken: ct);

            logger.LogInformation("Pedido {OrderId} concluído e confirmado (ack)", order.OrderId);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning(
                "Worker interrompido durante o pedido {OrderId}; sem ack, a mensagem volta para a fila",
                order.OrderId);
        }
        catch (Exception ex)
        {
            if (attempt >= MaxAttempts)
            {
                logger.LogError(
                    "Pedido {OrderId} esgotou {Attempts} tentativas: {Error}. Enviando para a parking queue",
                    order.OrderId, attempt, ex.Message);

                await SendToParkingAsync(channel, ea, attempt, ex.Message, ct);

                await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false, cancellationToken: ct);
            }
            else
            {
                logger.LogWarning(
                    "Falha na tentativa {Attempt} do pedido {OrderId}: {Error}. Enviando para a fila de retry",
                    attempt, order.OrderId, ex.Message);

                await channel.BasicNackAsync(
                    deliveryTag: ea.DeliveryTag,
                    multiple: false,
                    requeue: false,
                    cancellationToken: ct);
            }
        }
    }

    private static async Task SendToParkingAsync(
        IChannel channel,
        BasicDeliverEventArgs ea,
        long attempts,
        string error,
        CancellationToken ct)
    {
        var properties = new BasicProperties
        {
            Persistent = true,
            MessageId = ea.BasicProperties.MessageId,
            ContentType = ea.BasicProperties.ContentType,
            Headers = new Dictionary<string, object?>
            {
                ["x-attempts"] = attempts,
                ["x-last-error"] = error
            }
        };

        await channel.BasicPublishAsync(
            exchange: RabbitTopology.DeadLetterExchange,
            routingKey: RabbitTopology.ParkingKey,
            mandatory: false,
            basicProperties: properties,
            body: ea.Body,
            cancellationToken: ct);
    }

    private static long GetFailureCount(IReadOnlyBasicProperties properties)
    {
        if (properties.Headers is null ||
            !properties.Headers.TryGetValue("x-death", out var raw) ||
            raw is not IList<object?> deaths)
        {
            return 0;
        }

        foreach (var item in deaths)
        {
            if (item is not IDictionary<string, object?> death)
            {
                continue;
            }

            var queue = Encoding.UTF8.GetString((byte[])death["queue"]!);
            var reason = Encoding.UTF8.GetString((byte[])death["reason"]!);

            if (queue == RabbitTopology.ProcessQueue && reason == "rejected")
            {
                return (long)death["count"]!;
            }
        }

        return 0;
    }

    private static async Task ProcessAsync(OrderCreated order, long failures, CancellationToken ct)
    {
        switch (order.FailureMode)
        {
            case "Permanent":
                throw new InvalidOperationException("Falha permanente simulada");
            case "Transient" when failures < 2:
                throw new InvalidOperationException("Falha transitória simulada");
            case "Slow":
                await Task.Delay(TimeSpan.FromSeconds(30), ct);
                break;
            default:
                await Task.Delay(TimeSpan.FromSeconds(2), ct);
                break;
        }
    }
}