using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Orders.Contracts;
using Orders.Data;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog.Context;
using StackExchange.Redis;

namespace Orders.Worker;

public sealed class OrderConsumer(
    IOptions<RabbitMqOptions> options,
    IDbContextFactory<OrdersDbContext> dbFactory,
    IConnectionMultiplexer redis,
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
        var message = JsonSerializer.Deserialize<OrderCreated>(ea.Body.Span)!;
        var failures = DeathHeaderReader.CountRejections(ea.BasicProperties.Headers, RabbitTopology.ProcessQueue);
        var attempt = failures + 1;

        var correlationId = ea.BasicProperties.CorrelationId ?? "sem-correlation-id";
        using var correlationScope = LogContext.PushProperty("CorrelationId", correlationId);

        logger.LogInformation(
            "Recebido pedido {OrderId} (Attempt={Attempt}, MessageId={MessageId}, FailureMode={FailureMode})",
            message.OrderId, attempt, message.MessageId, message.FailureMode);

        try
        {
            await using var db = await dbFactory.CreateDbContextAsync(ct);

            var order = await db.Orders.FirstOrDefaultAsync(o => o.Id == message.OrderId, ct)
                ?? throw new InvalidOperationException($"Pedido {message.OrderId} não encontrado no banco");

            if (order.Status == OrderStatus.Completed)
            {
                logger.LogWarning(
                    "Pedido {OrderId} já estava concluído; mensagem duplicada ignorada",
                    order.Id);

                await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false, cancellationToken: ct);
                return;
            }

            order.Status = OrderStatus.Processing;
            order.Attempts = (int)attempt;
            order.UpdatedAtUtc = DateTime.UtcNow;
            await db.SaveChangesAsync(ct);
            await InvalidateCacheAsync(order.Id);

            await ProcessAsync(message, failures, ct);

            order.Status = OrderStatus.Completed;
            order.LastError = null;
            order.UpdatedAtUtc = DateTime.UtcNow;
            await db.SaveChangesAsync(ct);
            await InvalidateCacheAsync(order.Id);

            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false, cancellationToken: ct);

            logger.LogInformation("Pedido {OrderId} concluído e confirmado (ack)", order.Id);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning(
                "Worker interrompido durante o pedido {OrderId}; sem ack, a mensagem volta para a fila",
                message.OrderId);
        }
        catch (Exception ex)
        {
            var decision = RetryPolicy.Decide(attempt, MaxAttempts);
            var exhausted = decision == RetryDecision.Park;

            await TryRecordFailureAsync(message.OrderId, attempt, ex.Message, exhausted, ct);

            if (exhausted)
            {
                logger.LogError(
                    ex,
                    "Pedido {OrderId} esgotou {Attempts} tentativas. Enviando para a parking queue",
                    message.OrderId, attempt);

                await SendToParkingAsync(channel, ea, attempt, ex.Message, ct);

                await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false, cancellationToken: ct);
            }
            else
            {
                logger.LogWarning(
                    "Falha na tentativa {Attempt} do pedido {OrderId}: {Error}. Enviando para a fila de retry",
                    attempt, message.OrderId, ex.Message);

                await channel.BasicNackAsync(
                    deliveryTag: ea.DeliveryTag,
                    multiple: false,
                    requeue: false,
                    cancellationToken: ct);
            }
        }
    }

    private async Task InvalidateCacheAsync(Guid orderId)
    {
        try
        {
            await redis.GetDatabase().KeyDeleteAsync(CacheKeys.Order(orderId));
            logger.LogInformation("Cache do pedido {OrderId} invalidado", orderId);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                "Não foi possível invalidar o cache do pedido {OrderId}: {Error}",
                orderId, ex.Message);
        }
    }

    private async Task TryRecordFailureAsync(
        Guid orderId,
        long attempt,
        string error,
        bool exhausted,
        CancellationToken ct)
    {
        try
        {
            await using var db = await dbFactory.CreateDbContextAsync(ct);

            var order = await db.Orders.FirstOrDefaultAsync(o => o.Id == orderId, ct);
            if (order is null)
            {
                return;
            }

            order.Status = exhausted ? OrderStatus.Failed : OrderStatus.Retrying;
            order.Attempts = (int)attempt;
            order.LastError = error.Length > 1000 ? error[..1000] : error;
            order.UpdatedAtUtc = DateTime.UtcNow;

            await db.SaveChangesAsync(ct);
            await InvalidateCacheAsync(orderId);
        }
        catch (Exception dbEx)
        {
            logger.LogError(
                "Não foi possível registrar a falha do pedido {OrderId} no banco: {Error}",
                orderId, dbEx.Message);
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
            CorrelationId = ea.BasicProperties.CorrelationId,
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