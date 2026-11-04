using System.Text.Json;
using Orders.Contracts;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Orders.Worker;

public sealed class OrderConsumer(ILogger<OrderConsumer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        };

        await using var connection = await factory.CreateConnectionAsync(stoppingToken);
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
            autoAck: true,
            consumer: consumer,
            cancellationToken: stoppingToken);

        logger.LogInformation("Worker aguardando mensagens na fila {Queue}", RabbitTopology.ProcessQueue);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async Task HandleAsync(IChannel channel, BasicDeliverEventArgs ea, CancellationToken ct)
    {
        var order = JsonSerializer.Deserialize<OrderCreated>(ea.Body.Span)!;

        logger.LogInformation(
            "Recebido pedido {OrderId} (MessageId={MessageId}, Redelivered={Redelivered}, FailureMode={FailureMode})",
            order.OrderId, order.MessageId, ea.Redelivered, order.FailureMode);

        try
        {
            await ProcessAsync(order, ct);

            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false, cancellationToken: ct);

            logger.LogInformation("Pedido {OrderId} concluído e confirmado (ack)", order.OrderId);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning(
                "Worker interrompido durante o pedido {OrderId}; sem ack, a mensagem volta para a fila",
                order.OrderId);
        }
    }

    private static Task ProcessAsync(OrderCreated order, CancellationToken ct)
    {
        var duration = order.FailureMode == "Slow" ? TimeSpan.FromSeconds(30) : TimeSpan.FromSeconds(2);
        return Task.Delay(duration, ct);
    }
}