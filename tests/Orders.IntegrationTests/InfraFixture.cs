using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orders.Contracts;
using Orders.Data;
using Orders.Worker;
using RabbitMQ.Client;
using StackExchange.Redis;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;
using Testcontainers.Redis;
using Xunit;

namespace Orders.IntegrationTests;

public sealed record ParkedMessage(string? CorrelationId, IDictionary<string, object?> Headers);

public sealed class InfraFixture : IAsyncLifetime
{
    private const string RabbitUser = "orders";
    private const string RabbitPassword = "orders";

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:17").Build();

    private readonly RabbitMqContainer _rabbit = new RabbitMqBuilder("rabbitmq:4-management")
        .WithUsername(RabbitUser)
        .WithPassword(RabbitPassword)
        .Build();

    private readonly RedisContainer _redis = new RedisBuilder("redis:7-alpine").Build();

    private WebApplicationFactory<CreateOrderRequest>? _apiFactory;
    private IHost? _workerHost;
    private DbContextOptions<OrdersDbContext>? _dbOptions;

    public HttpClient Client { get; private set; } = null!;

    public RabbitMqOptions RabbitOptions { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await Task.WhenAll(_postgres.StartAsync(), _rabbit.StartAsync(), _redis.StartAsync());

        RabbitOptions = new RabbitMqOptions
        {
            HostName = _rabbit.Hostname,
            Port = _rabbit.GetMappedPublicPort(5672),
            UserName = RabbitUser,
            Password = RabbitPassword,
            VirtualHost = "/"
        };

        await ProbeRabbitAsync();

        _dbOptions = new DbContextOptionsBuilder<OrdersDbContext>()
            .UseNpgsql(_postgres.GetConnectionString())
            .Options;

        await using (var db = new OrdersDbContext(_dbOptions))
        {
            await db.Database.MigrateAsync();
        }

        Environment.SetEnvironmentVariable("ConnectionStrings__Orders", _postgres.GetConnectionString());
        Environment.SetEnvironmentVariable("ConnectionStrings__Redis", _redis.GetConnectionString());
        Environment.SetEnvironmentVariable("RabbitMq__HostName", RabbitOptions.HostName);
        Environment.SetEnvironmentVariable("RabbitMq__Port", RabbitOptions.Port.ToString());
        Environment.SetEnvironmentVariable("RabbitMq__UserName", RabbitOptions.UserName);
        Environment.SetEnvironmentVariable("RabbitMq__Password", RabbitOptions.Password);
        Environment.SetEnvironmentVariable("RabbitMq__VirtualHost", RabbitOptions.VirtualHost);

        _apiFactory = new WebApplicationFactory<CreateOrderRequest>();

        try
        {
            Client = _apiFactory.CreateClient();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"A API falhou ao iniciar. O contêiner do RabbitMQ aceitou a conexão direta, então o problema está na configuração que a API recebeu. Esperado: {RabbitOptions.HostName}:{RabbitOptions.Port}, usuário {RabbitOptions.UserName}.",
                ex);
        }

        var builder = Host.CreateApplicationBuilder();
        builder.Services.Configure<RabbitMqOptions>(options =>
        {
            options.HostName = RabbitOptions.HostName;
            options.Port = RabbitOptions.Port;
            options.UserName = RabbitOptions.UserName;
            options.Password = RabbitOptions.Password;
            options.VirtualHost = RabbitOptions.VirtualHost;
        });
        builder.Services.AddDbContextFactory<OrdersDbContext>(options =>
            options.UseNpgsql(_postgres.GetConnectionString()));
        builder.Services.AddSingleton<IConnectionMultiplexer>(
            ConnectionMultiplexer.Connect(_redis.GetConnectionString()));
        builder.Services.AddHostedService<OrderConsumer>();

        _workerHost = builder.Build();
        await _workerHost.StartAsync();
    }

    private async Task ProbeRabbitAsync()
    {
        try
        {
            await using var connection = await RabbitOptions.CreateFactory().CreateConnectionAsync();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Conexão direta ao RabbitMQ do contêiner falhou ({RabbitOptions.HostName}:{RabbitOptions.Port}, usuário {RabbitOptions.UserName}). O problema está no contêiner ou nas credenciais, não na API.",
                ex);
        }
    }

    public async Task DisposeAsync()
    {
        if (_workerHost is not null)
        {
            await _workerHost.StopAsync();
            _workerHost.Dispose();
        }

        Client?.Dispose();

        if (_apiFactory is not null)
        {
            await _apiFactory.DisposeAsync();
        }

        await _postgres.DisposeAsync();
        await _rabbit.DisposeAsync();
        await _redis.DisposeAsync();
    }

    public OrdersDbContext NewDbContext() => new(_dbOptions!);

    public async Task<Orders.Data.Order> WaitForOrderAsync(Guid id, Func<Orders.Data.Order, bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        Orders.Data.Order? last = null;

        while (DateTime.UtcNow < deadline)
        {
            await using var db = NewDbContext();
            last = await db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.Id == id);

            if (last is not null && predicate(last))
            {
                return last;
            }

            await Task.Delay(250);
        }

        throw new TimeoutException(
            $"Pedido {id} não chegou ao estado esperado em {timeout}. Último status: {last?.Status.ToString() ?? "inexistente"}");
    }

    public async Task<ParkedMessage?> FindInParkingAsync(string correlationId, TimeSpan timeout)
    {
        await using var connection = await RabbitOptions.CreateFactory().CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();

        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            var result = await channel.BasicGetAsync(RabbitTopology.ParkingQueue, autoAck: true);

            if (result is null)
            {
                await Task.Delay(250);
                continue;
            }

            if (result.BasicProperties.CorrelationId == correlationId)
            {
                return new ParkedMessage(
                    result.BasicProperties.CorrelationId,
                    result.BasicProperties.Headers ?? new Dictionary<string, object?>());
            }
        }

        return null;
    }
}

[CollectionDefinition(Name)]
public class InfraCollection : ICollectionFixture<InfraFixture>
{
    public const string Name = "infra";
}