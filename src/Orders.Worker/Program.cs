using Orders.Contracts;
using Orders.Worker;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<RabbitMqOptions>(
    builder.Configuration.GetSection(RabbitMqOptions.SectionName));

builder.Services.AddHostedService<OrderConsumer>();

var host = builder.Build();
host.Run();