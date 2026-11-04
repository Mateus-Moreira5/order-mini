using Orders.Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<OrderConsumer>();

var host = builder.Build();
host.Run();