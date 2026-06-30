using InventoryWorker;
using Serilog;
using Serilog.Sinks.Elasticsearch;

var builder = Host.CreateDefaultBuilder(args);

builder.UseSerilog((ctx, config) =>
{
    config
        .ReadFrom.Configuration(ctx.Configuration)
        .Enrich.FromLogContext()
        .Enrich.WithMachineName()
        .Enrich.WithProperty("Service", "InventoryWorker")
        .WriteTo.Console()
        .WriteTo.Elasticsearch(new ElasticsearchSinkOptions(
            new Uri(ctx.Configuration["ElasticSearch:Uri"] ?? "http://localhost:9200"))
        {
            AutoRegisterTemplate = true,
            IndexFormat = "orderflow-logs-{0:yyyy.MM.dd}",
            NumberOfReplicas = 0,
            NumberOfShards = 1
        });
});

builder.ConfigureServices((ctx, services) =>
{
    services.AddHostedService<OrderConsumerService>();
});

var host = builder.Build();
host.Run();