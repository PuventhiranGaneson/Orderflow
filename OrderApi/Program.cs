using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using Serilog;
using Serilog.Sinks.Elasticsearch;

var builder = WebApplication.CreateBuilder(args);

// --- Serilog setup ---
builder.Host.UseSerilog((ctx, config) =>
{
    config
        .ReadFrom.Configuration(ctx.Configuration)
        .Enrich.FromLogContext()
        .Enrich.WithMachineName()
        .Enrich.WithProperty("Service", "OrderApi")
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

var app = builder.Build();

// --- Health check endpoint (needed for K8s probes on Day 2) ---
app.MapGet("/healthz", () => Results.Ok(new { status = "healthy" }));

// --- The single business endpoint ---
app.MapPost("/orders", (OrderRequest order, IConfiguration config, ILogger<Program> logger) =>
{
    var orderId = Guid.NewGuid().ToString("N")[..8];

    logger.LogInformation("Order received: {OrderId}, Product: {Product}, Qty: {Quantity}",
        orderId, order.Product, order.Quantity);

    // Publish to RabbitMQ
    var factory = new ConnectionFactory
    {
        HostName = config["RabbitMQ:Host"] ?? "localhost",
        UserName = config["RabbitMQ:User"] ?? "guest",
        Password = config["RabbitMQ:Pass"] ?? "guest"
    };

    using var connection = factory.CreateConnection();
    using var channel = connection.CreateModel();

    // Declare the main queue with a dead-letter exchange
    channel.ExchangeDeclare("orders.dlx", ExchangeType.Fanout, durable: true);
    channel.QueueDeclare("orders.deadletter", durable: true, exclusive: false,
        autoDelete: false);
    channel.QueueBind("orders.deadletter", "orders.dlx", "");

    var args = new Dictionary<string, object>
    {
        { "x-dead-letter-exchange", "orders.dlx" }
    };
    channel.QueueDeclare("orders.created", durable: true, exclusive: false,
        autoDelete: false, arguments: args);

    var message = new OrderCreatedEvent(orderId, order.Product, order.Quantity,
        DateTime.UtcNow);
    var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

    var props = channel.CreateBasicProperties();
    props.Persistent = true; // survive broker restart

    channel.BasicPublish("", "orders.created", props, body);

    logger.LogInformation("Published OrderCreated event: {OrderId}", orderId);

    return Results.Accepted($"/orders/{orderId}", new { orderId, status = "accepted" });
});

app.Run();

// --- Models ---
public record OrderRequest(string Product, int Quantity);
public record OrderCreatedEvent(string OrderId, string Product, int Quantity,
    DateTime CreatedAt);