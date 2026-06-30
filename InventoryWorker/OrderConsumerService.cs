using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace InventoryWorker;

public class OrderConsumerService : BackgroundService
{
    private readonly ILogger<OrderConsumerService> _logger;
    private readonly IConfiguration _config;
    private IConnection? _connection;
    private IModel? _channel;

    public OrderConsumerService(ILogger<OrderConsumerService> logger, IConfiguration config)
    {
        _logger = logger;
        _config = config;
    }

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _config["RabbitMQ:Host"] ?? "localhost",
            UserName = _config["RabbitMQ:User"] ?? "guest",
            Password = _config["RabbitMQ:Pass"] ?? "guest",
            DispatchConsumersAsync = true
        };

        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();

        // Same queue declaration as producer (idempotent)
        _channel.ExchangeDeclare("orders.dlx", ExchangeType.Fanout, durable: true);
        _channel.QueueDeclare("orders.deadletter", durable: true, exclusive: false,
            autoDelete: false);
        _channel.QueueBind("orders.deadletter", "orders.dlx", "");

        var args = new Dictionary<string, object>
        {
            { "x-dead-letter-exchange", "orders.dlx" }
        };
        _channel.QueueDeclare("orders.created", durable: true, exclusive: false,
            autoDelete: false, arguments: args);

        // Prefetch 10 messages at a time
        _channel.BasicQos(0, 10, false);

        _logger.LogInformation("InventoryWorker connected to RabbitMQ, waiting for orders...");

        return base.StartAsync(cancellationToken);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.Received += async (_, ea) =>
        {
            try
            {
                var body = Encoding.UTF8.GetString(ea.Body.ToArray());
                var order = JsonSerializer.Deserialize<OrderCreatedEvent>(body);

                if (order is null)
                {
                    _logger.LogWarning("Received null/malformed message, nacking");
                    _channel!.BasicNack(ea.DeliveryTag, false, false); // goes to DLQ
                    return;
                }

                // Simulate inventory processing
                _logger.LogInformation(
                    "Processing order {OrderId}: reserving {Quantity}x {Product}",
                    order.OrderId, order.Quantity, order.Product);

                await Task.Delay(200, stoppingToken); // simulate work

                _logger.LogInformation("Order {OrderId} processed successfully", order.OrderId);

                _channel!.BasicAck(ea.DeliveryTag, false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process message");
                _channel!.BasicNack(ea.DeliveryTag, false, false); // to DLQ
            }
        };

        _channel!.BasicConsume("orders.created", autoAck: false, consumer);
        return Task.CompletedTask;
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _channel?.Close();
        _connection?.Close();
        _logger.LogInformation("InventoryWorker disconnected from RabbitMQ");
        return base.StopAsync(cancellationToken);
    }
}

public record OrderCreatedEvent(string OrderId, string Product, int Quantity,
    DateTime CreatedAt);