using Confluent.Kafka;
using Serilog;

var logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console()
    .CreateLogger();

const string bootstrapServer = "localhost:9092";
const string topicName = "my-topic";

var config = new ConsumerConfig()
{
    GroupId = "demo-consumer-group",
    BootstrapServers = bootstrapServer,
    AutoOffsetReset = AutoOffsetReset.Latest
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

consumer.Subscribe(topicName);

var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    logger.Warning("App shutting down");
    
    e.Cancel = true;
    cts.Cancel();
};

try
{
    logger.Information("Initializing consume of messages");
    
    while (true)
    {
        try
        {
            logger.Debug("Getting message");
            
            var consumeResult = consumer.Consume(cts.Token);
            
            logger.Information("Consumed message {@Result}", consumeResult);
        }
        catch (ConsumeException ex)
        {
            logger.Error("Error occured {ErrorReason}", ex.Error.Reason);
        }
    }
    
}
catch (OperationCanceledException ex)
{
    consumer.Close();
    logger.Warning("Operation canceled: {@Exception}", ex);
}