using Confluent.Kafka;
using Serilog;

var logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console()
    .CreateLogger();

const string bootstrapServer = "localhost:9092";
const string topicName = "my-topic";

var config = new ProducerConfig()
{
    BootstrapServers = bootstrapServer,
};

using var producer = new ProducerBuilder<Null, string>(config).Build();

try
{
    logger.Information("Producer created");

    for (int i = 0; i < 48; i++)
    {
        logger.Debug("Sending message");
        
        await producer.ProduceAsync(
            topicName,
            new Message<Null, string>()
            {
                Value = $"[{DateTime.Now.ToFileTime()}] Hello World from producer",
            });   
    }

    logger.Information("Message sent to topic");
}
catch (ProduceException<Null, string> ex)
{
    logger.Error("Delivery failed: {@Exception}", ex);
}