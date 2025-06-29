using Confluent.Kafka;
using Confluent.Kafka.Admin;

public class KafkaConsumer
{
	private readonly IConsumer<Null, string> _consumer;
	private readonly ILogger<KafkaConsumer> _logger;
	private readonly IConfiguration _config;

	public KafkaConsumer(IConfiguration config, ILogger<KafkaConsumer> logger)
	{
		_config = config;
		_logger = logger;
		var consumerConfig = new ConsumerConfig
		{
			BootstrapServers = config["Kafka:BootstrapServers"],
			GroupId = "matchmaking-worker",
			AutoOffsetReset = AutoOffsetReset.Earliest,
			SocketTimeoutMs = 10000
		};
		int retries = 5;
		while (retries > 0)
		{
			try
			{
				_consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
				_logger.LogInformation("Kafka consumer initialized successfully.");
				break;
			}
			catch (KafkaException ex)
			{
				_logger.LogError(ex, "Failed to initialize Kafka consumer. Retries left: {Retries}", retries);
				retries--;
				if (retries == 0) throw;
				Thread.Sleep(5000); // Wait 5 seconds before retrying
			}
		}

		using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = config["Kafka:BootstrapServers"] }).Build();
		try
		{
			admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "matchmaking.request", NumPartitions = 1, ReplicationFactor = 1 } }).GetAwaiter().GetResult();
			admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "matchmaking.complete", NumPartitions = 1, ReplicationFactor = 1 } }).GetAwaiter().GetResult();
			_logger.LogInformation("Kafka topics created or already exist.");
		}
		catch (CreateTopicsException ex)
		{
			_logger.LogWarning(ex, "Failed to create topics (they may already exist).");
		}
	}

	public void Subscribe(string topic)
	{
		_consumer.Subscribe(topic);
		_logger.LogInformation("Subscribed to Kafka topic: {Topic}", topic);
	}

	public ConsumeResult<Null, string> Consume(CancellationToken cancellationToken)
	{
		int retries = 3;
		while (retries > 0)
		{
			try
			{
				var result = _consumer.Consume(cancellationToken);
				_logger.LogInformation("Consumed message from topic {Topic}", result.Topic);
				return result;
			}
			catch (KafkaException ex)
			{
				_logger.LogError(ex, "Failed to consume message. Retries left: {Retries}", retries);
				retries--;
				if (retries == 0) throw;
				Thread.Sleep(2000); // Wait 2 seconds before retrying
			}
		}
		throw new KafkaException(new Error(ErrorCode.Unknown, "Failed to consume message after retries"));
	}
}
