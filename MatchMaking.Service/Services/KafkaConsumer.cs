using Confluent.Kafka;
using System.Text.Json;
using Confluent.Kafka.Admin;

public class KafkaConsumer : BackgroundService
{
	private readonly IConsumer<Null, string> _consumer;
	private readonly RedisConnection _redis;
	private readonly IConfiguration _config;
	private readonly ILogger<KafkaConsumer> _logger;

	public KafkaConsumer(RedisConnection redis, IConfiguration config, ILogger<KafkaConsumer> logger)
	{
		_redis = redis;
		_config = config;
		_logger = logger;
		var consumerConfig = new ConsumerConfig
		{
			BootstrapServers = config["Kafka:BootstrapServers"],
			GroupId = "matchmaking-service",
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
				Thread.Sleep(5000);
			}
		}
	}
		protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		int retries = 3;
		while (retries > 0 && !stoppingToken.IsCancellationRequested)
		{
			try
			{
				_consumer.Subscribe("matchmaking.complete");

				using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _config["Kafka:BootstrapServers"] }).Build();
				try
				{
					await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "matchmaking.complete", NumPartitions = 1, ReplicationFactor = 1 } });
					await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "matchmaking.request", NumPartitions = 1, ReplicationFactor = 1 } });
					_logger.LogInformation("Kafka topics created or already exist.");
				}
				catch (CreateTopicsException ex)
				{
					_logger.LogWarning(ex, "Failed to create topics (they may already exist).");
				}

				while (!stoppingToken.IsCancellationRequested)
				{
					var result = _consumer.Consume(stoppingToken);
					var message = JsonSerializer.Deserialize<MatchMessage>(result.Message.Value);
					await _redis.SaveMatchInfoAsync(message.matchId, message.userIds);
					_logger.LogInformation("Consumed message from matchmaking.complete: {Message}", result.Message.Value);
				}
			}
			catch (KafkaException ex)
			{
				_logger.LogError(ex, "Kafka consumer error. Retries left: {Retries}", retries);
				retries--;
				if (retries == 0) throw;
				await Task.Delay(5000, stoppingToken);
			}
		}
	}
}
