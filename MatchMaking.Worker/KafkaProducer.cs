using Confluent.Kafka;

public class KafkaProducer
{
	private readonly IProducer<Null, string> _producer;
	private readonly ILogger<KafkaProducer> _logger;

	public KafkaProducer(IConfiguration config, ILogger<KafkaProducer> logger)
	{
		_logger = logger;
		var producerConfig = new ProducerConfig
		{
			BootstrapServers = config["Kafka:BootstrapServers"],
			SocketTimeoutMs = 10000,
			MessageTimeoutMs = 10000
		};
		int retries = 5;
		while (retries > 0)
		{
			try
			{
				_producer = new ProducerBuilder<Null, string>(producerConfig).Build();
				_logger.LogInformation("Kafka producer initialized successfully.");
				break;
			}
			catch (KafkaException ex)
			{
				_logger.LogError(ex, "Failed to initialize Kafka producer. Retries left: {Retries}", retries);
				retries--;
				if (retries == 0) throw;
				Thread.Sleep(5000); // Wait 5 seconds before retrying
			}
		}
	}

	public async Task ProduceAsync(string topic, string message)
	{
		int retries = 3;
		while (retries > 0)
		{
			try
			{
				await _producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
				_logger.LogInformation("Produced message to topic {Topic}: {Message}", topic, message);
				return;
			}
			catch (KafkaException ex)
			{
				_logger.LogError(ex, "Failed to produce message to {Topic}. Retries left: {Retries}", topic, retries);
				retries--;
				if (retries == 0) throw;
				await Task.Delay(2000); // Wait 2 seconds before retrying
			}
		}
	}
}