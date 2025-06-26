using Microsoft.Extensions.Hosting;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.Text.RegularExpressions;

public class MatchMakingWorker : BackgroundService
{
	private readonly IConsumer<string, string> _kafkaConsumer;
	private readonly IProducer<string, string> _kafkaProducer;
	private readonly MatchMaker _matchMaker;
	private readonly string _bootstrapServers;

	public MatchMakingWorker(IConsumer<string, string> kafkaConsumer, IProducer<string, string> kafkaProducer, MatchMaker matchMaker, IConfiguration configuration)
	{
		_kafkaConsumer = kafkaConsumer;
		_kafkaProducer = kafkaProducer;
		_matchMaker = matchMaker;
		_bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "kafka:9092";
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		await EnsureTopicExistsAsync("matchmaking.request", stoppingToken);

		while (!stoppingToken.IsCancellationRequested)
		{
			try
			{
				_kafkaConsumer.Subscribe("matchmaking.request");
				while (!stoppingToken.IsCancellationRequested)
				{
					try
					{
						var result = _kafkaConsumer.Consume(stoppingToken);
						if (result?.Message?.Value != null)
						{
							_matchMaker.AddUser(result.Message.Value);
							if (_matchMaker.CanCreateMatch())
							{
								var match = _matchMaker.CreateMatch();
								await _kafkaProducer.ProduceAsync("matchmaking.complete",
									new Message<string, string> { Key = match.MatchId, Value = Newtonsoft.Json.JsonConvert.SerializeObject(match) });
							}
						}
					}
					catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
					{
						Console.WriteLine($"Topic 'matchmaking.request' not found, retrying in 5 seconds...");
						await Task.Delay(5000, stoppingToken);
					}
					catch (KafkaException ex) when (ex.Error.IsBrokerError)
					{
						Console.WriteLine($"Kafka broker error: {ex.Message}, retrying in 5 seconds...");
						await Task.Delay(5000, stoppingToken);
					}
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error in Kafka consumer: {ex.Message}, retrying in 5 seconds...");
				await Task.Delay(5000, stoppingToken);
			}
		}
	}

	private async Task EnsureTopicExistsAsync(string topic, CancellationToken cancellationToken)
	{
		var adminConfig = new AdminClientConfig
		{
			BootstrapServers = _bootstrapServers
		};

		using var adminClient = new AdminClientBuilder(adminConfig).Build();
		try
		{
			var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
			if (!metadata.Topics.Any(t => t.Topic == topic))
			{
				await adminClient.CreateTopicsAsync(new[]
				{
					new TopicSpecification
					{
						Name = topic,
						NumPartitions = 1,
						ReplicationFactor = 1
					}
				});
				Console.WriteLine($"Created topic: {topic}");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Failed to create topic {topic}: {ex.Message}, retrying in 5 seconds...");
			await Task.Delay(5000, cancellationToken);
			await EnsureTopicExistsAsync(topic, cancellationToken); // Retry
		}
	}

	public override void Dispose()
	{
		_kafkaConsumer.Close();
		_kafkaConsumer.Dispose();
		_kafkaProducer.Dispose();
		base.Dispose();
	}
}

public class Match
{
	public required string MatchId { get; set; }
	public required List<string> UserIds { get; set; }
}

public class MatchMaker
{
	private readonly int _usersPerMatch;
	private readonly List<string> _waitingUsers = new();

	public MatchMaker(IConfiguration config)
	{
		_usersPerMatch = config.GetValue<int>("MatchMaking:UsersPerMatch", 3);
	}

	public void AddUser(string userId)
	{
		_waitingUsers.Add(userId);
	}

	public bool CanCreateMatch()
	{
		return _waitingUsers.Count >= _usersPerMatch;
	}

	public Match CreateMatch()
	{
		var matchId = Guid.NewGuid().ToString();
		var userIds = _waitingUsers.Take(_usersPerMatch).ToList();
		_waitingUsers.RemoveRange(0, _usersPerMatch);
		return new Match { MatchId = matchId, UserIds = userIds };
	}
}