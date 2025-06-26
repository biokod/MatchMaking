using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container
builder.Services.AddControllers();

// Configure Kafka Producer
var producerConfig = new ProducerConfig
{
	BootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "kafka:9092",
	AllowAutoCreateTopics = false
};
builder.Services.AddSingleton<IProducer<string, string>>(_ =>
	new ProducerBuilder<string, string>(producerConfig).Build());

// Configure Redis
builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
	ConnectionMultiplexer.Connect(builder.Configuration["Redis:ConnectionString"] ?? "redis:6379"));

// Register MatchRepository
builder.Services.AddSingleton<IMatchRepository, RedisMatchRepository>();

// Configure Kafka Consumer for matchmaking.complete
var consumerConfig = new ConsumerConfig
{
	BootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "kafka:9092",
	GroupId = "matchmaking-service-group",
	AutoOffsetReset = AutoOffsetReset.Earliest,
	AllowAutoCreateTopics = false
};
builder.Services.AddHostedService<KafkaMatchCompleteConsumer>();

var app = builder.Build();

// Configure the HTTP request pipeline
app.UseAuthorization();
app.MapControllers();

app.Run();

public class Match
{
	public required string MatchId { get; set; }
	public required List<string> UserIds { get; set; }
}

public interface IMatchRepository
{
	Task<Match?> GetMatchByUserId(string userId);
	Task SaveMatch(Match match);
}

public class RedisMatchRepository : IMatchRepository
{
	private readonly IDatabase _redis;

	public RedisMatchRepository(IConnectionMultiplexer redis)
	{
		_redis = redis.GetDatabase();
	}

	public async Task<Match?> GetMatchByUserId(string userId)
	{
		var matchId = await _redis.StringGetAsync($"user:{userId}:match");
		if (matchId.IsNull)
			return null;

		var matchJson = await _redis.StringGetAsync($"match:{matchId}");
		if (matchJson.IsNull)
			return null;

		return JsonSerializer.Deserialize<Match>(matchJson!);
	}

	public async Task SaveMatch(Match match)
	{
		var matchJson = JsonSerializer.Serialize(match);
		await _redis.StringSetAsync($"match:{match.MatchId}", matchJson);

		foreach (var userId in match.UserIds)
		{
			await _redis.StringSetAsync($"user:{userId}:match", match.MatchId);
		}
	}
}

public class KafkaMatchCompleteConsumer : BackgroundService
{
	private readonly IConsumer<string, string> _consumer;
	private readonly IMatchRepository _matchRepository;
	private readonly string _bootstrapServers;

	public KafkaMatchCompleteConsumer(IMatchRepository matchRepository, IConfiguration configuration)
	{
		_bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "kafka:9092";
		var consumerConfig = new ConsumerConfig
		{
			BootstrapServers = _bootstrapServers,
			GroupId = "matchmaking-service-group",
			AutoOffsetReset = AutoOffsetReset.Earliest,
			AllowAutoCreateTopics = false
		};
		_consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
		_matchRepository = matchRepository;
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		await EnsureTopicExistsAsync("matchmaking.complete", stoppingToken);

		while (!stoppingToken.IsCancellationRequested)
		{
			try
			{
				_consumer.Subscribe("matchmaking.complete");
				while (!stoppingToken.IsCancellationRequested)
				{
					try
					{
						var result = _consumer.Consume(stoppingToken);
						if (result?.Message?.Value != null)
						{
							var match = JsonSerializer.Deserialize<Match>(result.Message.Value);
							if (match != null)
							{
								await _matchRepository.SaveMatch(match);
							}
						}
					}
					catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
					{
						Console.WriteLine($"Topic 'matchmaking.complete' not found, retrying in 5 seconds...");
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
		_consumer.Close();
		_consumer.Dispose();
		base.Dispose();
	}
}