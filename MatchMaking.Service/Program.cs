using Confluent.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;
using System.Text.Json;
using System.Text.RegularExpressions;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container
builder.Services.AddControllers();

// Configure Kafka Producer
var producerConfig = new ProducerConfig
{
	BootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "kafka:9092"
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
	AutoOffsetReset = AutoOffsetReset.Earliest
};
builder.Services.AddHostedService<KafkaMatchCompleteConsumer>();

var app = builder.Build();

// Configure the HTTP request pipeline
app.UseAuthorization();
app.MapControllers();

app.Run();

public class RedisMatchRepository : IMatchRepository
{
	private readonly IDatabase _redis;

	public RedisMatchRepository(IConnectionMultiplexer redis)
	{
		_redis = redis.GetDatabase();
	}

	public async Task<Match> GetMatchByUserId(string userId)
	{
		var matchId = await _redis.StringGetAsync($"user:{userId}:match");
		if (matchId.IsNull)
			return null;

		var matchJson = await _redis.StringGetAsync($"match:{matchId}");
		if (matchJson.IsNull)
			return null;

		return JsonSerializer.Deserialize<Match>(matchJson);
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

// Background service to consume matchmaking.complete topic
public class KafkaMatchCompleteConsumer : BackgroundService
{
	private readonly IConsumer<string, string> _consumer;
	private readonly IMatchRepository _matchRepository;

	public KafkaMatchCompleteConsumer(IMatchRepository matchRepository)
	{
		var consumerConfig = new ConsumerConfig
		{
			BootstrapServers = "kafka:9092",
			GroupId = "matchmaking-service-group",
			AutoOffsetReset = AutoOffsetReset.Earliest
		};
		_consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
		_matchRepository = matchRepository;
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		_consumer.Subscribe("matchmaking.complete");

		while (!stoppingToken.IsCancellationRequested)
		{
			var result = _consumer.Consume(stoppingToken);
			if (result?.Message?.Value != null)
			{
				var match = JsonSerializer.Deserialize<Match>(result.Message.Value);
				await _matchRepository.SaveMatch(match);
			}
		}
	}
}