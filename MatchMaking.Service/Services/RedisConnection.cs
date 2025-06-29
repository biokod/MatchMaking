using StackExchange.Redis;
using System.Text.Json;

public class RedisConnection
{
	private readonly ConnectionMultiplexer _redis;
	private readonly ILogger<RedisConnection> _logger;

	public RedisConnection(IConfiguration config, ILogger<RedisConnection> logger)
	{
		_logger = logger;
		try
		{
			_redis = ConnectionMultiplexer.Connect(config["Redis:ConnectionString"]);
			_logger.LogInformation("Redis connection established successfully.");
		}
		catch (RedisConnectionException ex)
		{
			_logger.LogError(ex, "Failed to connect to Redis at {ConnectionString}", config["Redis:ConnectionString"]);
			throw;
		}
	}

	public async Task SaveMatchInfoAsync(string matchId, List<string> userIds)
	{
		var db = _redis.GetDatabase();
		var matchInfo = JsonSerializer.Serialize(new { matchId, userIds });
		foreach (var userId in userIds)
		{
			await db.StringSetAsync(userId, matchInfo);
			_logger.LogInformation("Saved match info for matchId: {MatchId}, userId: {UserId}", matchId, userId);
		}
	}

	public async Task<object> GetMatchInfoAsync(string userId)
	{
		var db = _redis.GetDatabase();
		var value = await db.StringGetAsync(userId);
		return value.HasValue ? JsonSerializer.Deserialize<object>(value) : null;
	}
}
