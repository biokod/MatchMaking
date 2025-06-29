using Microsoft.AspNetCore.Mvc;

namespace MatchMaking.Service.Controllers;


[ApiController]
[Route("api/matchmaking")]
public class MatchMakingController : ControllerBase
{
	private readonly KafkaProducer _producer;
	private readonly RedisConnection _redis;
	private readonly ILogger<MatchMakingController> _logger;

	public MatchMakingController(KafkaProducer producer, RedisConnection redis, ILogger<MatchMakingController> logger)
	{
		_producer = producer;
		_redis = redis;
		_logger = logger;
	}

	[HttpPost("search")]
	public async Task<IActionResult> SearchMatch([FromBody] string userId)
	{
		_logger.LogInformation("Received POST /api/matchmaking/search for userId: {UserId}", userId);
		if (string.IsNullOrEmpty(userId))
		{
			_logger.LogWarning("Invalid userId received: {UserId}", userId);
			return BadRequest("UserId cannot be empty");
		}

		try
		{
			await _producer.ProduceAsync("matchmaking.request", userId);
			_logger.LogInformation("Successfully sent match request for userId: {UserId}", userId);
			return NoContent();
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Failed to process match request for userId: {UserId}", userId);
			return StatusCode(500, "Internal server error");
		}
	}

	[HttpGet("info")]
	public async Task<IActionResult> GetMatchInfo([FromQuery] string userId)
	{
		_logger.LogInformation("Received GET /api/matchmaking/info for userId: {UserId}", userId);
		if (string.IsNullOrEmpty(userId))
		{
			_logger.LogWarning("Invalid userId received: {UserId}", userId);
			return BadRequest("UserId cannot be empty");
		}

		try
		{
			var matchInfo = await _redis.GetMatchInfoAsync(userId);
			if (matchInfo == null)
			{
				_logger.LogInformation("No match found for userId: {UserId}", userId);
				return NotFound();
			}
			_logger.LogInformation("Returning match info for userId: {UserId}", userId);
			return Ok(matchInfo);
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Failed to retrieve match info for userId: {UserId}", userId);
			return StatusCode(500, "Internal server error");
		}
	}
}