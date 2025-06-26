using Microsoft.AspNetCore.Mvc;
using Confluent.Kafka;
using System.Threading.Tasks;

public class MatchMakingService : ControllerBase
{
	private readonly IProducer<string, string> _kafkaProducer;
	private readonly IMatchRepository _matchRepository;

	public MatchMakingService(IProducer<string, string> kafkaProducer, IMatchRepository matchRepository)
	{
		_kafkaProducer = kafkaProducer;
		_matchRepository = matchRepository;
	}

	[HttpPost("match/search")]
	public async Task<IActionResult> SearchMatch([FromQuery] string userId)
	{
		if (string.IsNullOrEmpty(userId))
			return BadRequest("userId is required");

		await _kafkaProducer.ProduceAsync("matchmaking.request", new Message<string, string> { Key = userId, Value = userId });
		return NoContent();
	}

	[HttpGet("match/info")]
	public async Task<IActionResult> GetMatchInfo([FromQuery] string userId)
	{
		if (string.IsNullOrEmpty(userId))
			return BadRequest("userId is required");

		var match = await _matchRepository.GetMatchByUserId(userId);
		if (match == null)
			return NotFound();

		return Ok(new { match.MatchId, match.UserIds });
	}
}
