using Microsoft.AspNetCore.Mvc;
using Confluent.Kafka;
using System.Threading.Tasks;

[ApiController]
[Route("match")]
public class MatchController : ControllerBase
{
	private readonly IProducer<string, string> _kafkaProducer;
	private readonly IMatchRepository _matchRepository;

	public MatchController(IProducer<string, string> kafkaProducer, IMatchRepository matchRepository)
	{
		_kafkaProducer = kafkaProducer;
		_matchRepository = matchRepository;
	}

	[HttpPost("search")]
	public async Task<IActionResult> SearchMatch([FromQuery] string userId)
	{
		if (string.IsNullOrEmpty(userId))
			return BadRequest("userId is required");

		await _kafkaProducer.ProduceAsync("matchmaking.request", new Message<string, string> { Key = userId, Value = userId });
		return NoContent();
	}

	[HttpGet("info")]
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
