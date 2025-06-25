using Microsoft.Extensions.Hosting;
using Confluent.Kafka;
using System.Threading;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Newtonsoft.Json;

public class MatchMakingWorker : BackgroundService
{
	private readonly IConsumer<string, string> _kafkaConsumer;
	private readonly IProducer<string, string> _kafkaProducer;
	private readonly MatchMaker _matchMaker;

	public MatchMakingWorker(IConsumer<string, string> kafkaConsumer, IProducer<string, string> kafkaProducer, MatchMaker matchMaker)
	{
		_kafkaConsumer = kafkaConsumer;
		_kafkaProducer = kafkaProducer;
		_matchMaker = matchMaker;
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		_kafkaConsumer.Subscribe("matchmaking.request");

		while (!stoppingToken.IsCancellationRequested)
		{
			var result = _kafkaConsumer.Consume(stoppingToken);
			if (result != null)
			{
				_matchMaker.AddUser(result.Message.Value);
				if (_matchMaker.CanCreateMatch())
				{
					var match = _matchMaker.CreateMatch();
					await _kafkaProducer.ProduceAsync("matchmaking.complete",
						new Message<string, string> { Key = match.MatchId, Value = JsonConvert.SerializeObject(match) });
				}
			}
		}
	}
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
		return new Match (matchId, userIds);
	}
}

public class Match
{
	public string MatchId { get; set; }
	public List<string> UserIds { get; set; }

	public Match(string matchId, List<string> userIds)
	{
		MatchId = matchId;
		UserIds = userIds;
	}
}