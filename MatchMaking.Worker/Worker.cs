using System.Text.Json;

public class Worker : BackgroundService
{
	private readonly KafkaConsumer _consumer;
	private readonly KafkaProducer _producer;
	private readonly int _playersPerMatch;
	private readonly List<string> _pendingUsers = new();
	private readonly ILogger<Worker> _logger;

	public Worker(KafkaConsumer consumer, KafkaProducer producer, IConfiguration config, ILogger<Worker> logger)
	{
		_consumer = consumer;
		_producer = producer;
		_playersPerMatch = config.GetValue<int>("PlayersPerMatch", 3);
		_logger = logger;
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		_consumer.Subscribe("matchmaking.request");

		while (!stoppingToken.IsCancellationRequested)
		{
			var result = _consumer.Consume(stoppingToken);
			var userId = result.Message.Value;
			_pendingUsers.Add(userId);
			_logger.LogInformation("Received user {UserId} for matchmaking", userId);

			if (_pendingUsers.Count >= _playersPerMatch)
			{
				var matchId = Guid.NewGuid().ToString();
				var matchUsers = _pendingUsers.Take(_playersPerMatch).ToList();
				_pendingUsers.RemoveRange(0, _playersPerMatch);

				var message = JsonSerializer.Serialize(new { matchId, userIds = matchUsers });
				await _producer.ProduceAsync("matchmaking.complete", message);
				_logger.LogInformation("Created match {MatchId} with users {UserIds}", matchId, string.Join(", ", matchUsers));
			}
		}
	}
}
