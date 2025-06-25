using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

IHost host = Host.CreateDefaultBuilder(args)
	.ConfigureServices((hostContext, services) =>
	{
		// Configure Kafka Consumer
		var consumerConfig = new ConsumerConfig
		{
			BootstrapServers = hostContext.Configuration["Kafka:BootstrapServers"] ?? "kafka:9092",
			GroupId = "matchmaking-worker-group",
			AutoOffsetReset = AutoOffsetReset.Earliest
		};
		services.AddSingleton<IConsumer<string, string>>(_ =>
			new ConsumerBuilder<string, string>(consumerConfig).Build());

		// Configure Kafka Producer
		var producerConfig = new ProducerConfig
		{
			BootstrapServers = hostContext.Configuration["Kafka:BootstrapServers"] ?? "kafka:9092"
		};
		services.AddSingleton<IProducer<string, string>>(_ =>
			new ProducerBuilder<string, string>(producerConfig).Build());

		// Register MatchMaker
		services.AddSingleton<MatchMaker>();

		// Register Worker
		services.AddHostedService<MatchMakingWorker>();
	})
	.Build();

await host.RunAsync();