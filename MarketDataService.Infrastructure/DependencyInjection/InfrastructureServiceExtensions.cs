using MarketDataService.Application.Validators;
using MarketDataService.Core.Interfaces;
using MarketDataService.Infrastructure.Adapters;
using MarketDataService.Infrastructure.Consumers;
using MarketDataService.Infrastructure.Health;
using MarketDataService.Infrastructure.Options;
using MarketDataService.Infrastructure.Publishing;
using MarketDataService.Infrastructure.Registry;
using MassTransit;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using StackExchange.Redis;
using System.Text.Json.Serialization;

namespace MarketDataService.Infrastructure.DependencyInjection;

public static class InfrastructureServiceExtensions
{
    // Topic exchange the arbitrage platform publishes domain events to.
    private const string EventsExchange = "arbitrage.events";

    public static IServiceCollection AddInfrastructure(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        // Options binding.
        services.Configure<RedisOptions>(configuration.GetSection("Redis"));
        services.Configure<PublishingOptions>(configuration.GetSection("Publishing"));
        services.Configure<HealthOptions>(configuration.GetSection("Health"));
        services.Configure<ExchangesOptions>(options =>
            configuration.GetSection("Exchanges").Bind(options.Exchanges));

        // The tick validator (Application) reads its stale threshold from Health config.
        services.Configure<TickValidationOptions>(configuration.GetSection("Health"));

        // Redis connection — resilient startup so the host runs even if Redis is briefly down.
        services.AddSingleton<IConnectionMultiplexer>(sp =>
        {
            var redisOptions = sp.GetRequiredService<IOptions<RedisOptions>>().Value;
            var config = ConfigurationOptions.Parse(redisOptions.ConnectionString);
            config.AbortOnConnectFail = false;
            return ConnectionMultiplexer.Connect(config);
        });
        services.AddSingleton<IDatabase>(sp =>
            sp.GetRequiredService<IConnectionMultiplexer>().GetDatabase());

        // Publishing.
        services.AddSingleton<IThrottle, ClientThrottle>();
        services.AddSingleton<ITickPublisher, RedisTickPublisher>();

        // Adapters — transient: each market gets its own connection instance (per-symbol state).
        services.AddTransient<IExchangeAdapter, BinanceAdapter>();
        services.AddTransient<IExchangeAdapter, AsterAdapter>();

        // Registry — one singleton, exposed via the interface and its concrete type
        // (the health check needs the concrete handle accessor).
        services.AddSingleton<RedisSubscriptionRegistry>();
        services.AddSingleton<ISubscriptionRegistry>(sp =>
            sp.GetRequiredService<RedisSubscriptionRegistry>());

        // Health — registered for an out-of-band publisher (no HTTP endpoint in this host).
        services.AddSingleton<WebSocketHealthCheck>();
        services.AddHealthChecks().AddCheck<WebSocketHealthCheck>("websockets");

        AddMessaging(services, configuration);

        return services;
    }

    private static void AddMessaging(IServiceCollection services, IConfiguration configuration)
    {
        services.AddMassTransit(x =>
        {
            x.AddConsumer<JobCreatedConsumer>();
            x.AddConsumer<JobFinishedConsumer>();
            x.AddConsumer<SubscriptionCreatedConsumer>();
            x.AddConsumer<SubscriptionDeletedConsumer>();

            x.UsingRabbitMq((ctx, cfg) =>
            {
                cfg.Host(
                    configuration["RabbitMQ:Host"],
                    configuration["RabbitMQ:VirtualHost"] ?? "/",
                    h =>
                    {
                        h.Username(configuration["RabbitMQ:Username"] ?? "guest");
                        h.Password(configuration["RabbitMQ:Password"] ?? "guest");
                    });

                cfg.ConfigureJsonSerializerOptions(opts =>
                {
                    opts.Converters.Add(new JsonStringEnumConverter());
                    return opts;
                });

                // One queue per event type, bound to the shared topic exchange by routing key.
                // Faulted messages land in MassTransit's automatic "<queue>_error" dead-letter queue
                // after the immediate retries are exhausted.
                MapEndpoint<JobCreatedConsumer>(cfg, ctx, "market-data.job-created", "job.created");
                MapEndpoint<JobFinishedConsumer>(cfg, ctx, "market-data.job-finished", "job.finished");
                MapEndpoint<SubscriptionCreatedConsumer>(cfg, ctx, "market-data.subscription-created", "subscription.created");
                MapEndpoint<SubscriptionDeletedConsumer>(cfg, ctx, "market-data.subscription-deleted", "subscription.deleted");
            });
        });
    }

    private static void MapEndpoint<TConsumer>(
        IRabbitMqBusFactoryConfigurator cfg,
        IRegistrationContext ctx,
        string queueName,
        string routingKey)
        where TConsumer : class, IConsumer
    {
        cfg.ReceiveEndpoint(queueName, e =>
        {
            e.ConfigureConsumeTopology = false;
            e.Bind(EventsExchange, b =>
            {
                b.ExchangeType = ExchangeType.Topic;
                b.RoutingKey = routingKey;
            });
            e.UseMessageRetry(r => r.Immediate(3));
            e.ConfigureConsumer<TConsumer>(ctx);
        });
    }
}
