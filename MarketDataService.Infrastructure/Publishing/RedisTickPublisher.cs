using System.Text.Json;
using System.Text.Json.Serialization;
using MarketDataService.Core.Interfaces;
using MarketDataService.Core.Models;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace MarketDataService.Infrastructure.Publishing;

/// <summary>
/// Publishes ticks to Redis Pub/Sub. Always fire-and-forget — the tick hot path never
/// awaits Redis. Publish failures are logged and swallowed so a Redis hiccup never
/// crashes the service.
/// </summary>
public sealed class RedisTickPublisher : ITickPublisher
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly ISubscriber _subscriber;
    private readonly IThrottle _throttle;
    private readonly ILogger<RedisTickPublisher> _logger;

    public RedisTickPublisher(
        IConnectionMultiplexer redis,
        IThrottle throttle,
        ILogger<RedisTickPublisher> logger)
    {
        _subscriber = redis.GetSubscriber();
        _throttle = throttle;
        _logger = logger;
    }

    public Task PublishToWorkersAsync(PriceTick tick, CancellationToken ct)
    {
        var key = tick.ToSubscriptionKey();
        Publish(key.WorkerChannel, tick);
        return Task.CompletedTask;
    }

    public Task PublishToClientAsync(PriceTick tick, CancellationToken ct)
    {
        if (!_throttle.ShouldPublish(tick.Symbol, tick.ContractType))
            return Task.CompletedTask;

        var key = tick.ToSubscriptionKey();
        Publish(key.ClientChannel, tick);
        return Task.CompletedTask;
    }

    private void Publish(string channelName, PriceTick tick)
    {
        try
        {
            var payload = JsonSerializer.Serialize(tick, SerializerOptions);
            var channel = RedisChannel.Literal(channelName);
            _subscriber.Publish(channel, payload, CommandFlags.FireAndForget);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish tick to channel {Channel}", channelName);
        }
    }
}
