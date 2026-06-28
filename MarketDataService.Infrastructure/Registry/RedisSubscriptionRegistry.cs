using System.Collections.Concurrent;
using MarketDataService.Application.EventHandlers;
using MarketDataService.Core.Interfaces;
using MarketDataService.Core.Models;
using MarketDataService.Infrastructure.WebSocket;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Vludik.Arbitrage.Events.Entities;

namespace MarketDataService.Infrastructure.Registry;

/// <summary>
/// Redis-backed consumer reference counter plus owner of the in-process WebSocket connections.
/// Counts are atomic (<c>INCR</c>/<c>DECR</c>) and shared across restarts; connections are local
/// to this process. A connection opens on the first consumer (count 0 → 1) and closes when the
/// count returns to zero.
/// </summary>
public sealed class RedisSubscriptionRegistry : ISubscriptionRegistry, IAsyncDisposable
{
    private readonly IDatabase _db;
    private readonly IConnectionMultiplexer _redis;
    private readonly IServiceProvider _serviceProvider;
    private readonly TickReceivedHandler _tickHandler;
    private readonly ILogger<RedisSubscriptionRegistry> _logger;

    // In-process map of live connections — separate concern from the shared Redis counter.
    private readonly ConcurrentDictionary<SubscriptionKey, ExchangeSubscription> _connections = new();

    // Connections live for the lifetime of the registry, NOT the per-message cancellation token.
    private readonly CancellationTokenSource _shutdownCts = new();

    public RedisSubscriptionRegistry(
        IConnectionMultiplexer redis,
        IServiceProvider serviceProvider,
        TickReceivedHandler tickHandler,
        ILogger<RedisSubscriptionRegistry> logger)
    {
        _redis = redis;
        _db = redis.GetDatabase();
        _serviceProvider = serviceProvider;
        _tickHandler = tickHandler;
        _logger = logger;
    }

    // Redis key format: consumers:{Exchange}:{Symbol}:{ContractType}
    private static string CounterKey(SubscriptionKey key) =>
        $"consumers:{key.Exchange}:{key.Symbol}:{key.ContractType}";

    public async Task AddConsumerAsync(SubscriptionKey key, CancellationToken ct)
    {
        // INCR is atomic and returns the new value — use it directly to detect the first consumer.
        var count = await _db.StringIncrementAsync(CounterKey(key)).ConfigureAwait(false);
        _logger.LogInformation("Consumer added for {Key}. Redis count: {Count}", key, count);

        if (count == 1)
            await EnsureConnectedAsync(key, _shutdownCts.Token).ConfigureAwait(false);
    }

    public async Task RemoveConsumerAsync(SubscriptionKey key, CancellationToken ct)
    {
        var count = await _db.StringDecrementAsync(CounterKey(key)).ConfigureAwait(false);

        // Guard against negative counts (e.g. a duplicate delete event).
        if (count < 0)
        {
            await _db.StringSetAsync(CounterKey(key), 0).ConfigureAwait(false);
            _logger.LogWarning("Consumer count for {Key} went negative ({Count}) — reset to 0", key, count);
            count = 0;
        }

        _logger.LogInformation("Consumer removed for {Key}. Redis count: {Count}", key, count);

        if (count <= 0 && _connections.TryRemove(key, out var subscription))
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
            _logger.LogInformation("WebSocket closed for {Key} — no remaining consumers", key);
        }
    }

    public async Task<int> GetConsumerCountAsync(SubscriptionKey key)
    {
        var value = await _db.StringGetAsync(CounterKey(key)).ConfigureAwait(false);
        return value.HasValue && value.TryParse(out long count) ? (int)count : 0;
    }

    public async Task<IReadOnlyList<SubscriptionKey>> GetAllActiveKeysAsync()
    {
        var result = new List<SubscriptionKey>();

        foreach (var endpoint in _redis.GetEndPoints())
        {
            var server = _redis.GetServer(endpoint);
            if (!server.IsConnected || server.IsReplica)
                continue;

            await foreach (var redisKey in server.KeysAsync(pattern: "consumers:*").ConfigureAwait(false))
            {
                var value = await _db.StringGetAsync(redisKey).ConfigureAwait(false);
                if (!value.HasValue || !value.TryParse(out long count) || count <= 0)
                    continue;

                // Parse "consumers:{Exchange}:{Symbol}:{ContractType}".
                var parts = ((string)redisKey!).Split(':');
                if (parts.Length != 4)
                    continue;

                if (Enum.TryParse<ContractType>(parts[3], ignoreCase: true, out var contractType))
                    result.Add(new SubscriptionKey(parts[1], parts[2], contractType));
            }
        }

        return result;
    }

    public IReadOnlyList<SubscriptionKey> GetConnectedKeys() => _connections.Keys.ToList();

    public async Task EnsureConnectedAsync(SubscriptionKey key, CancellationToken ct)
    {
        if (_connections.ContainsKey(key))
            return; // Already streaming.

        var adapter = _serviceProvider.GetServices<IExchangeAdapter>()
            .SingleOrDefault(a => string.Equals(a.Exchange, key.Exchange, StringComparison.OrdinalIgnoreCase));

        if (adapter is null)
        {
            _logger.LogError("No adapter found for exchange {Exchange}", key.Exchange);
            return;
        }

        if (adapter is WebSocketBase connection)
            connection.OnFatalError += _ => OnAdapterFatalAsync(key);

        // Connections run under the registry-lifetime token, never the caller's per-message token.
        var subscription = await adapter
            .SubscribeAsync(key.Symbol, key.ContractType, DispatchTick, _shutdownCts.Token)
            .ConfigureAwait(false);

        if (!_connections.TryAdd(key, subscription))
        {
            // Lost a race with a concurrent open — the other call owns the connection.
            await subscription.DisposeAsync().ConfigureAwait(false);
            return;
        }

        subscription.Start();
        _logger.LogInformation("WebSocket opened for {Key}", key);
    }

    /// <summary>Snapshot of live connection handles, used by the health check.</summary>
    public IReadOnlyList<ExchangeSubscription> GetActiveHandles() => _connections.Values.ToList();

    private void DispatchTick(PriceTick tick)
    {
        try
        {
            var task = _tickHandler.HandleAsync(tick, CancellationToken.None);
            if (!task.IsCompletedSuccessfully)
            {
                _ = task.ContinueWith(
                    t => _logger.LogError(t.Exception, "Tick handling failed for {Exchange} {Symbol} {ContractType}",
                        tick.Exchange, tick.Symbol, tick.ContractType),
                    TaskContinuationOptions.OnlyOnFaulted);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Tick handling failed for {Exchange} {Symbol} {ContractType}",
                tick.Exchange, tick.Symbol, tick.ContractType);
        }
    }

    private Task OnAdapterFatalAsync(SubscriptionKey key)
    {
        _logger.LogCritical("Adapter for {Key} died — removing connection (consumer count left intact)", key);

        // Dispose off the loop thread: we are invoked from inside the adapter's StartAsync,
        // and DisposeAsync awaits that very task — awaiting it here would deadlock.
        if (_connections.TryRemove(key, out var subscription))
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await subscription.DisposeAsync().ConfigureAwait(false);
                }
                catch
                {
                    // Best-effort teardown of an already-dead adapter.
                }
            });
        }

        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await _shutdownCts.CancelAsync().ConfigureAwait(false);
        }
        catch
        {
            // Best-effort cancellation during shutdown.
        }

        foreach (var key in _connections.Keys.ToList())
        {
            if (_connections.TryRemove(key, out var subscription))
                await subscription.DisposeAsync().ConfigureAwait(false);
        }

        _shutdownCts.Dispose();
    }
}
