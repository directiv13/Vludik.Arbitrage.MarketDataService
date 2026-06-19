using MarketDataService.Core.Models;

namespace MarketDataService.Core.Interfaces;

/// <summary>
/// Tracks consumer reference counts (in Redis) per market and owns the lifecycle of the
/// in-process WebSocket connections. A connection opens on the first consumer and closes
/// when the count returns to zero.
/// </summary>
public interface ISubscriptionRegistry
{
    /// <summary>Increments the Redis counter. Opens the WebSocket on the first consumer (0 → 1).</summary>
    Task AddConsumerAsync(SubscriptionKey key, CancellationToken ct);

    /// <summary>Decrements the Redis counter. Closes the WebSocket when the count reaches zero.</summary>
    Task RemoveConsumerAsync(SubscriptionKey key, CancellationToken ct);

    /// <summary>Current consumer count from Redis (0 if the key is absent).</summary>
    Task<int> GetConsumerCountAsync(SubscriptionKey key);

    /// <summary>All keys with a Redis count &gt; 0. Used for startup recovery.</summary>
    Task<IReadOnlyList<SubscriptionKey>> GetAllActiveKeysAsync();

    /// <summary>Keys with a live in-process WebSocket connection.</summary>
    IReadOnlyList<SubscriptionKey> GetConnectedKeys();

    /// <summary>
    /// Opens the WebSocket for a key whose counter is already non-zero, without touching the
    /// counter. Idempotent. Used by startup recovery to rebuild connections from Redis state.
    /// </summary>
    Task EnsureConnectedAsync(SubscriptionKey key, CancellationToken ct);
}
