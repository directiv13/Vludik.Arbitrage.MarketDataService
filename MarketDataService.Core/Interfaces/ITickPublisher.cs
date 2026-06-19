using MarketDataService.Core.Models;

namespace MarketDataService.Core.Interfaces;

/// <summary>
/// Publishes ticks to downstream consumers over Redis Pub/Sub.
/// </summary>
public interface ITickPublisher
{
    /// <summary>
    /// Publishes to the worker channel at full rate
    /// (<c>tick:{Exchange}:{Symbol}:{ContractType}</c>).
    /// </summary>
    Task PublishToWorkersAsync(PriceTick tick, CancellationToken ct);

    /// <summary>
    /// Publishes to the client channel (<c>tick:client:{Symbol}:{ContractType}</c>),
    /// throttled per symbol + contract type.
    /// </summary>
    Task PublishToClientAsync(PriceTick tick, CancellationToken ct);
}
