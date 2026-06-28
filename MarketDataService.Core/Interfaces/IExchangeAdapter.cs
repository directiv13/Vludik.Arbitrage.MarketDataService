using MarketDataService.Core.Models;
using Vludik.Arbitrage.Events.Entities;

namespace MarketDataService.Core.Interfaces;

/// <summary>
/// Contract every exchange adapter implements. An adapter knows how to open one
/// WebSocket connection for a given symbol/contract type and stream normalized ticks.
/// </summary>
public interface IExchangeAdapter
{
    /// <summary>Exchange name, e.g. "Binance", "Aster".</summary>
    string Exchange { get; }

    /// <summary>Contract types this exchange supports.</summary>
    IReadOnlyList<ContractType> SupportedContractTypes { get; }

    /// <summary>
    /// Configures a connection for the given market and returns a (not-yet-started)
    /// subscription handle. Call <see cref="ExchangeSubscription.Start"/> to begin streaming.
    /// </summary>
    Task<ExchangeSubscription> SubscribeAsync(
        string symbol,
        ContractType contractType,
        Action<PriceTick> onTick,
        CancellationToken ct);
}
