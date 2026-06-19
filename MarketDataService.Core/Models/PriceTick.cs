namespace MarketDataService.Core.Models;

/// <summary>
/// A normalized top-of-book price tick produced by an exchange adapter.
/// </summary>
public record PriceTick
{
    /// <summary>Exchange that produced the tick, e.g. "Binance", "Aster".</summary>
    public required string Exchange { get; init; }

    /// <summary>Trading symbol, e.g. "BTCUSDT".</summary>
    public required string Symbol { get; init; }

    /// <summary>Market the tick belongs to.</summary>
    public required ContractType ContractType { get; init; }

    /// <summary>Best bid price.</summary>
    public decimal BestBid { get; init; }

    /// <summary>Best ask price.</summary>
    public decimal BestAsk { get; init; }

    /// <summary>UTC timestamp set when the tick arrived at this service.</summary>
    public DateTime ReceivedAt { get; init; }

    /// <summary>Builds the <see cref="SubscriptionKey"/> this tick belongs to.</summary>
    public SubscriptionKey ToSubscriptionKey() => new(Exchange, Symbol, ContractType);
}
