namespace MarketDataService.Core.Models;

/// <summary>
/// Uniquely identifies a monitored market: exchange + symbol + contract type.
/// Also resolves the Redis channels this subscription publishes to.
/// </summary>
public record SubscriptionKey(string Exchange, string Symbol, ContractType ContractType)
{
    /// <summary>Redis channel for Spread Job workers — full rate.</summary>
    public string WorkerChannel => $"tick:{Exchange}:{Symbol}:{ContractType}";

    /// <summary>Redis channel for clients — throttled, exchange-agnostic.</summary>
    public string ClientChannel => $"tick:client:{Symbol}:{ContractType}";

    public override string ToString() => $"{Exchange}:{Symbol}:{ContractType}";
}
