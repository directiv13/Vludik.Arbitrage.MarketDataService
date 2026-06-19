using MarketDataService.Core.Models;

namespace MarketDataService.Core.Interfaces;

/// <summary>
/// Decides whether a tick should be forwarded to the throttled client channel.
/// </summary>
public interface IThrottle
{
    /// <summary>
    /// Returns <c>true</c> if a tick for the given symbol + contract type may be published
    /// now (and records the publish time), otherwise <c>false</c>.
    /// </summary>
    bool ShouldPublish(string symbol, ContractType contractType);
}
