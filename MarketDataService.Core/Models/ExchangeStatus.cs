namespace MarketDataService.Core.Models;

/// <summary>
/// Lifecycle state of a single exchange connection.
/// </summary>
public enum ExchangeStatus
{
    /// <summary>Connected and receiving data.</summary>
    Connected,

    /// <summary>Connection dropped; reconnect in progress.</summary>
    Reconnecting,

    /// <summary>Max reconnect attempts exceeded; the connection is stopped.</summary>
    Dead
}
