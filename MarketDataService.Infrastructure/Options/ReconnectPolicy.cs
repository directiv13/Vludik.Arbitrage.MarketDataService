namespace MarketDataService.Infrastructure.Options;

/// <summary>
/// Exponential-backoff reconnect policy for a WebSocket connection.
/// </summary>
public class ReconnectPolicy
{
    /// <summary>Maximum reconnect attempts before the adapter is declared fatally dead.</summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>Base delay (seconds) for the first reconnect attempt.</summary>
    public int InitialDelaySeconds { get; set; } = 1;

    /// <summary>Upper bound (seconds) on the backoff delay.</summary>
    public int MaxDelaySeconds { get; set; } = 30;
}
