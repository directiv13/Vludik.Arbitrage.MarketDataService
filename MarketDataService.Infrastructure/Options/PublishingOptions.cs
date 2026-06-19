namespace MarketDataService.Infrastructure.Options;

/// <summary>Publishing settings, bound from the <c>Publishing</c> configuration section.</summary>
public class PublishingOptions
{
    /// <summary>Minimum interval (ms) between client-channel publishes per symbol + contract type.</summary>
    public int ClientThrottleMs { get; set; } = 200;
}
