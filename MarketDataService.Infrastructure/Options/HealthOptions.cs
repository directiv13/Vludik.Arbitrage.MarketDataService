namespace MarketDataService.Infrastructure.Options;

/// <summary>Health-check settings, bound from the <c>Health</c> configuration section.</summary>
public class HealthOptions
{
    /// <summary>Seconds without a tick before a connection is reported Degraded.</summary>
    public double StaleThresholdSeconds { get; set; } = 5;
}
