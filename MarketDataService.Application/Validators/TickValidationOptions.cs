namespace MarketDataService.Application.Validators;

/// <summary>
/// Freshness threshold for tick validation. Bound from <c>Health:StaleThresholdSeconds</c>.
/// </summary>
public class TickValidationOptions
{
    /// <summary>Maximum tick age, in seconds, before it is considered stale.</summary>
    public double StaleThresholdSeconds { get; set; } = 5;
}
