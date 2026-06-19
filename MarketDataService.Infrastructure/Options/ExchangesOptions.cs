namespace MarketDataService.Infrastructure.Options;

/// <summary>Per-exchange settings, bound from the <c>Exchanges</c> configuration section.</summary>
public class ExchangesOptions
{
    /// <summary>Map of exchange name → its settings.</summary>
    public Dictionary<string, ExchangeOptions> Exchanges { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}

public class ExchangeOptions
{
    public ReconnectPolicy ReconnectPolicy { get; set; } = new();
}
