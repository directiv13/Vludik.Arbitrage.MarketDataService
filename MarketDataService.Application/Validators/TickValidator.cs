using MarketDataService.Core.Models;
using Microsoft.Extensions.Options;

namespace MarketDataService.Application.Validators;

/// <summary>
/// Validates ticks before they are published: freshness and bid/ask sanity.
/// </summary>
public class TickValidator
{
    private readonly IOptions<TickValidationOptions> _options;

    public TickValidator(IOptions<TickValidationOptions> options)
    {
        _options = options;
    }

    private TimeSpan StaleThreshold => TimeSpan.FromSeconds(_options.Value.StaleThresholdSeconds);

    /// <summary>True if the tick arrived within the configured stale threshold.</summary>
    public bool IsFresh(PriceTick tick) => DateTime.UtcNow - tick.ReceivedAt < StaleThreshold;

    /// <summary>True if bid/ask are positive and the book is not crossed.</summary>
    public bool IsSane(PriceTick tick) =>
        tick.BestBid > 0 && tick.BestAsk > 0 && tick.BestAsk >= tick.BestBid;
}
