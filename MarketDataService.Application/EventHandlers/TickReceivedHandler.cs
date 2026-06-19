using MarketDataService.Application.Validators;
using MarketDataService.Core.Interfaces;
using MarketDataService.Core.Models;
using Microsoft.Extensions.Logging;

namespace MarketDataService.Application.EventHandlers;

/// <summary>
/// Invoked for every incoming tick. Validates freshness, then publishes to both the
/// worker (full-rate) and client (throttled) channels.
/// </summary>
public class TickReceivedHandler
{
    private readonly ITickPublisher _publisher;
    private readonly TickValidator _validator;
    private readonly ILogger<TickReceivedHandler> _logger;

    public TickReceivedHandler(
        ITickPublisher publisher,
        TickValidator validator,
        ILogger<TickReceivedHandler> logger)
    {
        _publisher = publisher;
        _validator = validator;
        _logger = logger;
    }

    public async Task HandleAsync(PriceTick tick, CancellationToken ct)
    {
        if (!_validator.IsFresh(tick))
        {
            _logger.LogDebug("Stale tick from {Exchange}:{Symbol}:{ContractType} — skipping",
                tick.Exchange, tick.Symbol, tick.ContractType);
            return;
        }

        if (!_validator.IsSane(tick))
        {
            _logger.LogWarning("Insane tick from {Exchange}:{Symbol}:{ContractType} (bid={BestBid}, ask={BestAsk}) — skipping",
                tick.Exchange, tick.Symbol, tick.ContractType, tick.BestBid, tick.BestAsk);
            return;
        }

        _logger.LogDebug("Tick {Exchange}:{Symbol}:{ContractType} bid={BestBid} ask={BestAsk}",
            tick.Exchange, tick.Symbol, tick.ContractType, tick.BestBid, tick.BestAsk);

        await _publisher.PublishToWorkersAsync(tick, ct);
        await _publisher.PublishToClientAsync(tick, ct);
    }
}
