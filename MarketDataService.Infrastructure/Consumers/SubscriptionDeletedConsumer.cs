using MarketDataService.Application.Services;
using MassTransit;
using Microsoft.Extensions.Logging;
using Vludik.Arbitrage.Events;

namespace MarketDataService.Infrastructure.Consumers;

/// <summary>Releases both legs when a client subscription is deleted.</summary>
public class SubscriptionDeletedConsumer : IConsumer<SubscriptionDeletedEvent>
{
    private readonly MarketDataOrchestrator _orchestrator;
    private readonly ILogger<SubscriptionDeletedConsumer> _logger;

    public SubscriptionDeletedConsumer(MarketDataOrchestrator orchestrator, ILogger<SubscriptionDeletedConsumer> logger)
    {
        _orchestrator = orchestrator;
        _logger = logger;
    }

    public async Task Consume(ConsumeContext<SubscriptionDeletedEvent> context)
    {
        var e = context.Message;
        _logger.LogInformation("SubscriptionDeletedEvent received: {SubscriptionId} {Symbol}",
            e.SubscriptionId, e.Symbol);

        await _orchestrator.RemoveConsumersAsync(
            e.Symbol, e.BuyExchange, e.SellExchange, context.CancellationToken);
    }
}
