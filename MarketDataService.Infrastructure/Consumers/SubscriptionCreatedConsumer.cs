using MarketDataService.Application.Services;
using MarketDataService.Core.Events;
using MassTransit;
using Microsoft.Extensions.Logging;

namespace MarketDataService.Infrastructure.Consumers;

/// <summary>Starts monitoring both legs when a client subscription is created.</summary>
public class SubscriptionCreatedConsumer : IConsumer<SubscriptionCreatedEvent>
{
    private readonly MarketDataOrchestrator _orchestrator;
    private readonly ILogger<SubscriptionCreatedConsumer> _logger;

    public SubscriptionCreatedConsumer(MarketDataOrchestrator orchestrator, ILogger<SubscriptionCreatedConsumer> logger)
    {
        _orchestrator = orchestrator;
        _logger = logger;
    }

    public async Task Consume(ConsumeContext<SubscriptionCreatedEvent> context)
    {
        var e = context.Message;
        _logger.LogInformation("SubscriptionCreatedEvent received: {SubscriptionId} {Symbol}",
            e.SubscriptionId, e.Symbol);

        await _orchestrator.AddConsumersAsync(
            e.Symbol, e.BuyExchange, e.SellExchange, context.CancellationToken);
    }
}
