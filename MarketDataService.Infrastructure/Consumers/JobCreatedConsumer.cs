using MarketDataService.Application.Services;
using MassTransit;
using Microsoft.Extensions.Logging;
using Vludik.Arbitrage.Events;

namespace MarketDataService.Infrastructure.Consumers;

/// <summary>Starts monitoring both legs when a spread job is created.</summary>
public class JobCreatedConsumer : IConsumer<JobCreatedEvent>
{
    private readonly MarketDataOrchestrator _orchestrator;
    private readonly ILogger<JobCreatedConsumer> _logger;

    public JobCreatedConsumer(MarketDataOrchestrator orchestrator, ILogger<JobCreatedConsumer> logger)
    {
        _orchestrator = orchestrator;
        _logger = logger;
    }

    public async Task Consume(ConsumeContext<JobCreatedEvent> context)
    {
        var e = context.Message;
        _logger.LogInformation("JobCreatedEvent received: {JobId} {Symbol}", e.JobId, e.Symbol);

        await _orchestrator.AddConsumersAsync(
            e.Symbol, e.BuyExchange, e.SellExchange, context.CancellationToken);
    }
}
