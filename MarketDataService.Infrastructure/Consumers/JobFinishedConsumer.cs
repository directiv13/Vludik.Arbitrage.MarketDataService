using MarketDataService.Application.Services;
using MassTransit;
using Microsoft.Extensions.Logging;
using Vludik.Arbitrage.JobsService.Shared.Events;

namespace MarketDataService.Infrastructure.Consumers;

/// <summary>Releases both legs when a spread job finishes.</summary>
public class JobFinishedConsumer : IConsumer<JobDeletedEvent>
{
    private readonly MarketDataOrchestrator _orchestrator;
    private readonly ILogger<JobFinishedConsumer> _logger;

    public JobFinishedConsumer(MarketDataOrchestrator orchestrator, ILogger<JobFinishedConsumer> logger)
    {
        _orchestrator = orchestrator;
        _logger = logger;
    }

    public async Task Consume(ConsumeContext<JobDeletedEvent> context)
    {
        var e = context.Message;
        _logger.LogInformation("JobDeletedEvent received: {JobId} {Symbol}", e.JobId, e.Symbol);

        await _orchestrator.RemoveConsumersAsync(
            e.Symbol, e.BuyExchange, e.SellExchange, context.CancellationToken);
    }
}
