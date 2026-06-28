using MarketDataService.Application.Services;
using MassTransit;
using Microsoft.Extensions.Logging;
using Vludik.Arbitrage.Events;

namespace MarketDataService.Infrastructure.Consumers;

/// <summary>Releases both legs when a spread job finishes.</summary>
public class JobFinishedConsumer : IConsumer<JobFinishedEvent>
{
    private readonly MarketDataOrchestrator _orchestrator;
    private readonly ILogger<JobFinishedConsumer> _logger;

    public JobFinishedConsumer(MarketDataOrchestrator orchestrator, ILogger<JobFinishedConsumer> logger)
    {
        _orchestrator = orchestrator;
        _logger = logger;
    }

    public async Task Consume(ConsumeContext<JobFinishedEvent> context)
    {
        var e = context.Message;
        _logger.LogInformation("JobFinishedEvent received: {JobId} {Symbol}", e.JobId, e.Symbol);

        await _orchestrator.RemoveConsumersAsync(
            e.Symbol, e.BuyExchange, e.SellExchange, context.CancellationToken);
    }
}
