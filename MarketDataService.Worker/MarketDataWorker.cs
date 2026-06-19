using MarketDataService.Application.Services;

namespace MarketDataService.Worker;

/// <summary>
/// Background host for the service. On startup it recovers WebSocket connections from the
/// Redis consumer counters that survived a previous run, then idles — all further work is
/// driven by RabbitMQ events handled by the MassTransit consumers.
/// </summary>
public sealed class MarketDataWorker : BackgroundService
{
    private readonly MarketDataOrchestrator _orchestrator;
    private readonly ILogger<MarketDataWorker> _logger;

    public MarketDataWorker(MarketDataOrchestrator orchestrator, ILogger<MarketDataWorker> logger)
    {
        _orchestrator = orchestrator;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Recover connections from Redis before anything else. A Redis outage at boot must not
        // crash the host — log and continue; events will rebuild state.
        try
        {
            await _orchestrator.RecoverConnectionsAsync(stoppingToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Startup recovery from Redis failed — continuing without recovered connections");
        }

        _logger.LogInformation("Market Data Service ready. Listening for events via RabbitMQ.");

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
    }
}
