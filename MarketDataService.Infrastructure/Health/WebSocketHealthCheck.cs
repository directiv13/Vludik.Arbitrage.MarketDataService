using MarketDataService.Infrastructure.Options;
using MarketDataService.Infrastructure.Registry;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace MarketDataService.Infrastructure.Health;

/// <summary>
/// Reports adapter health by inspecting each active connection's last-tick time:
/// Healthy if fresh, Degraded if stale, Unhealthy if a connection has never produced
/// a tick. Returns the worst status across all active connections.
/// </summary>
public sealed class WebSocketHealthCheck : IHealthCheck
{
    private readonly RedisSubscriptionRegistry _registry;
    private readonly IOptions<HealthOptions> _options;

    public WebSocketHealthCheck(RedisSubscriptionRegistry registry, IOptions<HealthOptions> options)
    {
        _registry = registry;
        _options = options;
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var handles = _registry.GetActiveHandles();
        if (handles.Count == 0)
            return Task.FromResult(HealthCheckResult.Healthy("No active subscriptions."));

        var threshold = TimeSpan.FromSeconds(_options.Value.StaleThresholdSeconds);
        var now = DateTime.UtcNow;

        var worst = HealthStatus.Healthy;
        var details = new Dictionary<string, object>();

        foreach (var handle in handles)
        {
            var last = handle.LastTickReceivedAt;
            HealthStatus status;
            string note;

            if (last == default)
            {
                status = HealthStatus.Unhealthy;
                note = "no tick received yet";
            }
            else if (now - last > threshold)
            {
                status = HealthStatus.Degraded;
                note = $"stale {(now - last).TotalSeconds:F1}s";
            }
            else
            {
                status = HealthStatus.Healthy;
                note = $"fresh {(now - last).TotalSeconds:F1}s";
            }

            details[handle.Key.ToString()] = note;
            if (status < worst)
                worst = status;
        }

        var description = $"{handles.Count} active subscription(s).";
        return Task.FromResult(new HealthCheckResult(worst, description, data: details));
    }
}
