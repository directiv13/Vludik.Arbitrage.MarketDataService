using System.Collections.Concurrent;
using MarketDataService.Core.Interfaces;
using MarketDataService.Infrastructure.Options;
using Microsoft.Extensions.Options;
using Vludik.Arbitrage.Events.Entities;

namespace MarketDataService.Infrastructure.Publishing;

/// <summary>
/// Per-symbol + contract-type rate limiter for the client channel. Lock-free.
/// </summary>
public sealed class ClientThrottle : IThrottle
{
    private readonly ConcurrentDictionary<string, DateTime> _lastPublished = new();
    private readonly TimeSpan _interval;

    public ClientThrottle(IOptions<PublishingOptions> options)
    {
        _interval = TimeSpan.FromMilliseconds(options.Value.ClientThrottleMs);
    }

    public bool ShouldPublish(string symbol, ContractType contractType)
    {
        var key = $"{symbol}:{contractType}";
        var now = DateTime.UtcNow;
        var publish = false;

        _lastPublished.AddOrUpdate(
            key,
            _ =>
            {
                publish = true;
                return now;
            },
            (_, previous) =>
            {
                if (now - previous >= _interval)
                {
                    publish = true;
                    return now;
                }

                return previous;
            });

        return publish;
    }
}
