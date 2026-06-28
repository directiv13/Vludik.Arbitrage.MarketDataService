using MarketDataService.Core.Exceptions;
using MarketDataService.Core.Interfaces;
using MarketDataService.Core.Models;
using Microsoft.Extensions.Logging;
using Vludik.Arbitrage.Events.Entities;

namespace MarketDataService.Application.Services;

/// <summary>
/// Thin coordinator between the event consumers and the subscription registry. Every event
/// carries two legs (buy + sell); both are added/removed independently so one failing leg
/// never blocks the other. Holds no WebSocket or Redis logic — that lives in the registry.
/// </summary>
public class MarketDataOrchestrator
{
    private readonly ISubscriptionRegistry _registry;
    private readonly ILogger<MarketDataOrchestrator> _logger;

    public MarketDataOrchestrator(
        ISubscriptionRegistry registry,
        ILogger<MarketDataOrchestrator> logger)
    {
        _registry = registry;
        _logger = logger;
    }

    /// <summary>Adds a consumer for both legs of an event. Called by Job/Subscription created consumers.</summary>
    public Task AddConsumersAsync(
        string symbol,
        ExchangeRef buyExchange,
        ExchangeRef sellExchange,
        CancellationToken ct)
        => Task.WhenAll(
            AddLegAsync(symbol, buyExchange, ct),
            AddLegAsync(symbol, sellExchange, ct));

    /// <summary>Removes a consumer for both legs of an event. Called by Job/Subscription finished/deleted consumers.</summary>
    public Task RemoveConsumersAsync(
        string symbol,
        ExchangeRef buyExchange,
        ExchangeRef sellExchange,
        CancellationToken ct)
        => Task.WhenAll(
            RemoveLegAsync(symbol, buyExchange, ct),
            RemoveLegAsync(symbol, sellExchange, ct));

    /// <summary>On startup, reopen WebSockets for every market with a surviving non-zero Redis counter.</summary>
    public async Task RecoverConnectionsAsync(CancellationToken ct)
    {
        var activeKeys = await _registry.GetAllActiveKeysAsync();

        _logger.LogInformation("Recovering {Count} WebSocket connection(s) from Redis", activeKeys.Count);

        foreach (var key in activeKeys)
        {
            try
            {
                await _registry.EnsureConnectedAsync(key, ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to recover connection for {Key}", key);
            }
        }
    }

    private async Task AddLegAsync(string symbol, ExchangeRef exchange, CancellationToken ct)
    {
        if (!TryBuildKey(symbol, exchange, out var key))
            return;

        await _registry.AddConsumerAsync(key, ct);
    }

    private async Task RemoveLegAsync(string symbol, ExchangeRef exchange, CancellationToken ct)
    {
        if (!TryBuildKey(symbol, exchange, out var key))
            return;

        await _registry.RemoveConsumerAsync(key, ct);
    }

    private bool TryBuildKey(string symbol, ExchangeRef exchange, out SubscriptionKey key)
    {
        try
        {
            key = new SubscriptionKey(exchange.Name, symbol, exchange.Type);
            return true;
        }
        catch (UnsupportedContractException ex)
        {
            _logger.LogWarning(ex, "Skipping leg {Exchange} {Symbol} {Type} — unsupported contract type",
                exchange.Name, symbol, exchange.Type);
            key = null!;
            return false;
        }
    }
}
