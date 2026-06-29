using MarketDataService.Core.Exceptions;
using MarketDataService.Core.Interfaces;
using MarketDataService.Core.Models;
using MarketDataService.Infrastructure.Options;
using MarketDataService.Infrastructure.WebSocket;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Vludik.Arbitrage.Shared.Enums;

namespace MarketDataService.Infrastructure.Adapters;

/// <summary>
/// Aster adapter. Supports perpetual futures only (Binance-compatible depth format).
/// One instance owns one connection (resolved transiently per subscription).
/// </summary>
public sealed class AsterAdapter : WebSocketBase, IExchangeAdapter
{
    private readonly ILogger<AsterAdapter> _logger;

    public AsterAdapter(IOptions<ExchangesOptions> options, ILogger<AsterAdapter> logger)
        : base(logger, ResolvePolicy(options))
    {
        _logger = logger;
    }

    public string Exchange => "Aster";

    public IReadOnlyList<ContractType> SupportedContractTypes =>
        [ContractType.Perpetual];

    protected override string ExchangeName => Exchange;

    public Task<ExchangeSubscription> SubscribeAsync(
        string symbol,
        ContractType contractType,
        Action<PriceTick> onTick,
        CancellationToken ct)
    {
        if (!SupportedContractTypes.Contains(contractType))
            throw new UnsupportedContractException(Exchange, contractType);

        Configure(symbol, contractType, onTick);

        var key = new SubscriptionKey(Exchange, symbol, contractType);
        var subscription = new ExchangeSubscription(key, StartAsync, () => LastTickReceivedAt, ct);
        return Task.FromResult(subscription);
    }

    protected override string GetStreamUrl() => _contractType switch
    {
        ContractType.Perpetual =>
            $"wss://fstream.asterdex.com/stream?streams={_symbol.ToLowerInvariant()}@depth5",
        _ => throw new UnsupportedContractException(Exchange, _contractType)
    };

    protected override string? GetSubscriptionMessage() => null;

    protected override void OnMessageReceived(string message)
    {
        try
        {
            if (DepthTickParser.TryParse(message, Exchange, _symbol, _contractType, out var tick) && tick is not null)
                _onTick(tick);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Malformed message from {Exchange} {Symbol} {ContractType}: {Payload}",
                Exchange, _symbol, _contractType, message);
        }
    }

    private static ReconnectPolicy ResolvePolicy(IOptions<ExchangesOptions> options)
        => options.Value.Exchanges.TryGetValue("Aster", out var cfg) ? cfg.ReconnectPolicy : new ReconnectPolicy();
}
