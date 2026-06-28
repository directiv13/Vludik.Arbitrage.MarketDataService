using MarketDataService.Core.Exceptions;
using MarketDataService.Core.Interfaces;
using MarketDataService.Core.Models;
using MarketDataService.Infrastructure.Options;
using MarketDataService.Infrastructure.WebSocket;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Vludik.Arbitrage.Events.Entities;

namespace MarketDataService.Infrastructure.Adapters;

/// <summary>
/// Binance adapter. Supports perpetual futures and spot partial-book depth streams.
/// One instance owns one connection (resolved transiently per subscription).
/// </summary>
public sealed class BinanceAdapter : WebSocketBase, IExchangeAdapter
{
    private readonly ILogger<BinanceAdapter> _logger;

    public BinanceAdapter(IOptions<ExchangesOptions> options, ILogger<BinanceAdapter> logger)
        : base(logger, ResolvePolicy(options))
    {
        _logger = logger;
    }

    public string Exchange => "Binance";

    public IReadOnlyList<ContractType> SupportedContractTypes =>
        [ContractType.Perpetual, ContractType.Spot];

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
            $"wss://fstream.binance.com/stream?streams={_symbol.ToLowerInvariant()}@depth5@100ms",
        ContractType.Spot =>
            $"wss://stream.binance.com:9443/stream?streams={_symbol.ToLowerInvariant()}@depth5@100ms",
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
        => options.Value.Exchanges.TryGetValue("Binance", out var cfg) ? cfg.ReconnectPolicy : new ReconnectPolicy();
}
