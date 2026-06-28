using Vludik.Arbitrage.Events.Entities;

namespace MarketDataService.Core.Exceptions;

/// <summary>
/// Thrown / signalled when an adapter exhausts its reconnect attempts and gives up.
/// </summary>
public sealed class AdapterFatalException : Exception
{
    public AdapterFatalException(string exchange, string symbol, ContractType contractType, int attempts, Exception? inner = null)
        : base($"Adapter '{exchange}' for {symbol}:{contractType} failed permanently after {attempts} reconnect attempts.", inner)
    {
        Exchange = exchange;
        Symbol = symbol;
        ContractType = contractType;
        Attempts = attempts;
    }

    public string Exchange { get; }
    public string Symbol { get; }
    public ContractType ContractType { get; }
    public int Attempts { get; }
}
