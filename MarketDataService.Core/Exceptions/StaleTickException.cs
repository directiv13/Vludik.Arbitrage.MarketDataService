using Vludik.Arbitrage.Events.Entities;

namespace MarketDataService.Core.Exceptions;

/// <summary>
/// Thrown when no fresh data has been received for a market within the allowed threshold.
/// </summary>
public sealed class StaleTickException : Exception
{
    public StaleTickException(string exchange, string symbol, ContractType contractType, TimeSpan staleness)
        : base($"No fresh tick from {exchange}:{symbol}:{contractType} for {staleness.TotalSeconds:F1}s.")
    {
        Exchange = exchange;
        Symbol = symbol;
        ContractType = contractType;
        Staleness = staleness;
    }

    public string Exchange { get; }
    public string Symbol { get; }
    public ContractType ContractType { get; }
    public TimeSpan Staleness { get; }
}
