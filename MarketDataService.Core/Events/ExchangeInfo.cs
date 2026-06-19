using MarketDataService.Core.Exceptions;
using MarketDataService.Core.Models;

namespace MarketDataService.Core.Events;

/// <summary>
/// One leg of an arbitrage event: an exchange name plus its market type as a raw string.
/// </summary>
public record ExchangeInfo(
    string Name,   // "Binance", "Aster"
    string Type)   // "spot" or "perpetual"
{
    /// <summary>Maps the wire-format <see cref="Type"/> string to the <see cref="ContractType"/> enum.</summary>
    public ContractType ToContractType() => Type.ToLowerInvariant() switch
    {
        "spot" => ContractType.Spot,
        "perpetual" => ContractType.Perpetual,
        _ => throw new UnsupportedContractException(Name, Type)
    };
}
