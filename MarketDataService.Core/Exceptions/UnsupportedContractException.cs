using MarketDataService.Core.Models;

namespace MarketDataService.Core.Exceptions;

/// <summary>
/// Thrown when an exchange is asked to monitor a contract type it does not support.
/// </summary>
public sealed class UnsupportedContractException : Exception
{
    public UnsupportedContractException(string exchange, ContractType contractType)
        : base($"Exchange '{exchange}' does not support contract type '{contractType}'.")
    {
        Exchange = exchange;
        ContractType = contractType;
    }

    /// <summary>For raw (unparsed) contract-type strings coming off the wire, e.g. event payloads.</summary>
    public UnsupportedContractException(string exchange, string rawContractType)
        : base($"Exchange '{exchange}' does not support contract type '{rawContractType}'.")
    {
        Exchange = exchange;
        RawContractType = rawContractType;
    }

    public string Exchange { get; }
    public ContractType? ContractType { get; }
    public string? RawContractType { get; }
}
