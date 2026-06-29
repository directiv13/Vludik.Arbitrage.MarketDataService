using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using MarketDataService.Core.Models;
using Vludik.Arbitrage.Shared.Enums;

namespace MarketDataService.Infrastructure.Adapters;

/// <summary>
/// Combined-stream depth envelope: <c>{ "stream": "...", "data": { ... } }</c>.
/// Both Binance and Aster (Binance-derived) use this shape.
/// </summary>
internal sealed class DepthStreamEnvelope
{
    [JsonPropertyName("stream")] public string? Stream { get; set; }
    [JsonPropertyName("data")] public DepthData? Data { get; set; }
}

/// <summary>
/// Partial-book depth payload. Futures streams use <c>b</c>/<c>a</c>; spot partial-depth
/// streams use <c>bids</c>/<c>asks</c>. Both are accepted for robustness.
/// </summary>
internal sealed class DepthData
{
    [JsonPropertyName("b")] public List<List<string>>? B { get; set; }
    [JsonPropertyName("a")] public List<List<string>>? A { get; set; }
    [JsonPropertyName("bids")] public List<List<string>>? Bids { get; set; }
    [JsonPropertyName("asks")] public List<List<string>>? Asks { get; set; }
    [JsonPropertyName("T")] public long? Timestamp { get; set; }

    public List<List<string>>? BidLevels => B ?? Bids;
    public List<List<string>>? AskLevels => A ?? Asks;
}

/// <summary>Parses a raw combined-stream depth message into a <see cref="PriceTick"/>.</summary>
internal static class DepthTickParser
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true
    };

    /// <summary>
    /// Attempts to parse <paramref name="json"/> into a tick. Returns <c>false</c> for
    /// malformed payloads or messages without a usable top-of-book (e.g. control frames).
    /// </summary>
    public static bool TryParse(
        string json,
        string exchange,
        string symbol,
        ContractType contractType,
        out PriceTick? tick)
    {
        tick = null;

        var envelope = JsonSerializer.Deserialize<DepthStreamEnvelope>(json, Options);
        var data = envelope?.Data;
        if (data is null)
            return false;

        var bid = FirstPrice(data.BidLevels);
        var ask = FirstPrice(data.AskLevels);
        if (bid is null || ask is null)
            return false;

        tick = new PriceTick
        {
            Exchange = exchange,
            Symbol = symbol,
            ContractType = contractType,
            BestBid = bid.Value,
            BestAsk = ask.Value,
            Timestamp = data.Timestamp ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };
        return true;
    }

    private static decimal? FirstPrice(List<List<string>>? levels)
    {
        if (levels is null || levels.Count == 0)
            return null;

        var top = levels[0];
        if (top is null || top.Count == 0)
            return null;

        return decimal.TryParse(top[0], NumberStyles.Any, CultureInfo.InvariantCulture, out var price)
            ? price
            : null;
    }
}
