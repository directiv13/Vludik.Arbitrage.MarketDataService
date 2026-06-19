namespace MarketDataService.Core.Events;

/// <summary>Incoming event: a spread job started; begin monitoring both legs.</summary>
public record JobCreatedEvent
{
    public Guid JobId { get; init; }
    public string Symbol { get; init; } = string.Empty;
    public ExchangeInfo BuyExchange { get; init; } = null!;
    public ExchangeInfo SellExchange { get; init; } = null!;
    public long Timestamp { get; init; }
}
