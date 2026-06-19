namespace MarketDataService.Infrastructure.Options;

/// <summary>Redis settings, bound from the <c>Redis</c> configuration section.</summary>
public class RedisOptions
{
    /// <summary>StackExchange.Redis connection string, e.g. <c>redis:6379</c>.</summary>
    public string ConnectionString { get; set; } = "localhost:6379";
}
