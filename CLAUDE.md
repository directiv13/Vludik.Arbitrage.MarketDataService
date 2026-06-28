# CLAUDE.md — Market Data Service

## Role
You are a Senior .NET Developer working on the **Market Data Service** — a standalone .NET 9 Worker Service responsible for connecting to cryptocurrency exchange WebSockets on demand, normalizing price ticks, and publishing them to Redis Pub/Sub for downstream consumers.

Subscriptions are **dynamic and event-driven** — the Jobs and Subscription services publish RabbitMQ events (job/subscription created/finished) that this service consumes via **MassTransit**. Consumer reference counts are tracked in **Redis** (atomic `INCR`/`DECR`); a WebSocket opens on the first consumer for a market and closes when the count returns to zero. The service starts with zero active connections and builds them up on demand. On boot it **recovers** connections from any Redis counters that survived a previous run.

This service has **no REST API**, **no database**, and **no business logic**. It is a pure background worker (no HTTP host).

---

## Architecture Position

```
Jobs Service / Subscription Service
    │
    │  publish events to topic exchange "arbitrage.events":
    │    job.created / job.finished
    │    subscription.created / subscription.deleted
    ▼
RabbitMQ  ──(MassTransit consumers)──►  Market Data Service  ◄─── this service
                                            │  INCR/DECR consumers:{exchange}:{symbol}:{contractType}  (Redis)
                                            │  connects 1 WebSocket per market on first consumer (count 0→1)
                                            │  closes it when count returns to 0
                                            │
                                            └── PUBLISH "tick:{exchange}:{symbol}:{contractType}"  every tick (~100ms)
```

Each event carries **two** legs (a buy exchange and a sell exchange); both are reference-counted independently.

---

## Solution Structure

```
MarketDataService/
├── MarketDataService.sln
├── MarketDataService.Core/
├── MarketDataService.Application/
├── MarketDataService.Infrastructure/
└── MarketDataService.Worker/            # Entry point — pure background Host (no HTTP)
```

### Dependency Rule

```
Worker → Infrastructure → Application → Core
                        ↗
           Infrastructure
```

Nothing points outward from Core. Core knows nothing about Redis, WebSockets, or .NET hosting.

---

## Project: MarketDataService.Core
**Dependencies:** None (no NuGet packages, no framework references beyond primitives)

```
Core/
├── Models/
│   ├── PriceTick.cs               # Normalized tick record (+ ToSubscriptionKey())
│   ├── SubscriptionKey.cs         # Exchange + Symbol + ContractType value object
│   ├── ContractType.cs            # Enum: Perpetual, Spot
│   ├── ExchangeStatus.cs          # Enum: Connected, Reconnecting, Dead
│   └── ExchangeSubscription.cs    # IAsyncDisposable handle to one live connection
│
├── Events/                        # Incoming RabbitMQ event contracts (consumed only)
│   ├── ExchangeInfo.cs            # { Name, Type } + ToContractType()
│   ├── JobCreatedEvent.cs
│   ├── JobFinishedEvent.cs
│   ├── SubscriptionCreatedEvent.cs
│   └── SubscriptionDeletedEvent.cs
│
├── Interfaces/
│   ├── IExchangeAdapter.cs        # Contract all exchange adapters implement
│   ├── ITickPublisher.cs          # Publish ticks to consumers (worker + client channels)
│   ├── ISubscriptionRegistry.cs   # Redis reference counting + connection lifecycle
│   └── IThrottle.cs               # Decide whether to forward tick to client
│
└── Exceptions/
    ├── AdapterFatalException.cs       # Max reconnects exceeded
    ├── UnsupportedContractException.cs # Exchange doesn't support requested contract type
    └── StaleTickException.cs          # No data received within threshold
```

> **Note:** `ExchangeSubscription` lives in Core (BCL-only handle) so `IExchangeAdapter`
> can return it without Core taking a dependency on Infrastructure.

### Core Models

```csharp
// Core/Models/ContractType.cs
public enum ContractType
{
    Perpetual,  // Futures perpetual swap
    Spot        // Spot market
}

// Core/Models/SubscriptionKey.cs
public record SubscriptionKey(string Exchange, string Symbol, ContractType ContractType)
{
    // Redis channel for Spread Job workers — full rate
    public string WorkerChannel => $"tick:{Exchange}:{Symbol}:{ContractType}";

    // Redis channel for client — throttled
    public string ClientChannel => $"tick:client:{Symbol}:{ContractType}";

    public override string ToString() => $"{Exchange}:{Symbol}:{ContractType}";
}

// Core/Models/PriceTick.cs
public record PriceTick
{
    public required string Exchange      { get; init; }   // "Binance", "Aster"
    public required string Symbol        { get; init; }   // "BTCUSDT"
    public required ContractType ContractType { get; init; }
    public decimal BestBid               { get; init; }
    public decimal BestAsk               { get; init; }
    public DateTime ReceivedAt           { get; init; }   // UTC, set on arrival
}
```

### Core Interfaces

```csharp
// Core/Interfaces/IExchangeAdapter.cs
public interface IExchangeAdapter
{
    string Exchange { get; }
    IReadOnlyList<ContractType> SupportedContractTypes { get; }

    Task<ExchangeSubscription> SubscribeAsync(
        string symbol,
        ContractType contractType,
        Action<PriceTick> onTick,
        CancellationToken ct);
}

// Core/Interfaces/ITickPublisher.cs
public interface ITickPublisher
{
    Task PublishToWorkersAsync(PriceTick tick, CancellationToken ct); // full rate
    Task PublishToClientAsync(PriceTick tick, CancellationToken ct);  // throttled
}

// Core/Interfaces/ISubscriptionRegistry.cs
// Redis reference counting + owner of the in-process WebSocket connections.
public interface ISubscriptionRegistry
{
    // INCR Redis counter; opens the WebSocket on the first consumer (0 → 1).
    Task AddConsumerAsync(SubscriptionKey key, CancellationToken ct);

    // DECR Redis counter; closes the WebSocket when the count reaches zero.
    Task RemoveConsumerAsync(SubscriptionKey key, CancellationToken ct);

    Task<int> GetConsumerCountAsync(SubscriptionKey key);

    // Keys with a Redis count > 0 — used for startup recovery.
    Task<IReadOnlyList<SubscriptionKey>> GetAllActiveKeysAsync();

    // Keys with a live in-process WebSocket connection.
    IReadOnlyList<SubscriptionKey> GetConnectedKeys();

    // Open a connection for an already-counted key without incrementing (recovery). Idempotent.
    Task EnsureConnectedAsync(SubscriptionKey key, CancellationToken ct);
}
```

### Core Events

```csharp
// Core/Events/ExchangeInfo.cs — one leg of an event
public record ExchangeInfo(string Name, string Type)   // Type: "spot" | "perpetual"
{
    public ContractType ToContractType() => Type.ToLowerInvariant() switch
    {
        "spot"      => ContractType.Spot,
        "perpetual" => ContractType.Perpetual,
        _ => throw new UnsupportedContractException(Name, Type)
    };
}

// Core/Events/{JobCreated,JobFinished,SubscriptionCreated,SubscriptionDeleted}Event.cs
// All share the same shape — incoming only; this service never publishes events.
public record JobCreatedEvent
{
    public Guid JobId          { get; init; }   // SubscriptionId for the Subscription events
    public string Symbol       { get; init; } = string.Empty;
    public ExchangeInfo BuyExchange  { get; init; } = null!;
    public ExchangeInfo SellExchange { get; init; } = null!;
    public long Timestamp      { get; init; }
}
```

---

## Project: MarketDataService.Application
**Dependencies:** Core only

```
Application/
├── Services/
│   └── MarketDataOrchestrator.cs        # Thin coordinator: maps events → registry consumer counts
│
├── EventHandlers/
│   └── TickReceivedHandler.cs           # On tick → validate freshness → publish to both channels
│
├── Validators/
│   ├── TickValidator.cs                  # Is tick fresh? Are bid/ask sane?
│   └── TickValidationOptions.cs          # StaleThresholdSeconds (bound from Health config)
│
└── DependencyInjection/
    └── ApplicationServiceExtensions.cs   # AddApplication(this IServiceCollection)
```

> The MassTransit consumers live in **Infrastructure** (they depend on MassTransit); they
> call into this thin Application orchestrator, which only maps events to registry calls.

### Key Application Classes

```csharp
// Application/Services/MarketDataOrchestrator.cs
// Thin coordinator — maps each two-legged event to registry consumer counts.
// Holds no WebSocket/Redis logic (that lives in the registry). Both legs are processed
// independently (Task.WhenAll) so one failing leg never blocks the other.
public class MarketDataOrchestrator
{
    private readonly ISubscriptionRegistry _registry;

    public Task AddConsumersAsync(string symbol, ExchangeInfo buy, ExchangeInfo sell, CancellationToken ct)
        => Task.WhenAll(AddLegAsync(symbol, buy, ct), AddLegAsync(symbol, sell, ct));

    public Task RemoveConsumersAsync(string symbol, ExchangeInfo buy, ExchangeInfo sell, CancellationToken ct)
        => Task.WhenAll(RemoveLegAsync(symbol, buy, ct), RemoveLegAsync(symbol, sell, ct));

    // Called by MarketDataWorker on startup — reopen sockets for surviving Redis counters.
    public async Task RecoverConnectionsAsync(CancellationToken ct)
    {
        foreach (var key in await _registry.GetAllActiveKeysAsync())
            await _registry.EnsureConnectedAsync(key, ct);
    }

    // AddLeg/RemoveLeg build a SubscriptionKey from ExchangeInfo (catching unsupported
    // contract types) then call _registry.AddConsumerAsync / RemoveConsumerAsync.
}

// Application/EventHandlers/TickReceivedHandler.cs
public class TickReceivedHandler
{
    private readonly ITickPublisher _publisher;
    private readonly TickValidator _validator;

    public async Task HandleAsync(PriceTick tick, CancellationToken ct)
    {
        if (!_validator.IsFresh(tick)) { /* log Debug, skip */ return; }
        if (!_validator.IsSane(tick)) { /* log Warning, skip */ return; }

        await _publisher.PublishToWorkersAsync(tick, ct);
        await _publisher.PublishToClientAsync(tick, ct);
    }
}
```

---

## Project: MarketDataService.Infrastructure
**Dependencies:** Core, Application + StackExchange.Redis, System.Net.WebSockets, MassTransit.RabbitMQ

```
Infrastructure/
├── Adapters/                              # One file per exchange
│   ├── BinanceAdapter.cs                  # Supports Perpetual + Spot
│   ├── AsterAdapter.cs                    # Supports Perpetual only
│   └── DepthMessage.cs                    # Shared depth payload DTO + parser
│
├── WebSocket/
│   └── WebSocketBase.cs                   # Connect, receive loop, reconnect, ping/pong
│
├── Consumers/                             # MassTransit IConsumer<T> — one per event
│   ├── JobCreatedConsumer.cs             # → orchestrator.AddConsumersAsync(both legs)
│   ├── JobFinishedConsumer.cs            # → orchestrator.RemoveConsumersAsync(both legs)
│   ├── SubscriptionCreatedConsumer.cs    # → orchestrator.AddConsumersAsync(both legs)
│   └── SubscriptionDeletedConsumer.cs    # → orchestrator.RemoveConsumersAsync(both legs)
│
├── Publishing/
│   ├── RedisTickPublisher.cs              # Implements ITickPublisher
│   └── ClientThrottle.cs                  # Per-symbol+contractType 200ms throttle
│
├── Registry/
│   └── RedisSubscriptionRegistry.cs       # ISubscriptionRegistry: Redis counter + WS lifecycle
│
├── Health/
│   └── WebSocketHealthCheck.cs            # IHealthCheck: stale connection detection
│
├── Options/                               # ReconnectPolicy, Exchanges, Publishing, Health, Redis
│
└── DependencyInjection/
    └── InfrastructureServiceExtensions.cs # AddInfrastructure: Redis, adapters, registry, MassTransit
```

> `ExchangeSubscription` (the connection handle) lives in **Core** so `IExchangeAdapter`
> can return it. Adapters are registered **transient** — each market gets its own connection
> instance (the registry resolves a fresh adapter per `EnsureConnectedAsync`).

### WebSocket Stream URLs by Contract Type

Each adapter resolves the correct URL based on `ContractType`:

```csharp
// Infrastructure/Adapters/BinanceAdapter.cs
public class BinanceAdapter : WebSocketBase, IExchangeAdapter
{
    public string Exchange => "Binance";

    public IReadOnlyList<ContractType> SupportedContractTypes =>
        [ContractType.Perpetual, ContractType.Spot];

    protected override string GetStreamUrl() => _contractType switch
    {
        ContractType.Perpetual =>
            $"wss://fstream.binance.com/stream?streams={_symbol.ToLower()}@depth5@100ms",
        ContractType.Spot =>
            $"wss://stream.binance.com:9443/stream?streams={_symbol.ToLower()}@depth5@100ms",
        _ => throw new UnsupportedContractException(Exchange, _contractType)
    };

    protected override string? GetSubscriptionMessage() => null;

    protected override void OnMessageReceived(string message)
    {
        var raw = JsonSerializer.Deserialize<BinanceDepthMessage>(message);
        var tick = new PriceTick
        {
            Exchange     = Exchange,
            Symbol       = _symbol,
            ContractType = _contractType,
            BestBid      = decimal.Parse(raw.Data.Bids[0][0]),
            BestAsk      = decimal.Parse(raw.Data.Asks[0][0]),
            ReceivedAt   = DateTime.UtcNow
        };
        LastTickReceivedAt = tick.ReceivedAt;
        _onTick(tick);
    }
}

// Infrastructure/Adapters/AsterAdapter.cs
public class AsterAdapter : WebSocketBase, IExchangeAdapter
{
    public string Exchange => "Aster";

    // Aster only supports perpetual futures in MVP
    public IReadOnlyList<ContractType> SupportedContractTypes =>
        [ContractType.Perpetual];

    protected override string GetStreamUrl() => _contractType switch
    {
        ContractType.Perpetual =>
            $"wss://fstream.asterdex.com/stream?streams={_symbol.ToLower()}@depth5",
        _ => throw new UnsupportedContractException(Exchange, _contractType)
    };
}
```

### WebSocketBase

All adapters extend `WebSocketBase`. Never duplicate reconnect or ping/pong logic in adapters.

```csharp
public abstract class WebSocketBase
{
    protected abstract string GetStreamUrl();
    protected abstract string? GetSubscriptionMessage();
    protected abstract void OnMessageReceived(string message);

    public event Func<Exception, Task>? OnFatalError;
    public event Func<int, Task>? OnReconnect;

    // Read by WebSocketHealthCheck to detect stale connections
    public DateTime LastTickReceivedAt { get; protected set; }

    public async Task StartAsync(CancellationToken ct) { ... }
    protected async Task SendTextAsync(string text, CancellationToken ct) { ... }
}
```

Reconnect policy: exponential backoff `InitialDelaySeconds * 2^attempt`, capped at `MaxDelaySeconds`. On max retries → invoke `OnFatalError`, stop loop.

### RedisTickPublisher

```csharp
public class RedisTickPublisher : ITickPublisher
{
    public Task PublishToWorkersAsync(PriceTick tick, CancellationToken ct)
    {
        // tick:{Exchange}:{Symbol}:{ContractType}
        var channel = RedisChannel.Literal(tick.ToSubscriptionKey().WorkerChannel);
        _sub.Publish(channel, Serialize(tick), CommandFlags.FireAndForget);
        return Task.CompletedTask;
    }

    public Task PublishToClientAsync(PriceTick tick, CancellationToken ct)
    {
        if (!_throttle.ShouldPublish(tick.Symbol, tick.ContractType))
            return Task.CompletedTask;

        // tick:client:{Symbol}:{ContractType}
        var channel = RedisChannel.Literal(tick.ToSubscriptionKey().ClientChannel);
        _sub.Publish(channel, Serialize(tick), CommandFlags.FireAndForget);
        return Task.CompletedTask;
    }
}
```

### RedisSubscriptionRegistry

The registry owns both the **shared Redis reference counter** and the **in-process WebSocket
connections** for this instance.

- `AddConsumerAsync` → `INCR consumers:{Exchange}:{Symbol}:{ContractType}`; when the atomic result
  is `1`, opens the WebSocket (resolves a fresh adapter, subscribes, stores the handle, starts it).
- `RemoveConsumerAsync` → `DECR`; if the result is `< 0` it resets the key to `0` and logs a warning
  (never negative); when the result is `<= 0` it disposes and removes the connection.
- `EnsureConnectedAsync` → opens a connection for an already-counted key **without** incrementing
  (used by startup recovery); idempotent via `TryAdd`.
- Connections run under a **registry-lifetime** cancellation token — never the per-message token —
  so a socket is not torn down when the consumer that opened it returns.

---

## Project: MarketDataService.Worker
**Dependencies:** Application, Infrastructure, Microsoft.Extensions.Hosting, Serilog

This project is a **pure background Host** — no HTTP server, no minimal API.

```
Worker/
├── MarketDataService.Worker.csproj  # Microsoft.NET.Sdk.Worker
├── Program.cs                       # Composition root — Host.CreateApplicationBuilder
├── MarketDataWorker.cs              # BackgroundService: recovers connections, then idles
├── appsettings.json
└── appsettings.Development.json
```

```csharp
// Worker/Program.cs
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSerilog((services, configuration) => configuration
    .ReadFrom.Configuration(builder.Configuration)
    .ReadFrom.Services(services)
    .Enrich.FromLogContext()
    .WriteTo.Console());

builder.Services
    .AddApplication()
    .AddInfrastructure(builder.Configuration);   // registers the MassTransit bus (hosted service)

builder.Services.AddHostedService<MarketDataWorker>();

var host = builder.Build();
host.Run();
```

### MarketDataWorker — recovers on startup, then idles

```csharp
// Worker/MarketDataWorker.cs
public sealed class MarketDataWorker : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        // Rebuild WebSocket connections from any surviving Redis counters BEFORE anything else.
        // A Redis outage at boot must not crash the host — log and continue.
        try { await _orchestrator.RecoverConnectionsAsync(ct); }
        catch (Exception ex) { _logger.LogError(ex, "Startup recovery failed — continuing"); }

        _logger.LogInformation("Market Data Service ready. Listening for events via RabbitMQ.");
        await Task.Delay(Timeout.Infinite, ct);   // all work is driven by MassTransit consumers
    }
}
```

---

## Event Contract (RabbitMQ / MassTransit)

The service binds one queue per event type to the topic exchange **`arbitrage.events`**:

| Event | Routing key | Queue | Action |
|---|---|---|---|
| `JobCreatedEvent` | `job.created` | `market-data.job-created` | `AddConsumers` (both legs) |
| `JobFinishedEvent` | `job.finished` | `market-data.job-finished` | `RemoveConsumers` (both legs) |
| `SubscriptionCreatedEvent` | `subscription.created` | `market-data.subscription-created` | `AddConsumers` (both legs) |
| `SubscriptionDeletedEvent` | `subscription.deleted` | `market-data.subscription-deleted` | `RemoveConsumers` (both legs) |

Each event payload:
```json
{
  "jobId": "f970ed8f-171c-4fbb-8f79-89cf7092b2f5",
  "symbol": "BTCUSDT",
  "buyExchange":  { "name": "Binance", "type": "perpetual" },
  "sellExchange": { "name": "Aster",   "type": "perpetual" },
  "timestamp": 1718800000
}
```
(`Subscription*` events use `subscriptionId` instead of `jobId`.)

Each endpoint applies `UseMessageRetry(Immediate(3))`; messages that still fault land in
MassTransit's automatic `<queue>_error` dead-letter queue. The service **only consumes** events —
it never publishes any.

### Health
`WebSocketHealthCheck` is registered (per-connection staleness) but **not** exposed over HTTP
in this host — there is no web server. It is available for an out-of-band `IHealthCheckPublisher`.

---

## Redis Key Naming

```
Worker channel:   tick:{Exchange}:{Symbol}:{ContractType}   e.g. tick:Binance:BTCUSDT:Perpetual
Client channel:   tick:client:{Symbol}:{ContractType}       e.g. tick:client:BTCUSDT:Perpetual
Consumer counter: consumers:{Exchange}:{Symbol}:{ContractType}  e.g. consumers:Binance:BTCUSDT:Perpetual
```

The consumer counter is a Redis string mutated only via atomic `INCR`/`DECR` (never read-modify-write).
It is shared across restarts and used for startup recovery.

### Publishing Rules
- Always use `CommandFlags.FireAndForget` — never await Redis publish in the tick hot path
- Serialize with `System.Text.Json`
- Never log individual ticks at `Information` level — use `Debug` only (very high volume)
- Throttle client channel at source: publish at most once per `ClientThrottleMs` per symbol+contractType

---

## .csproj References

```xml
<!-- Application.csproj -->
<ProjectReference Include="..\MarketDataService.Core\MarketDataService.Core.csproj" />

<!-- Infrastructure.csproj -->
<ProjectReference Include="..\MarketDataService.Core\MarketDataService.Core.csproj" />
<ProjectReference Include="..\MarketDataService.Application\MarketDataService.Application.csproj" />
<PackageReference Include="StackExchange.Redis" Version="2.8.0" />
<PackageReference Include="Microsoft.Extensions.Diagnostics.HealthChecks" />
<PackageReference Include="MassTransit" Version="8.5.10" />          <!-- v8.x = Apache-2.0 (v9 needs a license) -->
<PackageReference Include="MassTransit.RabbitMQ" Version="8.5.10" />

<!-- Worker.csproj — Microsoft.NET.Sdk.Worker (no ASP.NET) -->
<ProjectReference Include="..\MarketDataService.Application\MarketDataService.Application.csproj" />
<ProjectReference Include="..\MarketDataService.Infrastructure\MarketDataService.Infrastructure.csproj" />
<PackageReference Include="Microsoft.Extensions.Hosting" />
<PackageReference Include="Serilog.Extensions.Hosting" />
<PackageReference Include="Serilog.Settings.Configuration" />
<PackageReference Include="Serilog.Sinks.Console" />
```

---

## Configuration

Static exchange config removed. `appsettings.json` now only contains infrastructure settings:

```json
{
  "Redis": {
    "ConnectionString": "redis:6379"
  },
  "RabbitMQ": {
    "Host": "rabbitmq",
    "VirtualHost": "/",
    "Username": "guest",
    "Password": "guest"
  },
  "Publishing": {
    "ClientThrottleMs": 200
  },
  "Health": {
    "StaleThresholdSeconds": 5
  },
  "Exchanges": {
    "Binance": {
      "ReconnectPolicy": {
        "MaxAttempts": 5,
        "InitialDelaySeconds": 1,
        "MaxDelaySeconds": 30
      }
    },
    "Aster": {
      "ReconnectPolicy": {
        "MaxAttempts": 5,
        "InitialDelaySeconds": 1,
        "MaxDelaySeconds": 30
      }
    }
  }
}
```

No `Symbols` list — markets are provided at runtime via RabbitMQ events. The `RabbitMQ` section
configures the broker connection used by MassTransit.

---

## Adding a New Exchange

1. Create `{Exchange}Adapter.cs` in `Infrastructure/Adapters/` extending `WebSocketBase`
2. Implement `GetStreamUrl()` with a `switch` on `ContractType`
3. Set `SupportedContractTypes` to only types that exchange actually supports
4. Parse exchange-specific message format → produce `PriceTick`
5. Register: `services.AddTransient<IExchangeAdapter, {Exchange}Adapter>()` (transient — one connection per market)
6. Add reconnect policy block to `appsettings.json`

**Nothing in Core, Application, or the event consumers changes.**

---

## Error Handling Rules

| Scenario | Behavior |
|---|---|
| Unsupported contract type in an event leg | Log Warning, skip that leg (the other leg still proceeds) |
| Unknown exchange (no adapter) | Log Error, skip — counter stays; no connection opened |
| Faulted consumer | Retry immediately ×3, then route to `<queue>_error` dead-letter queue |
| `RemoveConsumer` on a zero counter | Reset counter to 0, log Warning — never go negative |
| WebSocket disconnect | Reconnect with exponential backoff, log Warning |
| Max reconnect attempts exceeded | Log Critical, invoke `OnFatalError`, drop the connection (counter left intact) |
| Redis publish failure | Log Error, continue — do not crash the service |
| Redis unavailable at startup recovery | Log Error, continue — events rebuild state |
| Malformed message from exchange | Log Warning with raw payload, skip tick |
| No tick for > StaleThresholdSeconds | Health check reports Degraded |

**Never crash the host process due to a single adapter or leg failure.** Everything else keeps running.

---

## Logging Guidelines

```
Information  Service start, subscription added/removed, adapter connected/disconnected
Warning      Reconnect attempt, stale connection detected, malformed message
Error        Redis publish failure, adapter max retries reached
Critical     Unhandled exception, fatal adapter error
Debug        Individual tick received (disabled in Production)
```

Always use structured logging with named properties:

```csharp
// ✅
_logger.LogInformation("Subscription started: {Exchange} {Symbol} {ContractType}",
    key.Exchange, key.Symbol, key.ContractType);

// ❌
_logger.LogInformation($"Subscription started: {key}");
```

---

## Health Check

`WebSocketHealthCheck` reports per active subscription:

- `Healthy` — tick received within `StaleThresholdSeconds`
- `Degraded` — connected but stale
- `Unhealthy` — adapter stopped or fatal error

---

## NuGet / Package Restore

`Vludik.Arbitrage.*` packages (e.g. `Vludik.Arbitrage.Events`) are published to GitHub Packages,
not nuget.org. The solution-root `nuget.config` declares both sources and maps
`Vludik.Arbitrage.*` to GitHub via `packageSourceMapping`:

```xml
<packageSources>
  <clear />
  <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  <add key="github" value="https://nuget.pkg.github.com/directiv13/index.json" />
</packageSources>
<packageSourceCredentials>
  <github>
    <add key="Username" value="USERNAME" />
    <add key="ClearTextPassword" value="%GITHUB_TOKEN%" />
  </github>
</packageSourceCredentials>
```

`%GITHUB_TOKEN%` is expanded by NuGet itself from the environment — never hardcode the token.
A PAT with `read:packages` scope for the `directiv13` org must be present as `GITHUB_TOKEN`:
- Locally, export it in your shell before `dotnet restore`/`dotnet build` (the `<clear />` means
  restore no longer falls back to any machine/user `NuGet.Config`).
- For Docker, copy `.env.example` to `.env` and set `GITHUB_TOKEN` there — `docker compose build`
  picks it up automatically. It is never written into the final image (see below).

## Docker

```dockerfile
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=${GITHUB_TOKEN}
WORKDIR /src
COPY . .
RUN dotnet restore MarketDataService.Worker/MarketDataService.Worker.csproj
RUN dotnet publish MarketDataService.Worker/MarketDataService.Worker.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/runtime:9.0
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "MarketDataService.Worker.dll"]
```

Note: use the `runtime` base image (not `aspnet`) — the service is a pure worker with no HTTP server.
`GITHUB_TOKEN` is only `ARG`/`ENV` in the `build` stage (needed for restore) — the final stage is a
fresh `FROM`, so it never inherits the token; it does not appear in the shipped image's env or
`docker history`.

```yaml
# docker-compose.yml
market-data:
  build:
    context: .
    args:
      GITHUB_TOKEN: ${GITHUB_TOKEN}
  environment:
    - Redis__ConnectionString=redis:6379
    - RabbitMQ__Host=rabbitmq
    - RabbitMQ__Username=guest
    - RabbitMQ__Password=guest
  depends_on:
    - redis
    - rabbitmq
  restart: unless-stopped

rabbitmq:
  image: rabbitmq:3-management-alpine
  ports:
    - "5672:5672"     # AMQP
    - "15672:15672"   # Management UI
  restart: unless-stopped
```

Compose auto-loads `GITHUB_TOKEN` from a local `.env` file (gitignored — see `.env.example`) to
populate the build arg. No port is exposed for `market-data` — nothing calls it over HTTP.

---

## What This Service Does NOT Do

- ❌ No REST API / HTTP server — it is a pure background worker
- ❌ No static symbol list — markets come from RabbitMQ events at runtime
- ❌ No event publishing — it only consumes events
- ❌ No database access
- ❌ No order execution
- ❌ No spread calculation
- ❌ No job management
- ❌ No authentication or API keys
- ❌ No business logic beyond reference counting and routing ticks to Redis channels

---

## Consumers of This Service

| Consumer | How they interact | Channel |
|---|---|---|
| Jobs Service | Publishes `job.created` / `job.finished` to `arbitrage.events` | RabbitMQ |
| Subscription Service | Publishes `subscription.created` / `subscription.deleted` to `arbitrage.events` | RabbitMQ |
| Spread Jobs (Workers) | Redis SUB | `tick:{Exchange}:{Symbol}:{ContractType}` |
| Subscription Service | Redis SUB | `tick:client:{Symbol}:{ContractType}` |

Tick consumers subscribe to Redis independently. This service has no knowledge of them.

---

## Known Limitations

### Horizontal scaling — single-instance by design
The consumer reference counter lives in **shared Redis**, but the WebSocket connections are held
**in-process**. These two facts do not coordinate across instances:

- On startup recovery, **every** instance opens its own socket for **every** key with a non-zero
  counter → duplicate exchange connections and **duplicate** `tick:*` publishes.
- The `0 → 1` "open" only fires on the single instance that happens to process the create event, so
  scaling out does not distribute connections — it duplicates or randomly partitions them.

Run the Market Data Service as a **single instance**. Supporting multiple instances would require
partitioning markets across instances (e.g. consistent hashing) or moving connection ownership into
a coordinated/leased scheme — neither is implemented.