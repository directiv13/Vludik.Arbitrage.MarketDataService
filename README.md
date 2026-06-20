# Market Data Service

A standalone **.NET 9 background worker** that streams real-time cryptocurrency prices.

It connects to exchange WebSockets **on demand** — driven by RabbitMQ events, not a static config —
normalizes the order-book top into price ticks, and republishes them to **Redis Pub/Sub** for
downstream consumers. Active markets are reference-counted in Redis so a WebSocket opens on the first
interested consumer and closes when the last one goes away. On restart the service recovers its
connections from the surviving Redis counters.

There is **no REST API, no database, and no business logic** — it is a pure event-driven worker.

> For the full architectural specification see **[CLAUDE.md](CLAUDE.md)**. This README is the
> human-facing quick start.

---

## Key Features

- **Event-driven** — subscribes to RabbitMQ via [MassTransit](https://masstransit.io/); the Jobs and
  Subscription services publish create/finish events, no HTTP calls.
- **Redis reference counting** — atomic `INCR`/`DECR` per market; a WebSocket opens on count `0 → 1`
  and closes on the return to `0`. Idempotent and concurrency-safe.
- **Startup recovery** — on boot, rebuilds WebSocket connections from any Redis counters that
  survived a previous run.
- **Resilient WebSockets** — exponential-backoff reconnect, keep-alive ping/pong, and stale-connection
  detection live in a single `WebSocketBase`; adapters never duplicate that logic.
- **Two output channels** — full-rate worker channel + a throttled client channel
  (at most one tick per `ClientThrottleMs` per symbol).
- **Fail-soft everywhere** — a single adapter, a Redis hiccup, or an unreachable broker never crashes
  the host; failures are logged and retried.
- **Pluggable exchanges** — Binance (spot + perpetual) and Aster (perpetual) ship today; adding one is
  a single adapter class.
- **Clean architecture** — `Core` has zero dependencies; everything points inward.

---

## Project Structure

```
MarketDataService/
├── MarketDataService.sln
├── MarketDataService.Core/            # Models, events, interfaces, exceptions — zero dependencies
├── MarketDataService.Application/     # Orchestrator, tick validation/publishing pipeline
├── MarketDataService.Infrastructure/  # WebSocket adapters, Redis registry, MassTransit consumers
├── MarketDataService.Worker/          # Composition root — pure background Host (no HTTP)
├── docker-compose.yml                 # market-data + redis + rabbitmq
├── Dockerfile                         # multi-stage build → runtime:9.0
└── CLAUDE.md                          # full specification
```

**Dependency rule** (nothing points outward from `Core`):

```
Worker ──► Infrastructure ──► Application ──► Core
```

| Project | Responsibility |
|---|---|
| **Core** | `PriceTick`, `SubscriptionKey`, `ContractType`, the incoming `Events/`, and interfaces (`IExchangeAdapter`, `ITickPublisher`, `ISubscriptionRegistry`, `IThrottle`). No NuGet packages. |
| **Application** | `MarketDataOrchestrator` (maps events → registry calls), `TickReceivedHandler`, `TickValidator`. Depends only on `Core`. |
| **Infrastructure** | `WebSocketBase` + `BinanceAdapter`/`AsterAdapter`, `RedisSubscriptionRegistry`, `RedisTickPublisher`, `ClientThrottle`, the four MassTransit `Consumers/`, and DI wiring. |
| **Worker** | `Host.CreateApplicationBuilder`, Serilog, hosted `MarketDataWorker` (startup recovery, then idles). |

---

## Architecture at a glance

```
Jobs Service / Subscription Service
        │  publish to topic exchange "arbitrage.events"
        ▼
   ┌──────────┐   job.created / job.finished
   │ RabbitMQ │   subscription.created / subscription.deleted
   └────┬─────┘
        │ (MassTransit consumers)
        ▼
   Market Data Service
        │  INCR/DECR  consumers:{exchange}:{symbol}:{contractType}   (Redis)
        │  open 1 WebSocket per market on first consumer (0 → 1)
        │  close it when the count returns to 0
        ▼
   ┌──────────┐   PUBLISH tick:{exchange}:{symbol}:{contractType}     (full rate)
   │  Redis   │   PUBLISH tick:client:{symbol}:{contractType}         (throttled)
   └──────────┘
```

Every event carries **two legs** (a buy exchange and a sell exchange); both are reference-counted
independently — one failing leg never blocks the other.

**Incoming events** (topic exchange `arbitrage.events`):

| Event | Routing key | Queue | Effect |
|---|---|---|---|
| `JobCreatedEvent` | `job.created` | `market-data.job-created` | add consumer (both legs) |
| `JobFinishedEvent` | `job.finished` | `market-data.job-finished` | remove consumer (both legs) |
| `SubscriptionCreatedEvent` | `subscription.created` | `market-data.subscription-created` | add consumer (both legs) |
| `SubscriptionDeletedEvent` | `subscription.deleted` | `market-data.subscription-deleted` | remove consumer (both legs) |

**Redis keys:**

| Purpose | Key | Example |
|---|---|---|
| Worker tick channel | `tick:{Exchange}:{Symbol}:{ContractType}` | `tick:Binance:BTCUSDT:Perpetual` |
| Client tick channel | `tick:client:{Symbol}:{ContractType}` | `tick:client:BTCUSDT:Perpetual` |
| Consumer counter | `consumers:{Exchange}:{Symbol}:{ContractType}` | `consumers:Binance:BTCUSDT:Perpetual` |

---

## Prerequisites

- [.NET 9 SDK](https://dotnet.microsoft.com/download)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (for Redis + RabbitMQ)

---

## Local Setup (Redis + RabbitMQ in Docker)

Start just the infrastructure dependencies — Redis and RabbitMQ — and run the worker from the SDK:

```bash
docker compose up -d redis rabbitmq
```

This gives you:

| Service | Address | Notes |
|---|---|---|
| Redis | `localhost:6379` | tick channels + consumer counters |
| RabbitMQ (AMQP) | `localhost:5672` | event bus |
| RabbitMQ (Management UI) | http://localhost:15672 | login `guest` / `guest` |

Check they are healthy:

```bash
docker exec $(docker ps -qf name=redis) redis-cli ping        # → PONG
docker exec $(docker ps -qf name=rabbitmq) rabbitmq-diagnostics -q ping
```

---

## Run the project

Build, then run the worker in the **Development** environment (its `appsettings.Development.json`
points Redis and RabbitMQ at `localhost`):

```bash
dotnet build
DOTNET_ENVIRONMENT=Development dotnet run --project MarketDataService.Worker
```

Expected startup log:

```
[INF] Configured endpoint market-data.job-created, Consumer: ...JobCreatedConsumer
[INF] Configured endpoint market-data.job-finished, Consumer: ...JobFinishedConsumer
[INF] Configured endpoint market-data.subscription-created, Consumer: ...
[INF] Configured endpoint market-data.subscription-deleted, Consumer: ...
[INF] Recovering 0 WebSocket connection(s) from Redis
[INF] Market Data Service ready. Listening for events via RabbitMQ.
```

> The host is fail-soft: if Redis or RabbitMQ is unreachable it keeps running and retries — you'll
> see connection-retry warnings rather than a crash.

### Everything in containers

To build and run the worker *and* its dependencies together:

```bash
docker compose up --build
```

(The `market-data` container reads `Redis__ConnectionString=redis:6379` / `RabbitMQ__Host=rabbitmq`
from compose and exposes no port — nothing calls it over HTTP.)

---

## Try it — smoke test

You can exercise the whole pipeline without the upstream services by publishing an event directly to
the `arbitrage.events` exchange. MassTransit expects an enveloped JSON message; publish it via the
RabbitMQ Management HTTP API.

**1. Publish a `JobCreatedEvent`** (buy = Binance perpetual, sell = Aster perpetual):

```bash
curl -u guest:guest -H "Content-Type: application/json" \
  -X POST http://localhost:15672/api/exchanges/%2f/arbitrage.events/publish \
  -d '{
    "properties": { "content_type": "application/vnd.masstransit+json" },
    "routing_key": "job.created",
    "payload_encoding": "string",
    "payload": "{\"messageType\":[\"urn:message:MarketDataService.Core.Events:JobCreatedEvent\"],\"message\":{\"jobId\":\"11111111-1111-1111-1111-111111111111\",\"symbol\":\"BTCUSDT\",\"buyExchange\":{\"name\":\"Binance\",\"type\":\"perpetual\"},\"sellExchange\":{\"name\":\"Aster\",\"type\":\"perpetual\"},\"timestamp\":0}}"
  }'
# → {"routed":true}
```

**2. Verify the counters and the live tick stream:**

```bash
docker exec $(docker ps -qf name=redis) redis-cli get consumers:Binance:BTCUSDT:Perpetual   # → 1
docker exec $(docker ps -qf name=redis) redis-cli get consumers:Aster:BTCUSDT:Perpetual      # → 1
docker exec $(docker ps -qf name=redis) redis-cli psubscribe 'tick:*'                        # → live ticks
```

The worker log shows `WebSocket opened for …`, `Connected: …`, then `Tick …` lines (Debug level).

**3. Tear down** — publish a matching `JobFinishedEvent` (routing key `job.finished`, same payload
shape). The counter returns to `0` and the worker logs `WebSocket closed … no remaining consumers`.

---

## Configuration

Settings live in `MarketDataService.Worker/appsettings.json` and can be overridden by environment
variables using the standard double-underscore syntax.

| Setting | Env var | Default | Purpose |
|---|---|---|---|
| Redis connection | `Redis__ConnectionString` | `redis:6379` | StackExchange.Redis connection string |
| RabbitMQ host | `RabbitMQ__Host` | `rabbitmq` | broker hostname |
| RabbitMQ vhost | `RabbitMQ__VirtualHost` | `/` | virtual host |
| RabbitMQ user / pass | `RabbitMQ__Username` / `RabbitMQ__Password` | `guest` / `guest` | credentials |
| Client throttle | `Publishing__ClientThrottleMs` | `200` | min ms between client-channel publishes per market |
| Stale threshold | `Health__StaleThresholdSeconds` | `5` | tick age before a connection is Degraded |
| Reconnect policy | `Exchanges__{Exchange}__ReconnectPolicy__{MaxAttempts\|InitialDelaySeconds\|MaxDelaySeconds}` | `5 / 1 / 30` | per-exchange backoff |

---

## Observability

- **Structured logging** via [Serilog](https://serilog.net/) to the console; all log calls use named
  properties.
- **Log levels**: service start/stop, subscriptions, connect/disconnect at `Information`; reconnects
  and malformed messages at `Warning`; individual **ticks at `Debug` only** (very high volume — never
  enable Debug in production for ticks).
- **Health check** — `WebSocketHealthCheck` reports per-connection freshness
  (`Healthy` / `Degraded` / `Unhealthy`). It is registered but **not exposed over HTTP** (this is a
  pure worker); it's available for an out-of-band `IHealthCheckPublisher`.

---

## Extending — add a new exchange

1. Create `Infrastructure/Adapters/{Exchange}Adapter.cs` extending `WebSocketBase`.
2. Implement `GetStreamUrl()` with a `switch` on `ContractType`.
3. Set `SupportedContractTypes` to only what the exchange supports.
4. Parse the exchange payload into a `PriceTick`.
5. Register it transient: `services.AddTransient<IExchangeAdapter, {Exchange}Adapter>()` and add a
   reconnect-policy block to `appsettings.json`.

Nothing in `Core`, `Application`, or the event consumers changes. See **[CLAUDE.md](CLAUDE.md)** for details.

---

## Tech stack

| Area | Technology |
|---|---|
| Runtime | .NET 9 (`Microsoft.NET.Sdk.Worker`) |
| Messaging | RabbitMQ + MassTransit `8.5.10` (v8 = Apache-2.0) |
| Cache / Pub-Sub | Redis + StackExchange.Redis `2.8.0` |
| WebSockets | `System.Net.WebSockets.ClientWebSocket` (no third-party lib) |
| Logging | Serilog (console) |
| Containers | Docker / Docker Compose |

---

## Troubleshooting

| Symptom | Cause / behavior |
|---|---|
| `Connection Failed: rabbitmq://…` warnings | Broker not up yet. The host keeps running and retries — start RabbitMQ or wait. |
| `Recovering 0 …` with Redis down | Recovery is fail-soft; it logs and continues. Events rebuild state once Redis is back. |
| Counter logged as `went negative … reset to 0` | A duplicate/extra delete event; the guard clamps the counter at `0`. Safe to ignore. |
| Faulted event message | Retried immediately ×3, then moved to the auto-created `{queue}_error` dead-letter queue (inspect it in the RabbitMQ UI). |
| No ticks in `tick:*` | Confirm the counter is `> 0`, the worker shows `WebSocket opened`, and your symbol is valid on the exchange. |

---

## Known Limitations

**Single-instance by design.** The consumer counter lives in shared Redis, but WebSocket connections
are held **in-process**. Running multiple instances causes each one to open its own socket on recovery
for every active market — duplicate exchange connections and duplicate `tick:*` publishes — and the
`0 → 1` open only fires on the instance that processed the event. Run the Market Data Service as a
**single instance**; multi-instance would require partitioning markets or coordinated connection
ownership (not implemented). See **[CLAUDE.md](CLAUDE.md#known-limitations)**.

---

## What this service does **not** do

No REST API · no event publishing (consume only) · no database · no order execution · no spread
calculation · no job management · no authentication. It only reference-counts markets and routes
ticks to Redis.

---

## Project conventions

This repo currently ships without a `.gitignore` or `LICENSE` — add them as appropriate for your
workflow (e.g. the standard `dotnet`/`VisualStudio` gitignore and your chosen license).
