namespace MarketDataService.Core.Models;

/// <summary>
/// A handle to one active exchange subscription (one live WebSocket connection).
/// Owns the cancellation token source that stops the adapter's receive loop and
/// exposes the connection's freshness for health checks.
/// </summary>
/// <remarks>
/// This type only depends on BCL primitives (a cancellation source, a task and two
/// delegates), so it lives in Core even though it is created by Infrastructure
/// adapters — this keeps <see cref="Interfaces.IExchangeAdapter"/>'s return type
/// inside Core without dragging WebSocket/Redis dependencies into it.
/// </remarks>
public sealed class ExchangeSubscription : IAsyncDisposable
{
    private readonly Func<CancellationToken, Task> _run;
    private readonly Func<DateTime> _lastTickAccessor;
    private readonly CancellationTokenSource _cts;
    private readonly object _gate = new();
    private Task? _runTask;

    public ExchangeSubscription(
        SubscriptionKey key,
        Func<CancellationToken, Task> run,
        Func<DateTime> lastTickAccessor,
        CancellationToken external = default)
    {
        Key = key;
        _run = run ?? throw new ArgumentNullException(nameof(run));
        _lastTickAccessor = lastTickAccessor ?? throw new ArgumentNullException(nameof(lastTickAccessor));
        _cts = CancellationTokenSource.CreateLinkedTokenSource(external);
    }

    /// <summary>The market this subscription monitors.</summary>
    public SubscriptionKey Key { get; }

    /// <summary>UTC time of the last tick received on this connection (default if none yet).</summary>
    public DateTime LastTickReceivedAt => _lastTickAccessor();

    /// <summary>
    /// Starts the underlying connection loop once (idempotent). Fire-and-forget:
    /// the returned task is tracked internally so <see cref="DisposeAsync"/> can await it.
    /// </summary>
    public void Start()
    {
        lock (_gate)
        {
            _runTask ??= Task.Run(() => _run(_cts.Token));
        }
    }

    /// <summary>Cancels the connection loop and waits for it to drain.</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _cts.CancelAsync().ConfigureAwait(false);
        }
        catch
        {
            // Cancellation is best-effort; never throw while tearing down.
        }

        Task? running;
        lock (_gate)
        {
            running = _runTask;
        }

        if (running is not null)
        {
            try
            {
                await running.ConfigureAwait(false);
            }
            catch
            {
                // The loop swallows its own exceptions; ignore cancellation on shutdown.
            }
        }

        _cts.Dispose();
    }
}
