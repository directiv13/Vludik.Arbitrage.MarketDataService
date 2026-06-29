using System.Net.WebSockets;
using System.Text;
using MarketDataService.Core.Exceptions;
using MarketDataService.Core.Models;
using MarketDataService.Infrastructure.Options;
using Microsoft.Extensions.Logging;
using Vludik.Arbitrage.Shared.Enums;

namespace MarketDataService.Infrastructure.WebSocket;

/// <summary>
/// Base class for all exchange adapters. Owns the full connection lifecycle for one
/// market stream: connect, receive loop, keep-alive ping loop and exponential-backoff
/// reconnect. Subclasses only supply the stream URL, an optional subscription message
/// and message parsing. Reconnect logic lives here and must never be duplicated.
/// </summary>
public abstract class WebSocketBase
{
    private const int ReceiveBufferSize = 16 * 1024;
    private static readonly TimeSpan PingInterval = TimeSpan.FromSeconds(30);

    private readonly ILogger _logger;
    private readonly ReconnectPolicy _policy;

    // Per-connection state, configured by the adapter in SubscribeAsync.
    protected string _symbol = string.Empty;
    protected ContractType _contractType;
    protected Action<PriceTick> _onTick = static _ => { };

    protected WebSocketBase(ILogger logger, ReconnectPolicy policy)
    {
        _logger = logger;
        _policy = policy;
    }

    /// <summary>UTC time of the most recent message; read by the health check.</summary>
    public DateTime LastTickReceivedAt { get; protected set; }

    /// <summary>Raised once when the adapter exhausts its reconnect attempts.</summary>
    public event Func<Exception, Task>? OnFatalError;

    /// <summary>Raised before each reconnect attempt, with the attempt number.</summary>
    public event Func<int, Task>? OnReconnect;

    protected abstract string GetStreamUrl();
    protected abstract string? GetSubscriptionMessage();
    protected abstract void OnMessageReceived(string message);

    /// <summary>Configures this connection for a specific market. Called once before <see cref="StartAsync"/>.</summary>
    protected void Configure(string symbol, ContractType contractType, Action<PriceTick> onTick)
    {
        _symbol = symbol;
        _contractType = contractType;
        _onTick = onTick;
    }

    /// <summary>
    /// Runs the connect/receive/reconnect loop until cancelled or fatally failed.
    /// All exceptions are handled internally — this method never throws.
    /// </summary>
    public async Task StartAsync(CancellationToken ct)
    {
        var attempt = 0;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                using var ws = new ClientWebSocket();
                ws.Options.KeepAliveInterval = PingInterval;

                var url = GetStreamUrl();
                await ws.ConnectAsync(new Uri(url), ct).ConfigureAwait(false);

                var subscriptionMessage = GetSubscriptionMessage();
                if (!string.IsNullOrEmpty(subscriptionMessage))
                    await SendTextAsync(ws, subscriptionMessage, ct).ConfigureAwait(false);

                if (attempt > 0)
                    _logger.LogInformation("Reconnected: {Exchange} {Symbol} {ContractType}",
                        ExchangeName, _symbol, _contractType);
                else
                    _logger.LogInformation("Connected: {Exchange} {Symbol} {ContractType}",
                        ExchangeName, _symbol, _contractType);

                attempt = 0; // Successful connect resets the backoff sequence.

                await RunConnectionAsync(ws, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break; // Clean shutdown — not an error.
            }
            catch (Exception ex)
            {
                attempt++;

                if (attempt > _policy.MaxAttempts)
                {
                    _logger.LogCritical(ex,
                        "Adapter {Exchange} {Symbol} {ContractType} exhausted {MaxAttempts} reconnect attempts — giving up",
                        ExchangeName, _symbol, _contractType, _policy.MaxAttempts);

                    await InvokeFatalAsync(
                        new AdapterFatalException(ExchangeName, _symbol, _contractType, _policy.MaxAttempts, ex))
                        .ConfigureAwait(false);
                    return;
                }

                var delay = ComputeBackoff(attempt);
                _logger.LogWarning(ex,
                    "WebSocket error on {Exchange} {Symbol} {ContractType}; reconnect attempt {Attempt}/{MaxAttempts} in {Delay}s",
                    ExchangeName, _symbol, _contractType, attempt, _policy.MaxAttempts, delay.TotalSeconds);

                await RaiseReconnectAsync(attempt).ConfigureAwait(false);

                try
                {
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    /// <summary>Sends a UTF-8 text frame.</summary>
    protected static async Task SendTextAsync(ClientWebSocket ws, string text, CancellationToken ct)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        await ws.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, ct).ConfigureAwait(false);
    }

    /// <summary>Runs the receive and ping loops together; either stopping tears down the other.</summary>
    private async Task RunConnectionAsync(ClientWebSocket ws, CancellationToken ct)
    {
        using var connCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var receiveTask = ReceiveLoopAsync(ws, connCts.Token);
        var pingTask = PingLoopAsync(ws, connCts.Token);

        var finished = await Task.WhenAny(receiveTask, pingTask).ConfigureAwait(false);

        // Stop the sibling loop and let it drain.
        await connCts.CancelAsync().ConfigureAwait(false);
        try
        {
            await Task.WhenAll(receiveTask, pingTask).ConfigureAwait(false);
        }
        catch
        {
            // The faulting task's exception is observed below; sibling cancellation is expected.
        }

        // Surface the reason the connection ended so the reconnect loop can react.
        await finished.ConfigureAwait(false);
    }

    private async Task ReceiveLoopAsync(ClientWebSocket ws, CancellationToken ct)
    {
        var buffer = new byte[ReceiveBufferSize];
        using var message = new MemoryStream();

        while (!ct.IsCancellationRequested && ws.State == WebSocketState.Open)
        {
            message.SetLength(0);
            WebSocketReceiveResult result;

            do
            {
                result = await ws.ReceiveAsync(buffer, ct).ConfigureAwait(false);

                if (result.MessageType == WebSocketMessageType.Close)
                    throw new WebSocketException(WebSocketError.ConnectionClosedPrematurely,
                        "WebSocket closed by remote endpoint.");

                message.Write(buffer, 0, result.Count);
            }
            while (!result.EndOfMessage);

            if (result.MessageType != WebSocketMessageType.Text)
                continue;

            var text = Encoding.UTF8.GetString(message.GetBuffer(), 0, (int)message.Length);

            // A message arrived: mark freshness before dispatching to the parser.
            LastTickReceivedAt = DateTime.UtcNow;
            OnMessageReceived(text);
        }
    }

    private static async Task PingLoopAsync(ClientWebSocket ws, CancellationToken ct)
    {
        // The socket emits real WebSocket ping frames via KeepAliveInterval; this loop
        // guards liveness so a half-open socket triggers a reconnect promptly.
        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(PingInterval, ct).ConfigureAwait(false);

            if (ws.State != WebSocketState.Open)
                throw new WebSocketException(WebSocketError.InvalidState,
                    $"WebSocket no longer open (state: {ws.State}).");
        }
    }

    private TimeSpan ComputeBackoff(int attempt)
    {
        var seconds = _policy.InitialDelaySeconds * Math.Pow(2, attempt - 1);
        seconds = Math.Min(seconds, _policy.MaxDelaySeconds);
        return TimeSpan.FromSeconds(seconds);
    }

    private async Task InvokeFatalAsync(Exception ex)
    {
        var handler = OnFatalError;
        if (handler is null) return;

        try
        {
            await handler.Invoke(ex).ConfigureAwait(false);
        }
        catch (Exception handlerEx)
        {
            _logger.LogError(handlerEx, "OnFatalError handler threw for {Exchange} {Symbol} {ContractType}",
                ExchangeName, _symbol, _contractType);
        }
    }

    private async Task RaiseReconnectAsync(int attempt)
    {
        var handler = OnReconnect;
        if (handler is null) return;

        try
        {
            await handler.Invoke(attempt).ConfigureAwait(false);
        }
        catch (Exception handlerEx)
        {
            _logger.LogError(handlerEx, "OnReconnect handler threw for {Exchange} {Symbol} {ContractType}",
                ExchangeName, _symbol, _contractType);
        }
    }

    /// <summary>Exchange name used for logging (provided by the adapter).</summary>
    protected abstract string ExchangeName { get; }
}
