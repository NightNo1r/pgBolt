using System.Collections.Frozen;
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Process-wide queue settings. <see cref="Limits"/> gives the effective queue limits, including a pause at 0,
/// independently of weights. <see cref="Weights"/> gives effective weights independently of limits. Queue names use
/// ordinal comparison.
/// </summary>
internal sealed record QueueSettingsSnapshot(FrozenDictionary<string, int> Limits, FrozenDictionary<string, int> Weights)
{
    public static QueueSettingsSnapshot Empty { get; } = new(FrozenDictionary<string, int>.Empty, FrozenDictionary<string, int>.Empty);
}

/// <summary>
/// Process-wide queue settings snapshot. Loads queue_settings directly, bypassing the manager cache, and publishes the
/// entire snapshot. Readers access <see cref="Current"/> without locking. The initial load is a startup step and
/// <see cref="LoadAsync"/> propagates failure. The background loop then refreshes every QueueSettingsRefreshInterval
/// ±10%, retaining the previous snapshot on failure.
/// </summary>
internal sealed partial class QueueSettingsCache(IServerStore store, PgBoltProcessingOptions options, MonoClock clock, ILogger logger,
    PgBoltMetrics metrics)
    : IQueueSettingsRefresher
{
    private const double Jitter = 0.1;

    private readonly AsyncAutoResetEvent _signal = new();
    private readonly CancellationTokenSource _stop = new();
    private QueueSettingsSnapshot _current = QueueSettingsSnapshot.Empty;
    private Task _loop = Task.CompletedTask;
    private volatile bool _running;
    private int _waits;

    public QueueSettingsSnapshot Current => Volatile.Read(ref _current);

    internal Func<double> NextRandom { get; set; } = Random.Shared.NextDouble;

    /// <summary>For tests, counts waits that the loop has entered with their timer armed.</summary>
    internal int Waits => Volatile.Read(ref _waits);

    public async Task LoadAsync(CancellationToken ct)
    {
        var rows = await store.GetActiveQueueSettingsAsync(ct);
        var limits = rows.Where(r => r.Limit is not null).ToFrozenDictionary(r => r.Queue, r => r.Limit!.Value, StringComparer.Ordinal);
        var weights = rows.Where(r => r.Weight is not null).ToFrozenDictionary(r => r.Queue, r => r.Weight!.Value, StringComparer.Ordinal);
        Volatile.Write(ref _current, new QueueSettingsSnapshot(limits, weights));
    }

    public void Start()
    {
        _running = true;
        _loop = Task.Run(LoopAsync);
    }

    public async Task StopAsync()
    {
        _running = false;
        await _stop.CancelAsync();
        await _loop;
    }

    /// <summary>
    /// Requests an immediate refresh without waiting for it. Calls during a refresh latch a single follow-up refresh
    /// because the current one may have read the old state. Has no effect before Start or after StopAsync.
    /// </summary>
    public void RefreshNow()
    {
        if (_running)
            _signal.Set();
    }

    private async Task LoopAsync()
    {
        var token = _stop.Token;
        while (!token.IsCancellationRequested)
        {
            var interval = options.QueueSettingsRefreshInterval * (1 + Jitter * (2 * NextRandom() - 1));
            var wait = _signal.WaitAsync(interval, clock.Time, token);
            Interlocked.Increment(ref _waits);
            try
            {
                await wait;
                await LoadAsync(token);
            }
            catch (Exception e)
            {
                if (!token.IsCancellationRequested)
                {
                    metrics.StorageError(StorageOperations.Settings);
                    Log.QueueSettingsRefreshFailed(logger, e);
                }
            }
        }
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1200, EventName = nameof(QueueSettingsRefreshFailed), Level = LogLevel.Warning,
            Message = "PgBolt: refreshing queue settings failed; using the previous snapshot.")]
        internal static partial void QueueSettingsRefreshFailed(ILogger logger, Exception e);
    }
}
