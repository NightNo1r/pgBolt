using System.Collections.Frozen;
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Настройки очередей процесса: <see cref="Limits"/> — эффективные лимиты очередей (включая паузу 0), вес на них не
/// влияет; <see cref="Weights"/> — эффективные веса, лимит на них не влияет. Имена сравниваются ordinal.
/// </summary>
internal sealed record QueueSettingsSnapshot(FrozenDictionary<string, int> Limits, FrozenDictionary<string, int> Weights)
{
    public static QueueSettingsSnapshot Empty { get; } = new(FrozenDictionary<string, int>.Empty, FrozenDictionary<string, int>.Empty);
}

/// <summary>
/// Снимок настроек очередей процесса: читается из queue_settings в обход кэша менеджера и публикуется целиком,
/// читатели берут <see cref="Current"/> без lock'а. Первая загрузка — шаг старта (<see cref="LoadAsync"/> бросает);
/// дальше фоновый цикл раз в QueueSettingsRefreshInterval ±10 %, ошибка обновления оставляет прошлый снимок.
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

    /// <summary>Для тестов: сколько раз цикл встал на ожидание (таймер уже взведён).</summary>
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
    /// Внеочередное обновление, не ждёт его. Событие с латчем: вызовы во время идущего обновления дают ровно одно
    /// следующее сразу после него — идущее могло прочитать состояние до изменения. До Start и после StopAsync — no-op.
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
