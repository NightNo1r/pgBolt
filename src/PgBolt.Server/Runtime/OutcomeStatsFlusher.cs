using Microsoft.Extensions.Logging;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Сброс счётчиков исходов (часть 6 §4.1): раз в interval — один statement со всеми строками процесса; при остановке —
/// финальный сброс после идущего. Одновременно идёт не больше одного сброса: цикл последовательный, финальный — после него.
/// </summary>
internal sealed partial class OutcomeStatsFlusher(IServerStore store, OutcomeStats stats, TimeSpan interval, MonoClock clock, ILogger logger,
    PgBoltMetrics metrics)
{
    private readonly CancellationTokenSource _stop = new();     // пауза цикла
    private readonly CancellationTokenSource _abort = new();    // идущий statement (отменённая остановка)
    private readonly LogThrottle _throttle = new(clock, TimeSpan.FromMinutes(1));
    private Task _loop = Task.CompletedTask;
    private int _idleWaits;

    /// <summary>Для тестов: сколько раз цикл встал на паузу (таймер уже взведён).</summary>
    internal int IdleWaits => Volatile.Read(ref _idleWaits);

    public void Start() => _loop = LoopAsync();

    /// <summary>Останавливает цикл, дождавшись идущего сброса, и выполняет финальный; отменённый ct обрывает сброс и пропускает финальный.</summary>
    public async Task StopAsync(CancellationToken ct)
    {
        using var abort = ct.Register(static s => ((CancellationTokenSource)s!).Cancel(), _abort);
        await _stop.CancelAsync();
        await _loop;
        if (!ct.IsCancellationRequested)
            await FlushOnceAsync(ct);
    }

    private async Task FlushOnceAsync(CancellationToken ct)
    {
        var rows = stats.Take();
        if (rows.Count == 0)
            return;
        try
        {
            // WaitAsync: store, не соблюдающий токен, не держит остановку (цикл ждёт этот вызов, StopAsync — цикл).
            await store.FlushOutcomeStatsAsync(rows, ct).WaitAsync(ct);
        }
        catch (Exception e)
        {
            stats.Restore(rows);
            if (ct.IsCancellationRequested)
                return;
            metrics.StorageError(StorageOperations.StatsFlush);   // каждая ошибка, даже когда лог пропущен ограничителем
            if (_throttle.TryEnter())
                Log.OutcomeStatsFlushFailed(logger, e);
        }
    }

    private async Task LoopAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            try
            {
                var pause = Task.Delay(interval, clock.Time, _stop.Token);
                Interlocked.Increment(ref _idleWaits);
                await pause;
            }
            catch (OperationCanceledException)
            {
                return;
            }
            await FlushOnceAsync(_abort.Token);
        }
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 4200, EventName = nameof(OutcomeStatsFlushFailed), Level = LogLevel.Warning,
            Message = "PgBolt: flushing outcome statistics failed; values are kept for the next flush.")]
        internal static partial void OutcomeStatsFlushFailed(ILogger logger, Exception e);
    }
}
