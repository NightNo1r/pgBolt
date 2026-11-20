using Microsoft.Extensions.Logging;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Promoter, sweeper и обслуживание хранилища процесса. Promoter и sweeper выполняют все поды (SKIP LOCKED); задачи
/// обслуживания — только под, захвативший их в maintenance_tasks. Тикер recurring, синхронизация code-first и resolver
/// workflow. Цикл stats работает на любом поде с серверами: задача stats только на захватившем её поде, затем чтение
/// кластерного снимка для gauges.
/// </summary>
internal sealed partial class ProcessingMaintenance(IServerStore store, IWakeupSignal wakeup, PgBoltProcessingOptions options,
    IPgBoltMaintenance maintenance, PgBoltOptions core, MonoClock clock, ILogger logger, PgBoltMetrics metrics, ClusterQueueStats cluster,
    ClusterTableStats tables, RecurringDeclarations? recurring = null, ICompletionSignal? completions = null)
{
    internal const int BatchSize = 1000;
    internal static readonly TimeSpan HousekeepingRetryDelay = TimeSpan.FromMinutes(1);
    private const double IntervalJitter = 0.2;

    internal const int ResolverBatchSize = 500;
    internal const int ResolverRowBudget = 5000;
    internal const int CancelMembersPortion = 1000;
    internal const int CancelMembersMaxTransactions = 100;
    internal static readonly TimeSpan ResolverLagCheckInterval = TimeSpan.FromMinutes(1);

    internal const int StatsTrimBatchSize = 10_000;
    internal const double StatsClaimFactor = 0.8;
    internal static readonly TimeSpan StatsErrorLogPeriod = TimeSpan.FromMinutes(1);

    private readonly LogThrottle _statsErrors = new(clock, StatsErrorLogPeriod);

    private readonly AsyncAutoResetEvent _resolverWake = new();
    private IDisposable? _completionSubscription;
    private readonly LogThrottle _lagCheck = new(clock, ResolverLagCheckInterval);

    private readonly CancellationTokenSource _stop = new();
    private Task _loops = Task.CompletedTask;
    private int _idleWaits;

    /// <summary>Источник U[0, 1) для jitter интервалов (±20 %); тесты подменяют.</summary>
    internal Func<double> NextRandom { get; set; } = Random.Shared.NextDouble;

    /// <summary>Для тестов: сколько раз циклы встали на паузу (таймер паузы уже взведён).</summary>
    internal int IdleWaits => Volatile.Read(ref _idleWaits);

    internal const int SchedulerBatchSize = 100;
    internal const int SchedulerMaxFires = 1000;

    public void Start()
    {
        var loops = new List<Task>();
        if (options.RunMaintenance)
        {
            loops.Add(Task.Run(() => LoopAsync("promoter", PromoteOnceAsync, options.PromoterInterval)));
            loops.Add(Task.Run(() => LoopAsync("sweeper", SweepOnceAsync, options.SweeperInterval)));
            loops.Add(Task.Run(() => LoopAsync("housekeeping", HousekeepingOnceAsync, options.MaintenancePollInterval)));
            _completionSubscription = completions?.Subscribe(_resolverWake.Set);
            loops.Add(Task.Run(() => LoopAsync("resolver", ResolveOnceAsync, options.ResolverInterval, _resolverWake,
                ResolveOnTimerAsync)));
        }
        loops.Add(Task.Run(() => LoopAsync("stats", StatsOnceAsync, options.StatsSnapshotInterval, errorLog: _statsErrors)));
        if (options.RunScheduler)
            loops.Add(Task.Run(() => LoopAsync("scheduler", TickOnceAsync, options.SchedulerInterval)));
        if (recurring is { HasSync: true })
            loops.Add(Task.Run(() => LoopAsync("recurring-sync", SyncOnceAsync, options.RecurringSyncInterval)));
        _loops = Task.WhenAll(loops);
    }

    public Task StopAsync()
    {
        _completionSubscription?.Dispose();
        _stop.Cancel();
        return _loops;
    }

    /// <summary>Хранилище обрабатывает пачки до неполной. Каждая очередь с новыми задачами получает один сигнал.</summary>
    internal async Task<int> PromoteOnceAsync(CancellationToken ct)
    {
        var result = await store.PromoteDueAsync(BatchSize, ct);
        foreach (var queue in result.Queues)
        {
            wakeup.Signal(queue);
        }

        return result.Promoted;
    }

    /// <summary>SweepExpiredLeasesAsync(1000), пока пакет полный.</summary>
    internal async Task<int> SweepOnceAsync(CancellationToken ct)
    {
        var total = 0;
        while (true)
        {
            var swept = await store.SweepExpiredLeasesAsync(BatchSize, ct);
            total += swept;
            if (swept < BatchSize)
                return total;
        }
    }

    /// <summary>
    /// Итерация (первая и по сигналу; по таймеру — после проверки ResolveOnTimerAsync) — учёт (CountCompletionsAsync), обход (ResolveAsync),
    /// CancelMembers; следующая — сразу, если захвачен полный пакет, созданы новые сигналы, исчерпан бюджет строк или выполнена
    /// порция отмены: многошаговый путь не добавляет интервалов.
    /// Этап без работы пропускается: обход — если учёт не передал сигналов и в его снимке нет готовых к обходу; CancelMembers — если
    /// в снимке учёта нет готовых kind = 6 и обход не создал сигналов. Сигналы, закоммиченные другими подами после снимка учёта, берут
    /// их собственные обход / CancelMembers (финализация будит resolver своего процесса) или следующая итерация — по таймеру, не
    /// позже ResolverInterval.
    /// </summary>
    internal async Task<int> ResolveOnceAsync(CancellationToken ct)
    {
        var total = 0;
        while (true)
        {
            var counted = await store.CountCompletionsAsync(ResolverBatchSize, ct);
            foreach (var batch in counted.MissingBatches)
                Log.BatchCompletionNotCounted(logger, batch);
            var walked = counted.Walking > 0 || counted.WalkReady
                ? await store.ResolveAsync(ResolverBatchSize, ResolverRowBudget, options.ResolverInterval, ct)
                : ResolveResult.Empty;
            foreach (var queue in walked.Queues)
                wakeup.Signal(queue);
            var cancelled = counted.CancelReady || walked.NewSignals > 0
                ? await store.CancelMembersAsync(CancelMembersPortion, ResolverRowBudget, CancelMembersMaxTransactions,
                    options.ResolverInterval, ct)
                : CancelMembersResult.Empty;
            total += counted.Captured + walked.Captured + cancelled.Portions;
            await CheckResolverLagAsync(ct);

            var again = counted.Captured == ResolverBatchSize || counted.Finished > 0
                        || walked.Captured == ResolverBatchSize || walked.NewSignals > 0 || walked.BudgetExhausted
                        || cancelled.Portions > 0;
            if (!again)
                return total;
        }
    }

    /// <summary>
    /// Итерация, разбуженная таймером (не сигналом ICompletionSignal): сначала один запрос — есть ли сигнал, который взял бы
    /// захват; нет — без транзакций учёта, обхода и CancelMembers (в простое — один запрос на ResolverInterval). Проверка лага
    /// выполняется и тогда: отложенные (retry_after) сигналы — её случай. Сигнал и немедленный повтор знают о работе и не проверяют.
    /// </summary>
    internal async Task<int> ResolveOnTimerAsync(CancellationToken ct)
    {
        if (await store.HasReadyCompletionsAsync(ct))
            return await ResolveOnceAsync(ct);
        await CheckResolverLagAsync(ct);
        return 0;
    }

    /// <summary>Warning, если старейший сигнал старше 10 × ResolverInterval; проверка — не чаще раза в минуту на под.</summary>
    private async Task CheckResolverLagAsync(CancellationToken ct)
    {
        if (!_lagCheck.TryEnter())
            return;
        if (await store.OldestCompletionAgeAsync(ct) is { } age && age > 10 * options.ResolverInterval)
            Log.ResolverLagging(logger, age, options.ResolverInterval);
    }

    /// <summary>Транзакции тика подряд, пока пакет полный; очереди поставленных задач получают сигнал.</summary>
    internal async Task<int> TickOnceAsync(CancellationToken ct)
    {
        var total = 0;
        while (true)
        {
            var result = await store.TickRecurringAsync(SchedulerBatchSize, SchedulerMaxFires, options.EffectiveMisfireThreshold, ct);
            total += result.Fired;
            metrics.RecurringFired(result.Fired);
            foreach (var queue in result.Queues)
                wakeup.Signal(queue);
            LogTick(result);
            if (!result.Full)
                return total;
        }
    }

    /// <summary>Периодическая синхронизация набора владельца (сходимость после rolling deploy).</summary>
    internal async Task<int> SyncOnceAsync(CancellationToken ct)
    {
        var declarations = recurring ?? throw new InvalidOperationException("PgBolt: recurring declarations are not registered.");
        var result = await store.SyncRecurringAsync(declarations.Owner, declarations.Definitions, logger, ct);
        RecurringSyncLog.Write(logger, declarations.Owner, result);
        return result.Writes;
    }

    /// <summary>Ошибка строки — Error (не чаще раза в минуту: wake_at), пропуск misfire и конец срабатываний — Warning, наложение и ожидание — Debug.</summary>
    private void LogTick(RecurringTickResult result)
    {
        foreach (var error in result.Errors)
            Log.RecurringFireFailed(logger, error.Id, error.Error, RecurringPlanner.RowErrorDelay);
        foreach (var missed in result.Missed)
            Log.RecurringMisfireSkipped(logger, missed.Id, missed.FromUtc, missed.ToUtc);
        foreach (var id in result.OverlapSkipped)
            Log.RecurringOverlapSkipped(logger, id);
        foreach (var id in result.Waiting)
            Log.RecurringWaitingForPrevious(logger, id);
        foreach (var id in result.Exhausted)
            Log.RecurringExhausted(logger, id);
    }

    /// <summary>
    /// Задачи partitions, servers, retention по порядку; выполняются только захваченные. Исключение захвата или Complete
    /// прерывает итерацию (его логирует цикл). Возвращает число выполненных задач.
    /// </summary>
    internal async Task<int> HousekeepingOnceAsync(CancellationToken ct)
    {
        var ran = 0;
        if (await RunTaskAsync("partitions", options.PartitionsInterval, EnsurePartitionsAsync, HousekeepingRetryDelay, ct))
            ran++;
        if (await RunTaskAsync("servers", options.ServersCleanupInterval, RemoveDeadServersAsync, HousekeepingRetryDelay, ct))
            ran++;
        if (await RunTaskAsync("retention", options.RetentionInterval, ApplyRetentionAsync, errorRetryDelay: null, ct))
            ran++;
        return ran;
    }

    /// <summary>
    /// При RunMaintenance — захват на 0,8 × StatsSnapshotInterval (проход раз в 0,8–1,2 интервала при любом числе
    /// подов), снимок queue_stats, затем retention stats_minutes порциями. Ошибка — в лог не чаще раза в минуту.
    /// Затем на любом поде — чтение снимка очередей и статистики таблиц для кластерных gauges: владелец публикует тем же путём,
    /// что и остальные поды.
    /// Исключение захвата или Complete чтение не пропускает и уходит в цикл после него; отмена остановкой — без чтения.
    /// </summary>
    internal async Task<int> StatsOnceAsync(CancellationToken ct)
    {
        var ran = 0;
        try
        {
            if (options.RunMaintenance && await RunTaskAsync("stats", options.StatsSnapshotInterval * StatsClaimFactor,
                    SnapshotStatsAsync, errorRetryDelay: null, ct, _statsErrors))
                ran = 1;
        }
        finally
        {
            if (!ct.IsCancellationRequested)
            {
                await PublishQueueStatsAsync(ct);
                await PublishTableStatsAsync(ct);
            }
        }
        return ran;
    }

    /// <summary>Ошибка чтения оставляет прежний снимок: он стареет по своему моменту и скрывается, а не застывает.</summary>
    private Task PublishQueueStatsAsync(CancellationToken ct) =>
        ReadStatsAsync("queue", StorageOperations.StatsRead, Log.QueueStatsReadFailed,
            async () => cluster.Publish(await store.ReadQueueStatsAsync(ct)), ct);

    /// <summary>Ошибка чтения оставляет прежние значения: они стареют и скрываются, а не застывают.</summary>
    private Task PublishTableStatsAsync(CancellationToken ct) =>
        ReadStatsAsync("table", StorageOperations.TableStatsRead, Log.TableStatsReadFailed,
            async () => tables.Update(await store.ReadTableStatsAsync(ct)), ct);

    /// <summary>
    /// Чтение для gauges: отмена остановкой — <see cref="OperationCanceledException"/> без метрики и лога, любая другая ошибка —
    /// метрика и (не чаще раза в минуту) запись в лог.
    /// </summary>
    private async Task ReadStatsAsync(string what, string operation, Action<ILogger, Exception> logFailure, Func<Task> read,
        CancellationToken ct)
    {
        try
        {
            await read();
        }
        catch (Exception e) when (ct.IsCancellationRequested)
        {
            throw new OperationCanceledException($"PgBolt: reading {what} statistics was cancelled by stop.", e, ct);
        }
        catch (Exception e)
        {
            metrics.StorageError(operation);
            if (_statsErrors.TryEnter())
                logFailure(logger, e);
        }
    }

    private async Task<TaskOutcome> SnapshotStatsAsync(CancellationToken ct)
    {
        await store.SnapshotQueueStatsAsync(ct);
        while (await store.TrimStatsAsync(options.StatsRetention, StatsTrimBatchSize, ct) == StatsTrimBatchSize)
        {
        }
        return TaskOutcome.Ok;
    }

    /// <param name="errorRetryDelay">Ускоренный повтор после исключения; null — не раньше следующего интервала (retention).</param>
    /// <param name="errorLog">Ограничитель записи ошибки в лог; null — каждая.</param>
    private async Task<bool> RunTaskAsync(string name, TimeSpan interval,
        Func<CancellationToken, Task<TaskOutcome>> run, TimeSpan? errorRetryDelay, CancellationToken ct, LogThrottle? errorLog = null)
    {
        if (await store.TryClaimMaintenanceAsync(name, interval, ct) is not { } claimedAt)
            return false;
        var started = clock.Now;
        TaskOutcome outcome;
        string result;
        try
        {
            outcome = await run(ct);
            result = outcome.Error is null ? MaintenanceResults.Ok : MaintenanceResults.Deferred;
        }
        catch (Exception e) when (ct.IsCancellationRequested)
        {
            // Остановка: любое исключение отменённой операции (Npgsql при отмене может бросить и не OCE) — отмена,
            // без ошибки в логе и без Complete; цикл завершится по OperationCanceledException.
            throw new OperationCanceledException($"PgBolt: maintenance task {name} was cancelled by stop.", e, ct);
        }
        catch (Exception e)
        {
            // В том числе OperationCanceledException не от ct (например, таймаут операции) — ошибка задачи.
            if (errorLog?.TryEnter() ?? true)
                Log.MaintenanceTaskFailed(logger, e, name);
            outcome = new TaskOutcome(e.Message, errorRetryDelay);
            result = MaintenanceResults.Error;
        }
        metrics.MaintenanceRun(name, result, clock.Elapsed(started));
        await store.CompleteMaintenanceAsync(name, claimedAt, outcome.Error, outcome.RetryDelay, ct);
        return true;
    }

    private async Task<TaskOutcome> EnsurePartitionsAsync(CancellationToken ct)
    {
        var result = await maintenance.EnsurePartitionsAsync(null, ct);
        if (!result.TimedOut)
            return TaskOutcome.Ok;
        if (result.TodayPresent)
            Log.FuturePartitionsLockTimeout(logger, HousekeepingRetryDelay);
        else
            Log.TodayPartitionsMissing(logger, HousekeepingRetryDelay);
        return new TaskOutcome("lock_timeout", HousekeepingRetryDelay);
    }

    private async Task<TaskOutcome> RemoveDeadServersAsync(CancellationToken ct)
    {
        var removed = await maintenance.RemoveDeadServersAsync(options.DeadServerTimeout, ct);
        if (removed > 0)
            Log.DeadServersRemoved(logger, removed, options.DeadServerTimeout);
        return TaskOutcome.Ok;
    }

    /// <summary>LockTimeout/HoldTimeout и прочие Deferred — не ошибка задачи: только логи и счётчик отложенных партиций.</summary>
    private async Task<TaskOutcome> ApplyRetentionAsync(CancellationToken ct)
    {
        RetentionReport.Write(logger, await maintenance.ApplyRetentionAsync(ct), core, metrics);
        return TaskOutcome.Ok;
    }

    /// <param name="onTimer">Итерация после паузы, закончившейся таймаутом (не wake); null — once. Первая итерация — once.</param>
    /// <param name="errorLog">Ограничитель записи ошибки итерации в лог; null — каждая.</param>
    private async Task LoopAsync(string name, Func<CancellationToken, Task<int>> once, TimeSpan interval,
        AsyncAutoResetEvent? wake = null, Func<CancellationToken, Task<int>>? onTimer = null, LogThrottle? errorLog = null)
    {
        var ct = _stop.Token;
        var timedOut = false;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await (timedOut && onTimer is not null ? onTimer(ct) : once(ct));
            }
            catch (Exception) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                metrics.StorageError(name);
                if (errorLog?.TryEnter() ?? true)
                    Log.MaintenanceLoopIterationFailed(logger, e, name);
            }

            try
            {
                var delay = interval * (1 + IntervalJitter * (2 * NextRandom() - 1));
                var pause = wake is null ? Task.Delay(delay, clock.Time, ct) : wake.WaitAsync(delay, clock.Time, ct);
                Interlocked.Increment(ref _idleWaits);
                await pause;
                timedOut = pause is not Task<bool> { Result: true };
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>Результат задачи обслуживания для CompleteMaintenanceAsync: null/null — успех.</summary>
    private readonly record struct TaskOutcome(string? Error, TimeSpan? RetryDelay)
    {
        public static TaskOutcome Ok => default;
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 5000, EventName = nameof(BatchCompletionNotCounted), Level = LogLevel.Debug,
            Message = "PgBolt: completion of a member of batch {BatchId} is not counted: the batch row was removed by retention.")]
        internal static partial void BatchCompletionNotCounted(ILogger logger, Guid batchId);

        [LoggerMessage(EventId = 5001, EventName = nameof(ResolverLagging), Level = LogLevel.Warning,
            Message = "PgBolt: the oldest workflow completion signal is {Age} old (ResolverInterval {Interval}); continuations are delayed. " +
                "Check that a process with RunMaintenance = true is running and that no long user transaction holds workflow rows (After).")]
        internal static partial void ResolverLagging(ILogger logger, TimeSpan age, TimeSpan interval);

        [LoggerMessage(EventId = 5002, EventName = nameof(RecurringFireFailed), Level = LogLevel.Error,
            Message = "PgBolt: recurring job {Id} was not fired: {Error}; retrying in {Delay}.")]
        internal static partial void RecurringFireFailed(ILogger logger, string id, string error, TimeSpan delay);

        [LoggerMessage(EventId = 5003, EventName = nameof(RecurringMisfireSkipped), Level = LogLevel.Warning,
            Message = "PgBolt: recurring job {Id} skipped fires from {From:O} to {To:O} by its misfire policy.")]
        internal static partial void RecurringMisfireSkipped(ILogger logger, string id, DateTime @from, DateTime to);

        [LoggerMessage(EventId = 5004, EventName = nameof(RecurringOverlapSkipped), Level = LogLevel.Debug,
            Message = "PgBolt: recurring job {Id} fire skipped: previous run is still active.")]
        internal static partial void RecurringOverlapSkipped(ILogger logger, string id);

        [LoggerMessage(EventId = 5005, EventName = nameof(RecurringWaitingForPrevious), Level = LogLevel.Debug,
            Message = "PgBolt: recurring job {Id} waits for its previous run (catch-up).")]
        internal static partial void RecurringWaitingForPrevious(ILogger logger, string id);

        [LoggerMessage(EventId = 5006, EventName = nameof(RecurringExhausted), Level = LogLevel.Warning,
            Message = "PgBolt: recurring job {Id} has no future occurrences and will not fire again.")]
        internal static partial void RecurringExhausted(ILogger logger, string id);

        [LoggerMessage(EventId = 5007, EventName = nameof(MaintenanceTaskFailed), Level = LogLevel.Error,
            Message = "PgBolt: maintenance task {Task} failed.")]
        internal static partial void MaintenanceTaskFailed(ILogger logger, Exception e, string task);

        [LoggerMessage(EventId = 5008, EventName = nameof(FuturePartitionsLockTimeout), Level = LogLevel.Warning,
            Message = "PgBolt: future partitions were not created (lock_timeout); retrying in {Delay}.")]
        internal static partial void FuturePartitionsLockTimeout(ILogger logger, TimeSpan delay);

        [LoggerMessage(EventId = 5009, EventName = nameof(TodayPartitionsMissing), Level = LogLevel.Error,
            Message = "PgBolt: today's partitions are missing and could not be created (lock_timeout); enqueue fails until they exist. Retrying in {Delay}.")]
        internal static partial void TodayPartitionsMissing(ILogger logger, TimeSpan delay);

        [LoggerMessage(EventId = 5010, EventName = nameof(DeadServersRemoved), Level = LogLevel.Information,
            Message = "PgBolt: removed {Count} servers rows without heartbeat for {Timeout}.")]
        internal static partial void DeadServersRemoved(ILogger logger, int count, TimeSpan timeout);

        [LoggerMessage(EventId = 5011, EventName = nameof(MaintenanceLoopIterationFailed), Level = LogLevel.Error,
            Message = "PgBolt: {Loop} iteration failed; retrying after the next interval.")]
        internal static partial void MaintenanceLoopIterationFailed(ILogger logger, Exception e, string loop);

        [LoggerMessage(EventId = 5012, EventName = nameof(QueueStatsReadFailed), Level = LogLevel.Warning,
            Message = "PgBolt: reading queue statistics failed; the previous snapshot is kept.")]
        internal static partial void QueueStatsReadFailed(ILogger logger, Exception e);

        [LoggerMessage(EventId = 5013, EventName = nameof(TableStatsReadFailed), Level = LogLevel.Warning,
            Message = "PgBolt: reading table statistics failed; the previous values are kept.")]
        internal static partial void TableStatsReadFailed(ILogger logger, Exception e);
    }
}
