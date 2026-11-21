using Microsoft.Extensions.Logging;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Runs the process's promoter, sweeper, store maintenance, recurring ticker, code-first synchronization and workflow
/// resolver. Every pod runs the promoter and sweeper using SKIP LOCKED. Only the pod that claims a task in
/// maintenance_tasks runs it. The stats loop runs on every pod with servers: the claiming pod runs the stats task, then
/// each pod reads the cluster snapshot for its gauges.
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

    /// <summary>Uniform random source on [0, 1) for ±20% interval jitter. Tests can replace it.</summary>
    internal Func<double> NextRandom { get; set; } = Random.Shared.NextDouble;

    /// <summary>For tests, counts loop pauses that have started with their timers armed.</summary>
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

    /// <summary>
    /// The store processes batches until it returns a partial batch. Each queue with new jobs receives one signal.
    /// </summary>
    internal async Task<int> PromoteOnceAsync(CancellationToken ct)
    {
        var result = await store.PromoteDueAsync(BatchSize, ct);
        foreach (var queue in result.Queues)
        {
            wakeup.Signal(queue);
        }

        return result.Promoted;
    }

    /// <summary>Calls SweepExpiredLeasesAsync(1000) until a batch is smaller than the limit.</summary>
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
    /// On the first iteration or a signal (after ResolveOnTimerAsync checks for work on a timer tick), runs accounting
    /// (CountCompletionsAsync), traversal (ResolveAsync) and CancelMembers. Repeats immediately on a full claimed batch,
    /// new signals, an exhausted row budget or a completed cancellation chunk, avoiding interval delays between steps.
    /// Skips traversal when accounting forwarded no signals and its snapshot contained none ready for traversal. Skips
    /// CancelMembers when the snapshot had no ready kind = 6 signals and traversal created none. Signals committed by
    /// other pods after the snapshot are handled by their traversal or CancelMembers (finalization wakes the local
    /// resolver) or by the next timer iteration within ResolverInterval.
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
    /// On a timer wakeup without an ICompletionSignal, checks for a claimable signal with one query. If none exists,
    /// skips the accounting, traversal and CancelMembers transactions, leaving one query per idle ResolverInterval. Still
    /// checks lag to cover deferred retry_after signals. Signal wakeups and immediate retries already know work is
    /// available and skip the query.
    /// </summary>
    internal async Task<int> ResolveOnTimerAsync(CancellationToken ct)
    {
        if (await store.HasReadyCompletionsAsync(ct))
            return await ResolveOnceAsync(ct);
        await CheckResolverLagAsync(ct);
        return 0;
    }

    /// <summary>Warns when the oldest signal exceeds 10 × ResolverInterval. Checks at most once per minute per pod.</summary>
    private async Task CheckResolverLagAsync(CancellationToken ct)
    {
        if (!_lagCheck.TryEnter())
            return;
        if (await store.OldestCompletionAgeAsync(ct) is { } age && age > 10 * options.ResolverInterval)
            Log.ResolverLagging(logger, age, options.ResolverInterval);
    }

    /// <summary>Runs consecutive tick transactions while the batch is full. Signals queues that received jobs.</summary>
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

    /// <summary>Periodically synchronizes the owner's set so it converges after a rolling deployment.</summary>
    internal async Task<int> SyncOnceAsync(CancellationToken ct)
    {
        var declarations = recurring ?? throw new InvalidOperationException("PgBolt: recurring declarations are not registered.");
        var result = await store.SyncRecurringAsync(declarations.Owner, declarations.Definitions, logger, ct);
        RecurringSyncLog.Write(logger, declarations.Owner, result);
        return result.Writes;
    }

    /// <summary>
    /// Logs row errors at Error (at most once per minute via wake_at), skipped misfires and exhausted schedules at
    /// Warning, and overlaps and waits at Debug.
    /// </summary>
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
    /// Processes partitions, servers and retention in order, running only claimed tasks. Exceptions during claim or
    /// Complete end the iteration and are logged by the loop. Returns the number of tasks run.
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
    /// With RunMaintenance enabled, claims the stats task for 0.8 × StatsSnapshotInterval. The pass runs every 0.8–1.2
    /// intervals regardless of pod count. Captures queue_stats, then applies stats_minutes retention in chunks. Logs
    /// errors at most once per minute. Every pod then reads the queue snapshot and table statistics for cluster gauges,
    /// including the owner. Claim or Complete exceptions propagate to the loop after the read. Shutdown cancellation
    /// skips the read.
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

    /// <summary>A failed read preserves the previous snapshot. Its original timestamp lets it expire and disappear.</summary>
    private Task PublishQueueStatsAsync(CancellationToken ct) =>
        ReadStatsAsync("queue", StorageOperations.StatsRead, Log.QueueStatsReadFailed,
            async () => cluster.Publish(await store.ReadQueueStatsAsync(ct)), ct);

    /// <summary>A failed read preserves the previous values until they expire and disappear.</summary>
    private Task PublishTableStatsAsync(CancellationToken ct) =>
        ReadStatsAsync("table", StorageOperations.TableStatsRead, Log.TableStatsReadFailed,
            async () => tables.Update(await store.ReadTableStatsAsync(ct)), ct);

    /// <summary>
    /// Reads gauge data. Shutdown cancellation propagates <see cref="OperationCanceledException"/> without recording a
    /// metric or log. Other failures record a metric and log at most once per minute.
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

    /// <param name="name">Name of the maintenance task to claim.</param>
    /// <param name="interval">Interval passed to the store when claiming the task.</param>
    /// <param name="run">Operation to execute after a successful claim.</param>
    /// <param name="errorRetryDelay">
    /// Delay for an earlier retry after an exception. Null waits until the next interval (retention).
    /// </param>
    /// <param name="ct">Cancels the claim, execution and completion during shutdown.</param>
    /// <param name="errorLog">Error log rate limiter. Null logs every error.</param>
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

    /// <summary>
    /// LockTimeout, HoldTimeout and other Deferred results are expected maintenance outcomes. They produce logs and
    /// increment the deferred partition count.
    /// </summary>
    private async Task<TaskOutcome> ApplyRetentionAsync(CancellationToken ct)
    {
        RetentionReport.Write(logger, await maintenance.ApplyRetentionAsync(ct), core, metrics);
        return TaskOutcome.Ok;
    }

    /// <param name="name">Loop name used for logging and storage error metrics.</param>
    /// <param name="once">Operation run on startup or a wake signal.</param>
    /// <param name="interval">Base delay between iterations before jitter.</param>
    /// <param name="wake">Optional signal that wakes the loop before the next timer tick.</param>
    /// <param name="onTimer">
    /// Iteration to run after a timeout rather than a wake signal. Null uses once. The first iteration always uses once.
    /// </param>
    /// <param name="errorLog">Iteration error log rate limiter. Null logs every error.</param>
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

    /// <summary>Maintenance task result for CompleteMaintenanceAsync. A null/null pair indicates success.</summary>
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
