using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Отправка исходов: ack — пакетами по размеру, таймеру или при простое у кластерного лимита, остальное — поштучно и параллельно.
/// Каждая попытка начинается CAS OutcomePending → OutcomeInFlight; ошибка — обратно в OutcomePending и повтор
/// с backoff 100 мс → 5 с в пределах бюджета LeaseDuration от первой попытки. Запись покидает таблицу только
/// после завершения отправки (успех, LeaseLost, бюджет, прерывание остановкой). После успеха или LeaseLost —
/// onLeaseFreed с очередью выполнения: statement исхода вернулся, место в кластерном лимите видно следующему dequeue.
/// </summary>
internal sealed partial class CompletionSink
{
    private static readonly TimeSpan MinBackoff = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(5);

    private readonly Guid _serverId;
    private readonly IServerStore _store;
    private readonly ExecutionTable _table;
    private readonly PgBoltServerOptions _options;
    private readonly MonoClock _clock;
    private readonly ILogger _logger;
    private readonly OutcomeStats _stats;
    private readonly PgBoltMetrics _metrics;
    private readonly Action<string>? _onLeaseFreed;
    private readonly Func<string, bool>? _waitingForLease;
    private readonly Lock _lock = new();
    private readonly ConcurrentDictionary<Task, byte> _sends = new();
    private readonly CancellationTokenSource _abort = new();
    private List<Execution> _ackBuffer = [];
    private ITimer? _ackTimer;
    private bool _stopped;
    private int _backoffWaits;

    public CompletionSink(Guid serverId, IServerStore store, ExecutionTable table, PgBoltServerOptions options,
        MonoClock clock, ILogger logger, OutcomeStats stats, PgBoltMetrics metrics, Action<string>? onLeaseFreed = null,
        Func<string, bool>? waitingForLease = null)
    {
        _serverId = serverId;
        _store = store;
        _table = table;
        _options = options;
        _clock = clock;
        _logger = logger;
        _stats = stats;
        _metrics = metrics;
        _onLeaseFreed = onLeaseFreed;
        _waitingForLease = waitingForLease;
    }

    /// <summary>Для тестов: сколько раз отправка встала на ожидание backoff (таймер уже взведён).</summary>
    internal int BackoffWaits => Volatile.Read(ref _backoffWaits);

    public void Start()
    {
        lock (_lock)
        {
            _ackTimer ??= _clock.Time.CreateTimer(static s => ((CompletionSink)s!).OnAckTimer(), this,
                Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            if (_ackBuffer.Count > 0)
                _ackTimer.Change(_options.AckFlushInterval, Timeout.InfiniteTimeSpan);
        }
    }

    public bool Complete(Execution execution, JobOutcome outcome)
    {
        execution.Outcome = outcome;
        lock (_lock)
        {
            if (_stopped)
            {
                if (execution.TryTransition(ExecutionState.Running, ExecutionState.Removed))
                    Drop(execution, DropReasons.Stopped);
                Log.OutcomeDiscardedSinkStopped(_logger, outcome.GetType().Name, execution.Job.Id);
                return false;
            }
            if (!execution.TryTransition(ExecutionState.Running, ExecutionState.OutcomePending))
            {
                Log.OutcomeDiscardedNotRunning(_logger, outcome.GetType().Name, execution.Job.Id, execution.State);
                return false;
            }
            EnqueueLocked(execution);
            return true;
        }
    }

    public void EnqueueRegistered(Execution execution)
    {
        lock (_lock)
        {
            if (_stopped)
            {
                // Как в Complete: после финального сброса отправку никто не ждал бы. Lease истечёт, задачу заберёт sweeper.
                if (execution.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed))
                    Drop(execution, DropReasons.Stopped);
                Log.OutcomeDiscardedSinkStopped(_logger, execution.Outcome?.GetType().Name, execution.Job.Id);
                return;
            }
            EnqueueLocked(execution);
        }
    }

    public async Task StopAsync(CancellationToken ct)
    {
        using var abort = ct.Register(static s => ((CancellationTokenSource)s!).Cancel(), _abort);
        while (true)
        {
            Task[] pending;
            lock (_lock)
            {
                FlushAckLocked();
                pending = [.. _sends.Keys];
                if (pending.Length == 0)
                {
                    _stopped = true;
                    _ackTimer?.Dispose();
                    return;
                }
            }
            try
            {
                await Task.WhenAll(pending).WaitAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Отправки прерываются через _abort и сами снимают свои записи; дольше не ждём.
                await _abort.CancelAsync();
                Log.SinkStopCancelled(_logger, pending.Length);
                lock (_lock)
                {
                    DiscardAckBufferLocked();
                    _stopped = true;
                    _ackTimer?.Dispose();
                }
                return;
            }
        }
    }

    private void EnqueueLocked(Execution execution)
    {
        if (execution.Outcome is JobOutcome.Ack)
        {
            _ackBuffer.Add(execution);
            // У исчерпанного лимита ожидание таймера оставило бы простаивать уже закончившую работу очередь.
            if (_ackBuffer.Count >= _options.AckBatchSize || _waitingForLease?.Invoke(execution.Queue) == true)
            {
                FlushAckLocked();
            }
            else if (_ackBuffer.Count == 1)
            {
                _ackTimer?.Change(_options.AckFlushInterval, Timeout.InfiniteTimeSpan);
            }
            return;
        }
        StartSend(() => SendSingleAsync(execution));
        // Последний локальный слот могла освободить отмена или ошибка, а в буфере ещё есть успехи этой очереди.
        FlushIdleAcksLocked(execution.Queue);
    }

    /// <summary>Выдача могла увидеть занятый кластерный лимит уже после буферизации ACK.</summary>
    internal void RecheckBufferedAcks(string queue)
    {
        lock (_lock)
        {
            FlushIdleAcksLocked(queue);
        }
    }

    private void FlushIdleAcksLocked(string queue)
    {
        if (_ackBuffer.Count == 0 || _waitingForLease?.Invoke(queue) != true)
        {
            return;
        }

        // Коммит пачки освободит место, только если она содержит задачу этой очереди.
        foreach (var job in _ackBuffer)
        {
            if (job.Queue == queue)
            {
                FlushAckLocked();
                return;
            }
        }
    }

    private void OnAckTimer()
    {
        lock (_lock)
            FlushAckLocked();
    }

    private void FlushAckLocked()
    {
        if (_ackBuffer.Count == 0)
            return;
        var batch = _ackBuffer;
        _ackBuffer = new List<Execution>(Math.Min(_options.AckBatchSize, 1024));
        _ackTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        StartSend(() => SendAckBatchAsync(batch));
    }

    /// <summary>
    /// Отменённая остановка: ack, принятые после последнего сброса, не отправляются (отправка уже прервана) — отбрасываются
    /// с логом, как исход, пришедший после остановки, и покидают таблицу; lease истечёт, задачу заберёт sweeper.
    /// </summary>
    private void DiscardAckBufferLocked()
    {
        foreach (var execution in _ackBuffer)
        {
            if (!execution.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed))
                continue;                                  // уже снята heartbeat'ом (lease потерян)
            Drop(execution, DropReasons.Stopped);
            Log.AckDiscardedStopCancelled(_logger, nameof(JobOutcome.Ack), execution.Job.Id);
        }
        _ackBuffer.Clear();
        _ackTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    private void StartSend(Func<Task> send)
    {
        var task = Task.Run(send);
        _sends.TryAdd(task, 0);
        _ = task.ContinueWith(static (t, s) => ((ConcurrentDictionary<Task, byte>)s!).TryRemove(t, out _), _sends,
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private async Task SendAckBatchAsync(List<Execution> batch)
    {
        var backoff = MinBackoff;
        var ambiguous = false;
        var pending = batch;
        while (true)
        {
            var inFlight = BeginAttempt(pending);
            if (inFlight.Count == 0)
                return;
            IReadOnlyList<Guid> ackedIds;
            try
            {
                var leases = new LeaseRef[inFlight.Count];
                for (var i = 0; i < leases.Length; i++)
                    leases[i] = inFlight[i].Lease;
                using var budget = new CancellationTokenSource(RemainingBudget(inFlight), _clock.Time);
                using var attempt = CancellationTokenSource.CreateLinkedTokenSource(_abort.Token, budget.Token);
                ackedIds = await _store.AckAsync(_serverId, leases, attempt.Token).WaitAsync(attempt.Token);
            }
            catch (Exception error)
            {
                if (!_abort.IsCancellationRequested)
                    _metrics.StorageError(StorageOperations.Ack);
                ambiguous = true;
                pending = await AfterFailureAsync(inFlight, error, backoff);
                if (pending.Count == 0)
                    return;
                backoff = Next(backoff);
                continue;
            }

            // Ответ БД получен: дальше сбой не относится к хранилищу и не должен повторять отправку.
            var ids = ackedIds.ToHashSet();
            CountAcked(inFlight, ids);
            var acked = ackedIds.Count;
            var lost = acked < inFlight.Count;
            if (lost && !ambiguous)
            {
                // Пакет собран из выполнений разных очередей: потерянные учитываются по очереди каждого.
                foreach (var (queue, count) in inFlight.Where(e => !ids.Contains(e.Job.Id)).CountBy(e => e.Queue))
                    _metrics.OutcomeDropped(queue, DropReasons.LeaseLost, count);
            }
            foreach (var execution in inFlight)
                Finish(execution);
            // Лог последним: записи уже сняты с таблицы, а сбой логгера не должен вырваться из задачи отправки.
            try
            {
                if (lost && ambiguous)
                    Log.AckPartialAfterRetry(_logger, acked, inFlight.Count);
                else if (lost)
                    Log.AckPartialLeaseLost(_logger, acked, inFlight.Count);
            }
            catch
            {
                // Ответ БД уже учтён, записи сняты; иначе исключение логгера вышло бы из задачи отправки,
                // и Task.WhenAll в StopAsync пробросил бы его.
            }
            return;
        }
    }

    private async Task SendSingleAsync(Execution execution)
    {
        var backoff = MinBackoff;
        var ambiguous = false;
        List<Execution> single = [execution];
        while (true)
        {
            if (BeginAttempt(single).Count == 0)
                return;
            bool applied;
            try
            {
                using var budget = new CancellationTokenSource(RemainingBudget(single), _clock.Time);
                using var attempt = CancellationTokenSource.CreateLinkedTokenSource(_abort.Token, budget.Token);
                applied = await SendOutcomeAsync(execution, attempt.Token);
            }
            catch (Exception error)
            {
                if (!_abort.IsCancellationRequested)
                    _metrics.StorageError(StorageOperations.Outcome);
                ambiguous = true;
                if ((await AfterFailureAsync(single, error, backoff)).Count == 0)
                    return;
                backoff = Next(backoff);
                continue;
            }

            // Ответ БД получен: дальше сбой не относится к хранилищу и не должен повторять отправку.
            if (!applied && !ambiguous)
                _metrics.OutcomeDropped(execution.Queue, DropReasons.LeaseLost);
            Finish(execution);
            // Лог последним: запись уже снята с таблицы, а сбой логгера не должен вырваться из задачи отправки.
            try
            {
                if (!applied && ambiguous)
                    Log.OutcomeLeaseNotFoundAfterRetry(_logger, execution.Outcome!.GetType().Name, execution.Job.Id);
                else if (!applied)
                    Log.OutcomeDiscardedLeaseLost(_logger, execution.Outcome!.GetType().Name, execution.Job.Id);
            }
            catch
            {
                // Ответ БД уже учтён, записи сняты; иначе исключение логгера вышло бы из задачи отправки,
                // и Task.WhenAll в StopAsync пробросил бы его.
            }
            return;
        }
    }

    /// <returns>true — исход применён; false — LeaseLost.</returns>
    /// <remarks>ct — токен попытки; WaitAsync обрывает ожидание и тогда, когда вызов store токен не соблюдает.</remarks>
    private async Task<bool> SendOutcomeAsync(Execution execution, CancellationToken ct)
    {
        var job = execution.Job;
        switch (execution.Outcome)
        {
            case JobOutcome.Retry r:
                var retried = await _store.RetryAsync(job.Id, job.LeaseId, _serverId, r.Delay, r.Kind, r.Message, r.Exception, ct).WaitAsync(ct);
                return RecordOutcome(execution, retried) != FailOutcome.LeaseLost;
            case JobOutcome.Fail f:
                var failed = await _store.FailAsync(job.Id, job.LeaseId, _serverId, f.Kind, f.Message, f.Exception, ct).WaitAsync(ct);
                return RecordOutcome(execution, failed) != FailOutcome.LeaseLost;
            case JobOutcome.ConfirmCancel c:
                return await _store.ConfirmCancelAsync(job.Id, job.LeaseId, _serverId, c.Message, ct).WaitAsync(ct) != FailOutcome.LeaseLost;
            case JobOutcome.Release r:
                return await _store.ReleaseAsync(job.Id, job.LeaseId, _serverId, r.Ran, message: null, ct).WaitAsync(ct) != ReleaseOutcome.LeaseLost;
            default:
                throw new InvalidOperationException($"Outcome {execution.Outcome?.GetType().Name ?? "null"} cannot be sent individually.");
        }
    }

    /// <summary>Учитываются только job_id, подтверждённые ответом ack, — по очереди выполнения.</summary>
    private void CountAcked(List<Execution> inFlight, HashSet<Guid> acked)
    {
        if (acked.Count == 0)
            return;
        foreach (var (queue, count) in inFlight.Where(e => acked.Contains(e.Job.Id)).CountBy(e => e.Queue))
            _stats.Record(_serverId, queue, succeeded: count);
    }

    /// <summary>retry / fail учитываются по ответу store; Cancelled и LeaseLost — нет.</summary>
    private FailOutcome RecordOutcome(Execution execution, FailOutcome answer)
    {
        if (answer == FailOutcome.Retried)
            _stats.Record(_serverId, execution.Queue, retried: 1);
        else if (answer == FailOutcome.Failed)
            _stats.Record(_serverId, execution.Queue, failed: 1);
        return answer;
    }

    /// <summary>OutcomePending → OutcomeInFlight для каждой записи; не удалось (Removed heartbeat'ом) — запись исключается.</summary>
    private List<Execution> BeginAttempt(List<Execution> executions)
    {
        var inFlight = new List<Execution>(executions.Count);
        var now = _clock.Now;
        foreach (var execution in executions)
        {
            if (execution.OutcomeFirstAttemptAt == 0)
                execution.OutcomeFirstAttemptAt = now;
            if (execution.TryTransition(ExecutionState.OutcomePending, ExecutionState.OutcomeInFlight))
                inFlight.Add(execution);
            else
                Log.OutcomeNotSentExecutionState(_logger, execution.Job.Id, execution.State);
        }
        return inFlight;
    }

    /// <summary>
    /// Таймаут попытки: остаток бюджета LeaseDuration с первой попытки, для ack-пакета — минимальный
    /// среди записей; ≥ 0 (нулевой — уже отменённый токен).
    /// </summary>
    private TimeSpan RemainingBudget(List<Execution> executions)
    {
        var remaining = _options.LeaseDuration;
        foreach (var execution in executions)
        {
            var left = _options.LeaseDuration - _clock.Elapsed(execution.OutcomeFirstAttemptAt);
            if (left < remaining)
                remaining = left;
        }
        return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
    }

    private void Finish(Execution execution)
    {
        if (!execution.TryTransition(ExecutionState.OutcomeInFlight, ExecutionState.Removed))
            return;
        _table.Remove(execution);
        try
        {
            _onLeaseFreed?.Invoke(execution.Queue);
        }
        catch (Exception e)
        {
            // Исход уже применён: сбой уведомления не должен превращаться в повтор отправки.
            Log.LeaseFreedNotificationFailed(_logger, e, execution.Queue);
        }
    }

    /// <returns>Записи, которые нужно повторить (после ожидания backoff); пусто — отправка окончена.</returns>
    private async Task<List<Execution>> AfterFailureAsync(List<Execution> inFlight, Exception error, TimeSpan backoff)
    {
        var retry = new List<Execution>(inFlight.Count);
        var budgetLeft = TimeSpan.MaxValue;
        foreach (var execution in inFlight)
        {
            execution.TryTransition(ExecutionState.OutcomeInFlight, ExecutionState.OutcomePending);
            var elapsed = _clock.Elapsed(execution.OutcomeFirstAttemptAt);
            if (_abort.IsCancellationRequested || elapsed >= _options.LeaseDuration)
            {
                Abandon(execution, error);
                continue;
            }
            retry.Add(execution);
            budgetLeft = budgetLeft < _options.LeaseDuration - elapsed ? budgetLeft : _options.LeaseDuration - elapsed;
        }
        if (retry.Count == 0)
            return retry;

        var delay = backoff < budgetLeft ? backoff : budgetLeft;
        Log.OutcomeSendFailedRetrying(_logger, error, retry.Count, delay);
        try
        {
            var wait = Task.Delay(delay, _clock.Time, _abort.Token);
            Interlocked.Increment(ref _backoffWaits);
            await wait;
        }
        catch (OperationCanceledException)
        {
            foreach (var execution in retry)
                Abandon(execution, error);
            return [];
        }
        return retry;
    }

    private void Abandon(Execution execution, Exception error)
    {
        if (!execution.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed))
            return;
        var stopped = _abort.IsCancellationRequested;
        Drop(execution, stopped ? DropReasons.Stopped : DropReasons.SendTimeout);
        if (stopped)
            Log.OutcomeNotSentStopCancelled(_logger, error, execution.Job.Id);
        else
            Log.OutcomeSendBudgetExceeded(_logger, error, execution.Job.Id, _options.LeaseDuration);
    }

    /// <summary>Снятие записи, переведённой этим вызывающим в Removed без применения исхода, с учётом в метрике — до лога.</summary>
    private void Drop(Execution execution, string reason)
    {
        _table.Remove(execution);
        _metrics.OutcomeDropped(execution.Queue, reason);
    }

    private static TimeSpan Next(TimeSpan backoff) => backoff * 2 < MaxBackoff ? backoff * 2 : MaxBackoff;

    private static partial class Log
    {
        [LoggerMessage(EventId = 4000, EventName = nameof(OutcomeDiscardedSinkStopped), Level = LogLevel.Warning,
            Message = "PgBolt: outcome {Outcome} of job {JobId} discarded: completion sink is stopped.")]
        internal static partial void OutcomeDiscardedSinkStopped(ILogger logger, string? outcome, Guid jobId);

        [LoggerMessage(EventId = 4001, EventName = nameof(OutcomeDiscardedNotRunning), Level = LogLevel.Warning,
            Message = "PgBolt: outcome {Outcome} of job {JobId} discarded: execution is {State} (abandoned or lease lost).")]
        internal static partial void OutcomeDiscardedNotRunning(ILogger logger, string outcome, Guid jobId, ExecutionState state);

        [LoggerMessage(EventId = 4002, EventName = nameof(SinkStopCancelled), Level = LogLevel.Warning,
            Message = "PgBolt: completion sink stop was cancelled; {Count} outcome send(s) aborted.")]
        internal static partial void SinkStopCancelled(ILogger logger, int count);

        [LoggerMessage(EventId = 4003, EventName = nameof(AckDiscardedStopCancelled), Level = LogLevel.Warning,
            Message = "PgBolt: outcome {Outcome} of job {JobId} discarded: completion sink stop was cancelled; its lease will expire.")]
        internal static partial void AckDiscardedStopCancelled(ILogger logger, string outcome, Guid jobId);

        [LoggerMessage(EventId = 4004, EventName = nameof(AckPartialAfterRetry), Level = LogLevel.Information,
            Message = "PgBolt: ack affected {Acked} of {Count} jobs after a failed attempt; the rest was probably already applied.")]
        internal static partial void AckPartialAfterRetry(ILogger logger, int acked, int count);

        [LoggerMessage(EventId = 4005, EventName = nameof(AckPartialLeaseLost), Level = LogLevel.Warning,
            Message = "PgBolt: ack affected {Acked} of {Count} jobs; lease of the rest was lost.")]
        internal static partial void AckPartialLeaseLost(ILogger logger, int acked, int count);

        [LoggerMessage(EventId = 4006, EventName = nameof(OutcomeLeaseNotFoundAfterRetry), Level = LogLevel.Information,
            Message = "PgBolt: outcome {Outcome} of job {JobId}: lease not found after a failed attempt; the outcome was probably already applied.")]
        internal static partial void OutcomeLeaseNotFoundAfterRetry(ILogger logger, string outcome, Guid jobId);

        [LoggerMessage(EventId = 4007, EventName = nameof(OutcomeDiscardedLeaseLost), Level = LogLevel.Warning,
            Message = "PgBolt: outcome {Outcome} of job {JobId} discarded: lease was lost.")]
        internal static partial void OutcomeDiscardedLeaseLost(ILogger logger, string outcome, Guid jobId);

        [LoggerMessage(EventId = 4008, EventName = nameof(OutcomeNotSentExecutionState), Level = LogLevel.Debug,
            Message = "PgBolt: outcome of job {JobId} is not sent: execution is {State}.")]
        internal static partial void OutcomeNotSentExecutionState(ILogger logger, Guid jobId, ExecutionState state);

        [LoggerMessage(EventId = 4009, EventName = nameof(LeaseFreedNotificationFailed), Level = LogLevel.Error,
            Message = "PgBolt: lease-freed notification for queue {Queue} failed.")]
        internal static partial void LeaseFreedNotificationFailed(ILogger logger, Exception e, string queue);

        [LoggerMessage(EventId = 4010, EventName = nameof(OutcomeSendFailedRetrying), Level = LogLevel.Warning,
            Message = "PgBolt: sending outcome of {Count} job(s) failed; retrying in {Delay}.")]
        internal static partial void OutcomeSendFailedRetrying(ILogger logger, Exception error, int count, TimeSpan delay);

        [LoggerMessage(EventId = 4011, EventName = nameof(OutcomeNotSentStopCancelled), Level = LogLevel.Warning,
            Message = "PgBolt: outcome of job {JobId} was not sent before stop was cancelled; its lease will expire.")]
        internal static partial void OutcomeNotSentStopCancelled(ILogger logger, Exception error, Guid jobId);

        [LoggerMessage(EventId = 4012, EventName = nameof(OutcomeSendBudgetExceeded), Level = LogLevel.Error,
            Message = "PgBolt: outcome of job {JobId} was not sent within {Budget}; the execution is dropped, its lease will expire.")]
        internal static partial void OutcomeSendBudgetExceeded(ILogger logger, Exception error, Guid jobId, TimeSpan budget);
    }
}
