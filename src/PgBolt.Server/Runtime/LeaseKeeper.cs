using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Таблица выполнений сервера, heartbeat по известным lease и проверка срока lease каждого выполнения.
/// </summary>
/// <remarks>
/// Heartbeat-цикл: вызов раз в <c>HeartbeatInterval × (1 ± 0.1)</c>, расписание — от sentAt предыдущего вызова;
/// таймаут вызова — <c>HeartbeatInterval / 2</c>, поэтому вызовы не перекрываются.
/// Таймер сроков взведён на ближайший рубеж <c>validUntil − HeartbeatInterval</c> среди неотменённых записей
/// <see cref="ExecutionState.Running"/> и не зависит от того, завершился ли текущий вызов heartbeat.
/// </remarks>
internal sealed partial class LeaseKeeper
{
    private readonly ServerRegistration _registration;
    private readonly Guid _serverId;
    private readonly IServerStore _store;
    private readonly ExecutionTable _table;
    private readonly MonoClock _clock;
    private readonly ILogger _logger;
    private readonly Action _onRecovered;
    private readonly PgBoltMetrics _metrics;
    private readonly TimeSpan _lease;
    private readonly TimeSpan _interval;
    private readonly CancellationTokenSource _stop = new();
    private readonly Lock _timerLock = new();
    private ITimer? _deadlineTimer;          // под _timerLock
    private long _armedFor = long.MaxValue;  // под _timerLock: рубеж, на который взведён таймер
    private long _lastSuccess;
    private Task _loop = Task.CompletedTask;
    private int _started;
    private int _heartbeatWaits;

    public LeaseKeeper(ServerRegistration registration, IServerStore store, ExecutionTable table, PgBoltServerOptions options,
                       MonoClock clock, ILogger logger, Action onRecovered, PgBoltMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(options);
        _registration = registration ?? throw new ArgumentNullException(nameof(registration));
        _serverId = registration.Id;
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _table = table ?? throw new ArgumentNullException(nameof(table));
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _onRecovered = onRecovered ?? throw new ArgumentNullException(nameof(onRecovered));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _lease = options.LeaseDuration;
        _interval = options.HeartbeatInterval;
        _lastSuccess = clock.Now;
    }

    /// <summary>Источник U[0, 1) для разброса расписания ±10 %; заменяется в тестах.</summary>
    internal Func<double> Jitter { get; set; } = static () => Random.Shared.NextDouble();

    /// <summary>Число пауз цикла heartbeat, начатых до текущего момента (тесты: таймер паузы уже создан).</summary>
    internal int HeartbeatWaits => Volatile.Read(ref _heartbeatWaits);

    /// <summary>now ≥ lastSuccess + LeaseDuration − HeartbeatInterval; до первого успеха отсчёт — от <see cref="Start"/>.</summary>
    public bool HeartbeatLost => _clock.Now >= _clock.Add(Volatile.Read(ref _lastSuccess), _lease - _interval);

    /// <summary>Регистрирует взятую задачу как <see cref="ExecutionState.Running"/> (до запуска JobRunner).</summary>
    /// <param name="validUntil">fetchStartedAt + LeaseDuration.</param>
    public Execution Register(LeasedJob job, string queue, long validUntil)
    {
        var execution = new Execution(job, queue, validUntil, ExecutionState.Running);
        _table.Add(execution);
        ArmAtMost(DeadlineOf(execution));
        return execution;
    }

    /// <summary>Регистрирует задачу, которая не будет запускаться: сразу OutcomePending с исходом release, ran = false.</summary>
    public Execution RegisterForRelease(LeasedJob job, string queue, long validUntil)
    {
        var execution = new Execution(job, queue, validUntil, ExecutionState.OutcomePending)
        {
            Outcome = new JobOutcome.Release(false),
        };
        _table.Add(execution);
        return execution;
    }

    /// <summary>
    /// Откат <see cref="Register"/> для выполнения, которое диспетчер не смог запустить: Running → Removed и удаление из
    /// таблицы — heartbeat его больше не продлевает, lease истекает не позже validUntil.
    /// </summary>
    /// <param name="dropReason">Причина для метрики <c>outcome.dropped</c>; <c>null</c> — выполнение не считается потерянным.</param>
    /// <returns>false — запись уже не Running (откатывать нечего, метрика не пишется).</returns>
    public bool Unregister(Execution execution, string? dropReason = null)
    {
        if (!execution.TryTransition(ExecutionState.Running, ExecutionState.Removed))
            return false;
        _table.Remove(execution);
        if (dropReason is not null)
            _metrics.OutcomeDropped(execution.Queue, dropReason);
        return true;
    }

    public void Start()
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
            throw new InvalidOperationException("LeaseKeeper is already started.");
        var startedAt = _clock.Now;
        Volatile.Write(ref _lastSuccess, startedAt);
        lock (_timerLock)
            _deadlineTimer = _clock.Time.CreateTimer(static state => ((LeaseKeeper)state!).OnDeadlineTimer(), this,
                Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        RearmDeadlineTimer();
        _loop = RunHeartbeatLoopAsync(startedAt, _stop.Token);
    }

    /// <summary>Останавливает цикл (текущий вызов heartbeat отменяется и дожидается) и таймер сроков.</summary>
    public async Task StopAsync()
    {
        if (Volatile.Read(ref _started) == 0)
            return;
        await _stop.CancelAsync();
        await _loop;
        lock (_timerLock)
        {
            _deadlineTimer?.Dispose();
            _deadlineTimer = null;
            _armedFor = long.MaxValue;
        }
    }

    /// <summary>Один вызов heartbeat; не бросает.</summary>
    internal Task HeartbeatOnceAsync(CancellationToken ct) => HeartbeatCoreAsync(_clock.Now, ct);

    /// <summary>
    /// Отменяет с причиной HeartbeatLost каждое неотменённое выполнение Running, достигшее рубежа
    /// validUntil − HeartbeatInterval, и перевзводит таймер. Её же вызывает таймер.
    /// </summary>
    internal void CheckDeadlines()
    {
        var now = _clock.Now;
        foreach (var execution in _table.Snapshot())
        {
            if (!IsWatched(execution) || now < DeadlineOf(execution))
                continue;
            if (execution.Cancel(CancelReason.HeartbeatLost))
                Log.LeaseRenewalNotConfirmed(_logger, execution.Job.Id, execution.Lease.LeaseId);
        }
        RearmDeadlineTimer();
    }

    private async Task RunHeartbeatLoopAsync(long startedAt, CancellationToken stop)
    {
        var previousSentAt = startedAt;
        while (true)
        {
            var next = _clock.Add(previousSentAt, _interval * (1 + 0.1 * (2 * Jitter() - 1)));
            try
            {
                var pause = Task.Delay(_clock.Until(next), _clock.Time, stop);
                Interlocked.Increment(ref _heartbeatWaits);
                await pause;
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }

            previousSentAt = _clock.Now;
            await HeartbeatCoreAsync(previousSentAt, stop);
            if (stop.IsCancellationRequested)
                return;
        }
    }

    private async Task HeartbeatCoreAsync(long sentAt, CancellationToken ct)
    {
        var snapshot = _table.Snapshot();
        var leases = new LeaseRef[snapshot.Count];
        for (var i = 0; i < leases.Length; i++)
            leases[i] = snapshot[i].Lease;

        HeartbeatResult result;
        using (var timeout = new CancellationTokenSource(_interval / 2, _clock.Time))
        using (var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeout.Token))
        {
            try
            {
                result = await _store.HeartbeatAsync(_serverId, leases, _lease, linked.Token);
            }
            catch (Exception) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e) when (timeout.IsCancellationRequested)
            {
                _metrics.StorageError(StorageOperations.Heartbeat);
                Log.HeartbeatTimedOut(_logger, e, _serverId, _interval / 2, leases.Length);
                return;
            }
            catch (Exception e)
            {
                _metrics.StorageError(StorageOperations.Heartbeat);
                Log.HeartbeatFailed(_logger, e, _serverId, leases.Length);
                return;
            }
        }

        var alive = result.Leases;
        var wasLost = HeartbeatLost;
        AdvanceLastSuccess(sentAt);

        // now() БД не раньше sentAt, значит lease_until ≥ sentAt + LeaseDuration.
        var renewedUntil = _clock.Add(sentAt, _lease);
        var returned = new HashSet<Guid>(alive.Count);
        foreach (var row in alive)
        {
            returned.Add(row.LeaseId);
            if (!_table.TryGet(row.LeaseId, out var execution))
                continue;
            execution.ExtendValidUntil(renewedUntil);
            if (row.CancelRequested && execution.Cancel(CancelReason.CancelRequested))
                Log.JobCancellationRequested(_logger, execution.Job.Id);
        }

        // lease из снимка, которого нет в ответе, подтверждённо потерян. Зарегистрированные после снимка не проверяются.
        foreach (var execution in snapshot)
        {
            if (returned.Contains(execution.Lease.LeaseId))
                continue;
            if (execution.TryTransition(ExecutionState.Running, ExecutionState.Removed))
            {
                execution.Cancel(CancelReason.LeaseLost);
                _table.Remove(execution);
                _metrics.OutcomeDropped(execution.Queue, DropReasons.LeaseLost);
                Log.LeaseLostExecutionCancelled(_logger, execution.Lease.LeaseId, execution.Job.Id);
            }
            else if (execution.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed))
            {
                _table.Remove(execution);
                _metrics.OutcomeDropped(execution.Queue, DropReasons.LeaseLost);
                Log.LeaseLostOutcomeDropped(_logger, execution.Lease.LeaseId, execution.Job.Id, execution.Outcome);
            }
            // OutcomeInFlight не трогается: авторитетен результат statement'а.
        }

        RearmDeadlineTimer();

        if (wasLost && !HeartbeatLost)
        {
            Log.HeartbeatRecovered(_logger, _serverId);
            try
            {
                _onRecovered();
            }
            catch (Exception e)
            {
                Log.HeartbeatRecoveryCallbackFailed(_logger, e, _serverId);
            }
        }

        if (!result.ServerPresent)
            await RegisterAgainAsync(ct);
    }

    /// <summary>
    /// Строки servers нет (удалена уборкой после паузы дольше DeadServerTimeout) — upsert регистрации.
    /// Таймаут — HeartbeatInterval / 2, поэтому следующий успешный ответ не позже S + 1.6 H; не бросает.
    /// Выполняется внутри цикла под его токеном: StopAsync отменяет и дожидается её до удаления строк серверов.
    /// </summary>
    private async Task RegisterAgainAsync(CancellationToken ct)
    {
        using var timeout = new CancellationTokenSource(_interval / 2, _clock.Time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeout.Token);
        try
        {
            await _store.RegisterServerAsync(_registration, linked.Token);
            Log.ServerReRegistered(_logger, _serverId);
        }
        catch (Exception) when (ct.IsCancellationRequested)
        {
            // Остановка: любое исключение отменённой регистрации (Npgsql при отмене может бросить и не OCE) — не ошибка.
        }
        catch (Exception e)
        {
            _metrics.StorageError(StorageOperations.Heartbeat);
            Log.ServerReRegistrationFailed(_logger, e, _serverId);
        }
    }

    private void AdvanceLastSuccess(long sentAt)
    {
        var current = Volatile.Read(ref _lastSuccess);
        while (sentAt > current)
        {
            var seen = Interlocked.CompareExchange(ref _lastSuccess, sentAt, current);
            if (seen == current)
                return;
            current = seen;
        }
    }

    /// <summary>
    /// Таймер следит только за Running без причины отмены: выполнение, уже отменённое по любой причине
    /// (в том числе HeartbeatLost), повторная отмена не меняет, а его прошедший рубеж зациклил бы таймер.
    /// </summary>
    private static bool IsWatched(Execution execution) =>
        execution.State == ExecutionState.Running && execution.FirstReason == CancelReason.None;

    private long DeadlineOf(Execution execution) => _clock.Add(execution.ValidUntil, -_interval);

    /// <summary>Сдвигает таймер на более ранний рубеж (регистрация). Change может сработать синхронно — Lock реентерабелен.</summary>
    private void ArmAtMost(long deadline)
    {
        lock (_timerLock)
        {
            if (_deadlineTimer is null || deadline >= _armedFor)
                return;
            _armedFor = deadline;
            _deadlineTimer.Change(DueIn(deadline), Timeout.InfiniteTimeSpan);
        }
    }

    /// <summary>Взводит таймер на ближайший рубеж среди отслеживаемых записей (или выключает).</summary>
    private void RearmDeadlineTimer()
    {
        lock (_timerLock)
        {
            if (_deadlineTimer is null)
                return;
            var next = long.MaxValue;
            // Перечисление без Snapshot(): без копии и без захвата всех полос словаря под _timerLock (бенчмарк
            // ExecutionTableBenchmarks). Add в Register предшествует ArmAtMost под этим же lock'ом — новая запись либо
            // видна здесь, либо её рубеж взведёт ArmAtMost после нас.
            foreach (var execution in _table.Entries())
                if (IsWatched(execution))
                    next = Math.Min(next, DeadlineOf(execution));
            _armedFor = next;
            _deadlineTimer.Change(next == long.MaxValue ? Timeout.InfiniteTimeSpan : DueIn(next), Timeout.InfiniteTimeSpan);
        }
    }

    /// <summary>Задержка до рубежа, не больше предела таймера (≈ 49 дней); раннее срабатывание безвредно — таймер перевзведётся.</summary>
    private TimeSpan DueIn(long deadline)
    {
        var due = _clock.Until(deadline);
        return due > MaxTimerDue ? MaxTimerDue : due;
    }

    private static readonly TimeSpan MaxTimerDue = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private void OnDeadlineTimer()
    {
        try
        {
            CheckDeadlines();
        }
        catch (Exception e)
        {
            Log.LeaseDeadlineCheckFailed(_logger, e, _serverId);
        }
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 4100, EventName = nameof(LeaseRenewalNotConfirmed), Level = LogLevel.Warning,
            Message = "PgBolt: lease renewal of job {JobId} (lease {LeaseId}) is not confirmed; cancelling the execution.")]
        internal static partial void LeaseRenewalNotConfirmed(ILogger logger, Guid jobId, Guid leaseId);

        [LoggerMessage(EventId = 4101, EventName = nameof(HeartbeatTimedOut), Level = LogLevel.Warning,
            Message = "PgBolt: heartbeat of server {ServerId} timed out after {Timeout}; {Count} leases not renewed.")]
        internal static partial void HeartbeatTimedOut(ILogger logger, Exception e, Guid serverId, TimeSpan timeout, int count);

        [LoggerMessage(EventId = 4102, EventName = nameof(HeartbeatFailed), Level = LogLevel.Warning,
            Message = "PgBolt: heartbeat of server {ServerId} failed; {Count} leases not renewed.")]
        internal static partial void HeartbeatFailed(ILogger logger, Exception e, Guid serverId, int count);

        [LoggerMessage(EventId = 4103, EventName = nameof(JobCancellationRequested), Level = LogLevel.Information,
            Message = "PgBolt: cancellation of job {JobId} requested; cancelling the execution.")]
        internal static partial void JobCancellationRequested(ILogger logger, Guid jobId);

        [LoggerMessage(EventId = 4104, EventName = nameof(LeaseLostExecutionCancelled), Level = LogLevel.Warning,
            Message = "PgBolt: lease {LeaseId} of job {JobId} is lost; the execution is cancelled and its outcome will be dropped.")]
        internal static partial void LeaseLostExecutionCancelled(ILogger logger, Guid leaseId, Guid jobId);

        [LoggerMessage(EventId = 4105, EventName = nameof(LeaseLostOutcomeDropped), Level = LogLevel.Warning,
            Message = "PgBolt: lease {LeaseId} of job {JobId} is lost; pending outcome {Outcome} is dropped.")]
        internal static partial void LeaseLostOutcomeDropped(ILogger logger, Guid leaseId, Guid jobId, JobOutcome? outcome);

        [LoggerMessage(EventId = 4106, EventName = nameof(HeartbeatRecovered), Level = LogLevel.Information,
            Message = "PgBolt: heartbeat of server {ServerId} recovered.")]
        internal static partial void HeartbeatRecovered(ILogger logger, Guid serverId);

        [LoggerMessage(EventId = 4107, EventName = nameof(HeartbeatRecoveryCallbackFailed), Level = LogLevel.Error,
            Message = "PgBolt: heartbeat recovery callback of server {ServerId} failed.")]
        internal static partial void HeartbeatRecoveryCallbackFailed(ILogger logger, Exception e, Guid serverId);

        [LoggerMessage(EventId = 4108, EventName = nameof(ServerReRegistered), Level = LogLevel.Warning,
            Message = "PgBolt: servers row of server {ServerId} was missing (removed by cleanup); registered again.")]
        internal static partial void ServerReRegistered(ILogger logger, Guid serverId);

        [LoggerMessage(EventId = 4109, EventName = nameof(ServerReRegistrationFailed), Level = LogLevel.Warning,
            Message = "PgBolt: re-registering server {ServerId} failed; retrying on the next heartbeat.")]
        internal static partial void ServerReRegistrationFailed(ILogger logger, Exception e, Guid serverId);

        [LoggerMessage(EventId = 4110, EventName = nameof(LeaseDeadlineCheckFailed), Level = LogLevel.Error,
            Message = "PgBolt: lease deadline check of server {ServerId} failed.")]
        internal static partial void LeaseDeadlineCheckFailed(ILogger logger, Exception e, Guid serverId);
    }
}
