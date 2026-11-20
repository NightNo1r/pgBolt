using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Fetch-цикл виртуального сервера: обход плана SlotAllocator (порядок и доли общего пула — его стратегия),
/// адаптивный polling, пробуждение по latched событию (слот освобождён, wakeup-сигнал, остановка, снятие паузы
/// heartbeat, коммит исхода). Пока кластерный лимит очереди исчерпан (LimitReached последнего dequeue), её результат
/// считается пустым, а wakeup-сигнал и освобождение слота поглощаются: место в лимите появляется только после коммита
/// исхода, о нём сообщает leaseFreed. Dequeue ограничен собственным таймаутом HeartbeatInterval и остановкой
/// не отменяется; задачи dequeue, вернувшегося позже таймаута (Npgsql не смог доставить отмену), не запускаются, а уходят release.
/// Неожиданная ошибка итерации логируется и ставит выборку на паузу; цикл не падает.
/// </summary>
internal sealed partial class Dispatcher
{
    private static readonly TimeSpan MinErrorPause = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxErrorPause = TimeSpan.FromSeconds(30);
    private const double PollJitter = 0.1;

    private readonly PgBoltServerOptions _options;
    private readonly SlotAllocator _slots;
    private readonly IFetchStrategy _fetch;
    private readonly LeaseKeeper _keeper;
    private readonly CompletionSink _sink;
    private readonly IWakeupSignal _wakeup;
    private readonly MonoClock _clock;
    private readonly ILogger _logger;
    private readonly Action<Execution> _startExecution;
    private readonly PgBoltMetrics _metrics;
    private readonly AsyncAutoResetEvent _wake = new();
    private readonly QueueState[] _queues;
    private readonly Dictionary<string, QueueState> _byName;
    private readonly Func<string, bool> _isDue;
    private volatile bool _stopping;
    private int _idleWaits;

    public Dispatcher(PgBoltServerOptions options, SlotAllocator slots, IFetchStrategy fetch, LeaseKeeper keeper,
        CompletionSink sink, IWakeupSignal wakeup, MonoClock clock, ILogger logger, Action<Execution> startExecution, PgBoltMetrics metrics)
    {
        _options = options;
        _slots = slots;
        _fetch = fetch;
        _keeper = keeper;
        _sink = sink;
        _wakeup = wakeup;
        _clock = clock;
        _logger = logger;
        _startExecution = startExecution;
        _metrics = metrics;
        _queues = [.. options.Queues.Select(q => new QueueState(q))];
        _byName = _queues.ToDictionary(q => q.Options.Name, StringComparer.Ordinal);
        _isDue = queue => _byName[queue].NextPoll <= _clock.Now;   // вызывается SlotAllocator.Plan под его lock
    }

    public Task Completion { get; private set; } = Task.CompletedTask;

    /// <summary>Источник U[0, 1) для jitter интервала polling (±10 %); тесты подменяют.</summary>
    internal Func<double> NextRandom { get; set; } = Random.Shared.NextDouble;

    /// <summary>Для тестов: сколько раз цикл встал на ожидание (таймер ожидания уже взведён).</summary>
    internal int IdleWaits => Volatile.Read(ref _idleWaits);

    public void Start() => Completion = Task.Run(RunAsync);

    public void SignalStop()
    {
        _stopping = true;
        _wake.Set();
    }

    public void OnSlotFreed(string queue)
    {
        if (_byName.TryGetValue(queue, out var q))
            Volatile.Write(ref q.SlotFreed, 1);
        _wake.Set();
    }

    /// <summary>Исход задачи очереди закоммичен — её место в кластерном лимите видно следующему dequeue.</summary>
    public void OnLeaseFreed(string queue)
    {
        if (_byName.TryGetValue(queue, out var q))
            Volatile.Write(ref q.LeaseFreed, 1);
        _wake.Set();
    }

    public void Wake() => _wake.Set();

    private async Task RunAsync()
    {
        var subscriptions = Subscribe();
        try
        {
            var errorPause = TimeSpan.Zero;
            long pausedUntil = 0;
            while (!_stopping)
            {
                try
                {
                    if (_keeper.HeartbeatLost)
                    {
                        // Пауза выборки до снятия состояния (onRecovered → Wake); таймаут — страховка.
                        await IdleAsync(_options.HeartbeatInterval);
                        ApplyFlags();
                        continue;
                    }
                    if (pausedUntil != 0 && _clock.Now < pausedUntil)
                    {
                        await IdleAsync(_clock.Until(pausedUntil));
                        ApplyFlags();
                        continue;
                    }

                    // По плану слотов (очередь, n): следующая запись вычисляется после Take по предыдущей.
                    foreach (var (name, n) in _slots.Plan(_isDue))
                    {
                        if (_stopping || _keeper.HeartbeatLost)
                            break;
                        var q = _byName[name];

                        var fetchStartedAt = _clock.Now;
                        var pollBeforeSlot = q.PollBeforeSlot;
                        q.PollBeforeSlot = null;
                        IReadOnlyList<LeasedJob> jobs;
                        var measure = _options.FetchMeasured;
                        var started = fetchStartedAt;
                        try
                        {
                            using var timeout = new CancellationTokenSource(_options.HeartbeatInterval, _clock.Time);
                            if (measure is not null) { started = _clock.Now; }
                            var result = await _fetch.FetchAsync(name, n, timeout.Token);
                            measure?.Invoke(_clock.Elapsed(started), result.Jobs.Count);
                            jobs = result.Jobs;
                            q.LimitReached = result.LimitReached;
                        }
                        catch (Exception e)
                        {
                            measure?.Invoke(_clock.Elapsed(started), null);
                            _metrics.StorageError(StorageOperations.Dequeue);
                            errorPause = NextErrorPause(errorPause);
                            pausedUntil = _clock.Add(_clock.Now, errorPause);
                            Log.DequeueFailed(_logger, e, name, errorPause);
                            break;
                        }
                        errorPause = TimeSpan.Zero;

                        var validUntil = _clock.Add(fetchStartedAt, _options.LeaseDuration);
                        if (_stopping || _keeper.HeartbeatLost)
                        {
                            // Взяты после сигнала остановки или во время сбоя heartbeat: не запускаются.
                            foreach (var job in jobs)
                                _sink.EnqueueRegistered(_keeper.RegisterForRelease(job, name, validUntil));
                            break;
                        }

                        var now = _clock.Now;
                        if (now > _clock.Add(fetchStartedAt, _options.HeartbeatInterval))
                        {
                            // Dequeue пережил свой таймаут: отмену Npgsql доставляет по сети, и без сети вызов
                            // возвращается позже. До рубежа отмены осталось меньше HeartbeatInterval — такое выполнение
                            // сразу отменилось бы с HeartbeatLost и потратило попытку. Задачи не запускаются.
                            foreach (var job in jobs)
                                _sink.EnqueueRegistered(_keeper.RegisterForRelease(job, name, validUntil));
                            if (jobs.Count > 0)
                                Log.DequeueTimedOutJobsReleased(_logger, name, jobs.Count, _clock.Elapsed(fetchStartedAt), _options.HeartbeatInterval);
                        }
                        else
                        {
                            foreach (var job in jobs)
                                StartOne(job, name, validUntil);
                        }

                        // Пустая проверка по освобождению слота не сдвигает плановый опрос.
                        if (jobs.Count == 0 && !q.LimitReached && pollBeforeSlot is { } deadline && fetchStartedAt < deadline)
                        {
                            q.NextPoll = deadline;
                            continue;
                        }

                        // У исчерпанного лимита мест нет, сколько бы строк ни взято: немедленный повтор взял бы 0 под lock.
                        if (jobs.Count == 0 || q.LimitReached)
                        {
                            q.Interval = Min(q.Interval * 2, q.Options.MaxPollInterval);
                            q.NextPoll = _clock.Add(now, q.Interval * (1 + PollJitter * (2 * NextRandom() - 1)));
                        }
                        else
                        {
                            q.Interval = q.Options.MinPollInterval;
                            q.NextPoll = jobs.Count >= n ? now : _clock.Add(now, q.Interval);
                        }
                    }

                    if (_stopping)
                        break;
                    if (pausedUntil != 0 && _clock.Now < pausedUntil)
                        continue;                  // пауза после ошибки — ожидание в начале цикла
                    ApplyFlags();
                    await IdleAsync(NextPollDelay());
                    ApplyFlags();
                }
                catch (Exception e)
                {
                    // Не ошибка dequeue (та обработана выше): сбой запуска выполнения (уже откачен в StartOne), регистрации,
                    // стратегии плана.
                    // Цикл не падает — иначе выборка молча остановилась бы при живом heartbeat.
                    errorPause = NextErrorPause(errorPause);
                    pausedUntil = _clock.Add(_clock.Now, errorPause);
                    Log.DispatcherIterationFailed(_logger, e, errorPause);
                }
            }
        }
        finally
        {
            foreach (var subscription in subscriptions)
            {
                try
                {
                    subscription.Dispose();
                }
                catch (Exception e)
                {
                    Log.WakeupSubscriptionDisposeFailed(_logger, e);
                }
            }
        }
    }

    /// <summary>Подписки IWakeupSignal на все очереди; сбой пользовательской реализации — очередь без сигналов.</summary>
    private List<IDisposable> Subscribe()
    {
        var subscriptions = new List<IDisposable>(_queues.Length);
        foreach (var q in _queues)
        {
            try
            {
                subscriptions.Add(_wakeup.Subscribe(q.Options.Name, () =>
                {
                    Volatile.Write(ref q.Signaled, 1);
                    _wake.Set();
                }));
            }
            catch (Exception e)
            {
                Log.WakeupSubscriptionFailed(_logger, e, q.Options.Name);
            }
        }
        return subscriptions;
    }

    /// <summary>
    /// Register → Take → startExecution одной задачи. Сбой после Register откатывается до передачи исключения в обработчик
    /// итерации: сначала снимается запись (не остаётся Running, heartbeat её не продлевает), затем
    /// возвращается слот. Порядок важен: Unregister не бросает, а Release бросает при ошибке учёта слотов — такая ошибка
    /// логируется и не мешает снятию записи и передаче исходного исключения.
    /// startExecution по контракту либо запускает выполнение, либо бросает, не запустив его.
    /// </summary>
    private void StartOne(LeasedJob job, string queue, long validUntil)
    {
        var execution = _keeper.Register(job, queue, validUntil);
        var slotTaken = false;
        try
        {
            execution.FromReserved = _slots.Take(queue);
            slotTaken = true;
            _startExecution(execution);
        }
        catch
        {
            _keeper.Unregister(execution, DropReasons.Abandoned);
            if (slotTaken)
            {
                try
                {
                    _slots.Release(queue, execution.FromReserved);
                }
                catch (Exception releaseError)
                {
                    Log.StartRollbackSlotReleaseFailed(_logger, releaseError, queue, job.Id);
                }
            }
            Log.JobStartRolledBack(_logger, job.Id, queue);
            throw;
        }
    }

    /// <summary>
    /// До ближайшего nextPoll среди очередей со свободным слотом; таких нет — до пробуждения. Ноль не ведёт к холостому
    /// циклу при любой стратегии: стратегия не может закончить план, пока у готовой очереди есть свободный слот в пределах
    /// бюджета плана (SlotAllocator бросает исключение). Слоты, освободившиеся посреди плана, раздаёт следующий план —
    /// с нулевой задержкой он начинается сразу.
    /// </summary>
    private TimeSpan NextPollDelay()
    {
        var delay = Timeout.InfiniteTimeSpan;
        foreach (var q in _queues)
        {
            if (_slots.Available(q.Options.Name) < 1)
                continue;
            var until = _clock.Until(q.NextPoll);
            if (delay == Timeout.InfiniteTimeSpan || until < delay)
                delay = until;
        }
        return delay;
    }

    /// <summary>
    /// Флаги latched и применяются после обновления LimitReached по результату dequeue — сигнал, пришедший во время
    /// dequeue, не теряется. У исчерпанного лимита signaled и slotFreed сбрасываются без действия: сигнал enqueue давал бы
    /// dequeue под lock каждую итерацию, а слот освобождается до коммита исхода, и dequeue увидел бы строку ещё взятой.
    /// Без LimitReached сбрасывается leaseFreed: место уже учтено dequeue по slotFreed.
    /// </summary>
    private void ApplyFlags()
    {
        var now = _clock.Now;
        foreach (var q in _queues)
        {
            var signaled = Interlocked.Exchange(ref q.Signaled, 0) == 1;
            var slotFreed = Interlocked.Exchange(ref q.SlotFreed, 0) == 1;
            var leaseFreed = Interlocked.Exchange(ref q.LeaseFreed, 0) == 1;
            if (q.LimitReached)
            {
                if (leaseFreed)
                    q.NextPoll = now;
                continue;
            }
            if (signaled)
            {
                q.PollBeforeSlot = null;
                q.NextPoll = now;
                q.Interval = q.Options.MinPollInterval;
            }
            else if (slotFreed)
            {
                // До следующей выборки несколько слотов могут освободиться подряд.
                q.PollBeforeSlot ??= q.NextPoll;
                q.NextPoll = now;
            }
        }
    }

    private async Task IdleAsync(TimeSpan timeout)
    {
        var wait = _wake.WaitAsync(timeout, _clock.Time, CancellationToken.None);
        Interlocked.Increment(ref _idleWaits);
        await wait;
    }

    private static TimeSpan NextErrorPause(TimeSpan current) =>
        current == TimeSpan.Zero ? MinErrorPause : Min(current * 2, MaxErrorPause);

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;

    private sealed class QueueState(QueueOptions options)
    {
        public QueueOptions Options { get; } = options;
        public TimeSpan Interval = options.MinPollInterval;
        public long NextPoll;
        public long? PollBeforeSlot;
        public int Signaled;
        public int SlotFreed;
        public int LeaseFreed;
        public bool LimitReached;   // только поток цикла
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 2000, EventName = nameof(DequeueFailed), Level = LogLevel.Error,
            Message = "PgBolt: dequeue from queue {Queue} failed; fetching paused for {Pause}.")]
        internal static partial void DequeueFailed(ILogger logger, Exception e, string queue, TimeSpan pause);

        [LoggerMessage(EventId = 2001, EventName = nameof(DequeueTimedOutJobsReleased), Level = LogLevel.Warning,
            Message = "PgBolt: dequeue from queue {Queue} returned {Count} jobs after {Elapsed}, past its timeout {Timeout}; they are released without running.")]
        internal static partial void DequeueTimedOutJobsReleased(ILogger logger, string queue, int count, TimeSpan elapsed, TimeSpan timeout);

        [LoggerMessage(EventId = 2002, EventName = nameof(DispatcherIterationFailed), Level = LogLevel.Critical,
            Message = "PgBolt: dispatcher iteration failed unexpectedly; fetching paused for {Pause}.")]
        internal static partial void DispatcherIterationFailed(ILogger logger, Exception e, TimeSpan pause);

        [LoggerMessage(EventId = 2003, EventName = nameof(WakeupSubscriptionDisposeFailed), Level = LogLevel.Warning,
            Message = "PgBolt: disposing a wakeup subscription failed.")]
        internal static partial void WakeupSubscriptionDisposeFailed(ILogger logger, Exception e);

        [LoggerMessage(EventId = 2004, EventName = nameof(WakeupSubscriptionFailed), Level = LogLevel.Error,
            Message = "PgBolt: wakeup subscription for queue {Queue} failed; the queue is polled without wakeup signals.")]
        internal static partial void WakeupSubscriptionFailed(ILogger logger, Exception e, string queue);

        [LoggerMessage(EventId = 2005, EventName = nameof(StartRollbackSlotReleaseFailed), Level = LogLevel.Error,
            Message = "PgBolt: slot of queue {Queue} was not returned after the failed start of job {JobId}.")]
        internal static partial void StartRollbackSlotReleaseFailed(ILogger logger, Exception releaseError, string queue, Guid jobId);

        [LoggerMessage(EventId = 2006, EventName = nameof(JobStartRolledBack), Level = LogLevel.Warning,
            Message = "PgBolt: job {JobId} of queue {Queue} was not started; its registration is rolled back, the lease will expire.")]
        internal static partial void JobStartRolledBack(ILogger logger, Guid jobId, string queue);
    }
}
