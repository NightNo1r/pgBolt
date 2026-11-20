namespace PgBolt.Benchmarks.Copies;

// Копия ядра синхронизации таймера сроков: src/PgBolt.Server/Runtime/LeaseKeeper.cs @ 52b4a8f.
// Дословно: _timerLock/_deadlineTimer/_armedFor, Register, Start (создание таймера и первый Rearm), остановка таймера
// из StopAsync, IsWatched, DeadlineOf, ArmAtMost, RearmDeadlineTimer, DueIn. Опущено: heartbeat-цикл и CheckDeadlines
// (используют те же RearmDeadlineTimer/_table). Таймер — из MonoClock.Time (в бенчмарке — заглушка StubTimeProvider).

/// <summary>Общий контракт вариантов: регистрация (ArmAtMost) и перевзвод (Rearm).</summary>
internal interface ILeaseDeadlines
{
    void Start();

    Execution Register(Guid jobId, Guid leaseId, string queue, long validUntil);

    void Rearm();

    void Stop();
}

/// <summary>Текущий вариант: ArmAtMost и RearmDeadlineTimer под _timerLock.</summary>
internal sealed class LockLeaseDeadlines : ILeaseDeadlines
{
    private readonly ExecutionTable _table;
    private readonly MonoClock _clock;
    private readonly TimeSpan _interval;
    private readonly Lock _timerLock = new();
    private ITimer? _deadlineTimer;          // под _timerLock
    private long _armedFor = long.MaxValue;  // под _timerLock: рубеж, на который взведён таймер

    public LockLeaseDeadlines(ExecutionTable table, MonoClock clock, TimeSpan heartbeatInterval)
    {
        _table = table;
        _clock = clock;
        _interval = heartbeatInterval;
    }

    public Execution Register(Guid jobId, Guid leaseId, string queue, long validUntil)
    {
        var execution = new Execution(jobId, leaseId, queue, validUntil, ExecutionState.Running);
        _table.Add(execution);
        ArmAtMost(DeadlineOf(execution));
        return execution;
    }

    public void Start()
    {
        lock (_timerLock)
            _deadlineTimer = _clock.Time.CreateTimer(static state => ((LockLeaseDeadlines)state!).OnDeadlineTimer(), this,
                Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        RearmDeadlineTimer();
    }

    public void Stop()
    {
        lock (_timerLock)
        {
            _deadlineTimer?.Dispose();
            _deadlineTimer = null;
            _armedFor = long.MaxValue;
        }
    }

    public void Rearm() => RearmDeadlineTimer();

    private static bool IsWatched(Execution execution) =>
        execution.State == ExecutionState.Running && execution.FirstReason == CancelReason.None;

    private long DeadlineOf(Execution execution) => _clock.Add(execution.ValidUntil, -_interval);

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

    private void RearmDeadlineTimer()
    {
        lock (_timerLock)
        {
            if (_deadlineTimer is null)
                return;
            var next = long.MaxValue;
            foreach (var execution in _table.Snapshot())
                if (IsWatched(execution))
                    next = Math.Min(next, DeadlineOf(execution));
            _armedFor = next;
            _deadlineTimer.Change(next == long.MaxValue ? Timeout.InfiniteTimeSpan : DueIn(next), Timeout.InfiniteTimeSpan);
        }
    }

    private TimeSpan DueIn(long deadline)
    {
        var due = _clock.Until(deadline);
        return due > MaxTimerDue ? MaxTimerDue : due;
    }

    private static readonly TimeSpan MaxTimerDue = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private void OnDeadlineTimer()
    {
        // В продукте — CheckDeadlines (отмена просроченных) и RearmDeadlineTimer; заглушка таймера не срабатывает.
    }
}
