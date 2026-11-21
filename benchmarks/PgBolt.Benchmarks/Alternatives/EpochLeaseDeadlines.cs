using PgBolt.Benchmarks.Copies;

namespace PgBolt.Benchmarks.Alternatives;

/// <summary>
/// Lock-free ArmAtMost using a sequence counter. Infrequent Rearm, Start, and Stop operations still serialize under _timerLock and recalculate
/// the snapshot minimum. Dispatcher registration runs without the lock.
/// </summary>
/// <remarks>
/// <para>
/// Rearm increments the epoch to odd with a full barrier, takes a snapshot, updates _armedFor, calls Change, then increments the epoch to even.
/// ArmAtMost executes a full barrier after Add and reads the epoch. By Dekker's argument, either Rearm sees the new entry or ArmAtMost sees its
/// odd or a later epoch. An odd epoch means a rearm may have missed the entry, so wait for it to finish. At an even epoch, past rearms have
/// finished and future ones will see the entry. If _armedFor is already at or before the deadline, nothing needs to change.
/// </para>
/// <para>
/// To move the deadline earlier, CAS _armedFor and call Change. A Rearm that started after the epoch read might have called Change first,
/// allowing this Change to overwrite its earlier deadline. Read the epoch again after Change and retry with the current _armedFor if it changed.
/// The timer's internal lock ensures that an earlier Rearm Change makes its epoch increment visible. Only the dispatcher calls ArmAtMost, so
/// there is one writer lowering the deadline.
/// </para>
/// </remarks>
internal sealed class EpochLeaseDeadlines : ILeaseDeadlines
{
    private readonly ExecutionTable _table;
    private readonly MonoClock _clock;
    private readonly TimeSpan _interval;
    private readonly Lock _timerLock = new();   // сериализует перевзводы между собой
    private ITimer? _deadlineTimer;             // пишется под _timerLock при нечётной эпохе
    private long _armedFor = long.MaxValue;
    private long _epoch;                        // нечётная — идёт перевзвод

    public EpochLeaseDeadlines(ExecutionTable table, MonoClock clock, TimeSpan heartbeatInterval)
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
        {
            Interlocked.Increment(ref _epoch);
            Volatile.Write(ref _deadlineTimer, _clock.Time.CreateTimer(static _ => { }, null,
                Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan));
            Interlocked.Increment(ref _epoch);
        }
        Rearm();
    }

    public void Stop()
    {
        lock (_timerLock)
        {
            Interlocked.Increment(ref _epoch);
            _deadlineTimer?.Dispose();
            Volatile.Write(ref _deadlineTimer, null);
            Volatile.Write(ref _armedFor, long.MaxValue);
            Interlocked.Increment(ref _epoch);
        }
    }

    public void Rearm()
    {
        lock (_timerLock)
        {
            if (_deadlineTimer is null)
                return;
            Interlocked.Increment(ref _epoch);           // нечётная; полный барьер перед чтением таблицы
            var next = long.MaxValue;
            foreach (var execution in _table.Snapshot())
                if (IsWatched(execution))
                    next = Math.Min(next, DeadlineOf(execution));
            Volatile.Write(ref _armedFor, next);
            _deadlineTimer.Change(next == long.MaxValue ? Timeout.InfiniteTimeSpan : DueIn(next), Timeout.InfiniteTimeSpan);
            Interlocked.Increment(ref _epoch);           // чётная
        }
    }

    private void ArmAtMost(long deadline)
    {
        Interlocked.MemoryBarrier();                     // Add → чтение эпохи (Деккер с Rearm)
        var reapply = false;
        var spin = new SpinWait();
        while (true)
        {
            var epoch = Volatile.Read(ref _epoch);
            if ((epoch & 1) != 0)
            {
                spin.SpinOnce(sleep1Threshold: -1);      // идёт перевзвод — его снимок мог не увидеть запись
                continue;
            }
            var timer = Volatile.Read(ref _deadlineTimer);
            if (timer is null)
                return;                                  // не запущен или остановлен: Start сам перевзведёт по таблице
            var armed = Volatile.Read(ref _armedFor);
            long target;
            if (deadline < armed)
            {
                if (Interlocked.CompareExchange(ref _armedFor, deadline, armed) != armed)
                    continue;                            // Rearm успел записать своё — пересмотреть
                target = deadline;
            }
            else if (reapply)
                target = armed;
            else
                return;
            timer.Change(target == long.MaxValue ? Timeout.InfiniteTimeSpan : DueIn(target), Timeout.InfiniteTimeSpan);
            if (Volatile.Read(ref _epoch) == epoch)
                return;
            reapply = true;                              // Rearm пересёкся с нашим Change — восстановить его рубеж
        }
    }

    private static bool IsWatched(Execution execution) =>
        execution.State == ExecutionState.Running && execution.FirstReason == CancelReason.None;

    private long DeadlineOf(Execution execution) => _clock.Add(execution.ValidUntil, -_interval);

    private TimeSpan DueIn(long deadline)
    {
        var due = _clock.Until(deadline);
        return due > MaxTimerDue ? MaxTimerDue : due;
    }

    private static readonly TimeSpan MaxTimerDue = TimeSpan.FromMilliseconds(uint.MaxValue - 1);
}

/// <summary>
/// Intentionally incorrect lock-free ArmAtMost using CAS and Change without an epoch. Used only by --verify to show that the check detects a
/// Rearm whose snapshot misses the entry and writes a later deadline after our CAS and Change.
/// </summary>
internal sealed class NaiveLeaseDeadlines : ILeaseDeadlines
{
    private readonly ExecutionTable _table;
    private readonly MonoClock _clock;
    private readonly TimeSpan _interval;
    private readonly Lock _timerLock = new();
    private ITimer? _deadlineTimer;
    private long _armedFor = long.MaxValue;

    public NaiveLeaseDeadlines(ExecutionTable table, MonoClock clock, TimeSpan heartbeatInterval)
    {
        _table = table;
        _clock = clock;
        _interval = heartbeatInterval;
    }

    public Execution Register(Guid jobId, Guid leaseId, string queue, long validUntil)
    {
        var execution = new Execution(jobId, leaseId, queue, validUntil, ExecutionState.Running);
        _table.Add(execution);
        var deadline = _clock.Add(execution.ValidUntil, -_interval);
        var timer = Volatile.Read(ref _deadlineTimer);
        var armed = Volatile.Read(ref _armedFor);
        while (timer is not null && deadline < armed)
        {
            var seen = Interlocked.CompareExchange(ref _armedFor, deadline, armed);
            if (seen == armed)
            {
                timer.Change(_clock.Until(deadline), Timeout.InfiniteTimeSpan);
                break;
            }
            armed = seen;
        }
        return execution;
    }

    public void Start()
    {
        lock (_timerLock)
            Volatile.Write(ref _deadlineTimer, _clock.Time.CreateTimer(static _ => { }, null,
                Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan));
        Rearm();
    }

    public void Stop()
    {
        lock (_timerLock)
        {
            _deadlineTimer?.Dispose();
            Volatile.Write(ref _deadlineTimer, null);
        }
    }

    public void Rearm()
    {
        lock (_timerLock)
        {
            if (_deadlineTimer is null)
                return;
            var next = long.MaxValue;
            foreach (var execution in _table.Snapshot())
                if (execution.State == ExecutionState.Running && execution.FirstReason == CancelReason.None)
                    next = Math.Min(next, _clock.Add(execution.ValidUntil, -_interval));
            Volatile.Write(ref _armedFor, next);
            _deadlineTimer.Change(next == long.MaxValue ? Timeout.InfiniteTimeSpan : _clock.Until(next), Timeout.InfiniteTimeSpan);
        }
    }
}
