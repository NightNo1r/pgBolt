using PgBolt.Benchmarks.Copies;

namespace PgBolt.Benchmarks.Alternatives;

/// <summary>
/// Lock-free ArmAtMost с эпохой (seqlock). Перевзводы (Rearm/Start/Stop) по-прежнему сериализованы _timerLock — они редки
/// и пересчитывают минимум по снимку; без lock'а стал горячий путь регистрации на потоке диспетчера.
/// </summary>
/// <remarks>
/// <para>
/// Rearm: эпоха → нечётная (Interlocked.Increment, полный барьер), Snapshot, _armedFor, Change, эпоха → чётная.
/// ArmAtMost после Add: полный барьер, чтение эпохи. Деккер: либо снимок Rearm видит новую запись, либо ArmAtMost видит
/// его нечётную (или более позднюю) эпоху. Нечётная — Rearm идёт и мог не увидеть запись: ждём его конца. Чётная — все
/// Rearm, не учтённые в ней, увидят запись, а учтённые завершены: _armedFor ≤ рубежа — делать нечего.
/// </para>
/// <para>
/// Понижение: CAS _armedFor и Change. Rearm, начавшийся между чтением эпохи и нашим Change, мог выполнить свой Change
/// раньше — тогда наш, более поздний рубеж, затёр его. Значит, после Change перечитываем эпоху (порядок обеспечивает
/// lock внутри таймера: Change Rearm'а раньше нашего ⇒ его инкремент эпохи нам виден); изменилась — повторяем с текущим
/// _armedFor. ArmAtMost вызывает только поток диспетчера (один писатель-понижатель).
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
/// НЕКОРРЕКТНЫЙ наивный lock-free ArmAtMost (CAS + Change без эпохи) — только для --verify, чтобы показать, что проверка
/// ловит гонку: Rearm, снимок которого не видел запись, записывает более поздний рубеж после нашего CAS/Change.
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
