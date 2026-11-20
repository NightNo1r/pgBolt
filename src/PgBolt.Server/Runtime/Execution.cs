using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>Состояние записи выполнения в таблице LeaseKeeper (§6.2). Removed терминально.</summary>
internal enum ExecutionState { Running, OutcomePending, OutcomeInFlight, Removed }

/// <summary>Причина отмены выполнения (§5.1); значима первая.</summary>
internal enum CancelReason { None, Shutdown, CancelRequested, HeartbeatLost, LeaseLost, Timeout }

/// <summary>
/// Запись таблицы выполнений (§6.2): одна взятая задача с её lease. Состояние, первая причина отмены и
/// validUntil меняются атомарно (CAS) и читаются без блокировок.
/// </summary>
internal sealed class Execution
{
    private readonly CancellationTokenSource _cts = new();
    private int _state;
    private int _firstReason;
    private long _validUntil;

    public Execution(LeasedJob job, string queue, long validUntil, ExecutionState initialState)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(queue);
        if (initialState is not (ExecutionState.Running or ExecutionState.OutcomePending))
            throw new ArgumentOutOfRangeException(nameof(initialState), initialState, "An execution starts as Running or OutcomePending.");
        Job = job;
        Queue = queue;
        Lease = new LeaseRef(job.Id, job.LeaseId);
        _validUntil = validUntil;
        _state = (int)initialState;
    }

    public LeasedJob Job { get; }

    public string Queue { get; }

    public LeaseRef Lease { get; }

    /// <summary>Вид слота, выданного <see cref="SlotAllocator.Take"/>.</summary>
    public bool FromReserved { get; set; }

    /// <summary>Отменяется <see cref="Cancel"/>; источник живёт столько же, сколько запись, и не освобождается (таймеров нет).</summary>
    public CancellationToken Token => _cts.Token;

    public CancelReason FirstReason => (CancelReason)Volatile.Read(ref _firstReason);

    /// <summary>Нижняя граница lease_until по монотонным часам (<see cref="MonoClock"/>).</summary>
    public long ValidUntil => Volatile.Read(ref _validUntil);

    public ExecutionState State => (ExecutionState)Volatile.Read(ref _state);

    /// <summary>Ставится до перехода в OutcomePending; видимость обеспечивает барьер CAS перехода.</summary>
    public JobOutcome? Outcome { get; set; }

    /// <summary>Момент первой попытки отправки исхода (бюджет LeaseDuration, §6.1); 0 — не начиналась.</summary>
    public long OutcomeFirstAttemptAt { get; set; }

    /// <summary>
    /// Запоминает причину, если она первая, и отменяет <see cref="Token"/> (всегда, идемпотентно).
    /// Колбэки токена запускаются асинхронно (<see cref="CancellationTokenSource.CancelAsync"/>):
    /// код задачи, продолжающийся после отмены, не выполняется на потоке heartbeat или таймера.
    /// </summary>
    /// <returns>true — причина стала первой.</returns>
    public bool Cancel(CancelReason reason)
    {
        if (reason == CancelReason.None)
            throw new ArgumentOutOfRangeException(nameof(reason), reason, "None is not a cancellation reason.");
        var first = Interlocked.CompareExchange(ref _firstReason, (int)reason, (int)CancelReason.None) == (int)CancelReason.None;
        _ = _cts.CancelAsync();
        return first;
    }

    /// <summary>validUntil = max(validUntil, candidate).</summary>
    public void ExtendValidUntil(long candidate)
    {
        var current = Volatile.Read(ref _validUntil);
        while (candidate > current)
        {
            var seen = Interlocked.CompareExchange(ref _validUntil, candidate, current);
            if (seen == current)
                return;
            current = seen;
        }
    }

    /// <summary>CAS from → to. Переходы вне §6.2 — <see cref="InvalidOperationException"/>.</summary>
    /// <returns>false — текущее состояние не равно <paramref name="from"/>; состояние не меняется.</returns>
    public bool TryTransition(ExecutionState from, ExecutionState to)
    {
        if (!IsAllowed(from, to))
            throw new InvalidOperationException($"Execution state transition {from} -> {to} is not allowed.");
        return Interlocked.CompareExchange(ref _state, (int)to, (int)from) == (int)from;
    }

    private static bool IsAllowed(ExecutionState from, ExecutionState to) => (from, to) switch
    {
        (ExecutionState.Running, ExecutionState.OutcomePending) => true,
        (ExecutionState.OutcomePending, ExecutionState.OutcomeInFlight) => true,
        (ExecutionState.OutcomeInFlight, ExecutionState.OutcomePending) => true,
        (ExecutionState.Running, ExecutionState.Removed) => true,
        (ExecutionState.OutcomePending, ExecutionState.Removed) => true,
        (ExecutionState.OutcomeInFlight, ExecutionState.Removed) => true,
        _ => false,
    };
}
