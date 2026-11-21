using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>State of an execution in the LeaseKeeper table (§6.2). Removed is terminal.</summary>
internal enum ExecutionState { Running, OutcomePending, OutcomeInFlight, Removed }

/// <summary>Reason for canceling an execution (§5.1). The first reason takes precedence.</summary>
internal enum CancelReason { None, Shutdown, CancelRequested, HeartbeatLost, LeaseLost, Timeout }

/// <summary>
/// Execution table entry (§6.2) for one claimed job and its lease. State, the first cancellation reason and validUntil
/// are updated atomically using CAS and read without locks.
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

    /// <summary>Kind of slot allocated by <see cref="SlotAllocator.Take"/>.</summary>
    public bool FromReserved { get; set; }

    /// <summary>
    /// Canceled by <see cref="Cancel"/>. The source lives as long as the entry and requires no disposal because it has no
    /// timers.
    /// </summary>
    public CancellationToken Token => _cts.Token;

    public CancelReason FirstReason => (CancelReason)Volatile.Read(ref _firstReason);

    /// <summary>Lower bound on lease_until on the monotonic clock (<see cref="MonoClock"/>).</summary>
    public long ValidUntil => Volatile.Read(ref _validUntil);

    public ExecutionState State => (ExecutionState)Volatile.Read(ref _state);

    /// <summary>Set before the transition to OutcomePending. The transition's CAS barrier makes the value visible.</summary>
    public JobOutcome? Outcome { get; set; }

    /// <summary>
    /// Time of the first outcome send attempt, used for the LeaseDuration budget (§6.1). Zero means no attempt has
    /// started.
    /// </summary>
    public long OutcomeFirstAttemptAt { get; set; }

    /// <summary>
    /// Records the first reason and always cancels <see cref="Token"/> idempotently. Token callbacks run asynchronously
    /// through <see cref="CancellationTokenSource.CancelAsync"/>, keeping job continuations off the heartbeat and timer
    /// threads.
    /// </summary>
    /// <returns>True if this call recorded the first reason.</returns>
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

    /// <summary>
    /// Uses CAS to transition from one state to another. A transition outside §6.2 throws
    /// <see cref="InvalidOperationException"/>.
    /// </summary>
    /// <returns>False if the current state differs from <paramref name="from"/> and the state was left unchanged.</returns>
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
