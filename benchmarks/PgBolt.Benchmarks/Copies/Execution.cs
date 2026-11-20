// Копия: src/PgBolt.Server/Runtime/Execution.cs @ 52b4a8f.
// Изменено: вместо LeasedJob (PgBolt.Postgres) — пара (jobId, leaseId) и LeaseRef-копия; свойство Job убрано.
// Переходы состояния (CAS), отмена, ExtendValidUntil — дословно.

namespace PgBolt.Benchmarks.Copies;

/// <summary>Копия PgBolt.Postgres.Storage.LeaseRef.</summary>
internal readonly record struct LeaseRef(Guid JobId, Guid LeaseId);

internal enum ExecutionState { Running, OutcomePending, OutcomeInFlight, Removed }

internal enum CancelReason { None, Shutdown, CancelRequested, HeartbeatLost, LeaseLost, Timeout }

internal sealed class Execution
{
    private readonly CancellationTokenSource _cts = new();
    private int _state;
    private int _firstReason;
    private long _validUntil;

    public Execution(Guid jobId, Guid leaseId, string queue, long validUntil, ExecutionState initialState)
    {
        ArgumentNullException.ThrowIfNull(queue);
        if (initialState is not (ExecutionState.Running or ExecutionState.OutcomePending))
            throw new ArgumentOutOfRangeException(nameof(initialState), initialState, "An execution starts as Running or OutcomePending.");
        Queue = queue;
        Lease = new LeaseRef(jobId, leaseId);
        _validUntil = validUntil;
        _state = (int)initialState;
    }

    public string Queue { get; }

    public LeaseRef Lease { get; }

    public bool FromReserved { get; set; }

    public CancellationToken Token => _cts.Token;

    public CancelReason FirstReason => (CancelReason)Volatile.Read(ref _firstReason);

    public long ValidUntil => Volatile.Read(ref _validUntil);

    public ExecutionState State => (ExecutionState)Volatile.Read(ref _state);

    public JobOutcome? Outcome { get; set; }

    public long OutcomeFirstAttemptAt { get; set; }

    /// <summary>Только для бенчмарков: возврат переиспользуемой записи в Running между вызовами (в продукте нет).</summary>
    internal void ResetForReuse()
    {
        Volatile.Write(ref _state, (int)ExecutionState.Running);
        Outcome = null;
    }

    public bool Cancel(CancelReason reason)
    {
        if (reason == CancelReason.None)
            throw new ArgumentOutOfRangeException(nameof(reason), reason, "None is not a cancellation reason.");
        var first = Interlocked.CompareExchange(ref _firstReason, (int)reason, (int)CancelReason.None) == (int)CancelReason.None;
        _ = _cts.CancelAsync();
        return first;
    }

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

/// <summary>Копия src/PgBolt.Server/Runtime/JobOutcome.cs @ 52b4a8f, только нужные бенчмарку исходы.</summary>
internal abstract record JobOutcome
{
    public sealed record Ack : JobOutcome;

    public sealed record Release(bool Ran) : JobOutcome;
}
