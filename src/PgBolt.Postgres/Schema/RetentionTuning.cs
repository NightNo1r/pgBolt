namespace PgBolt.Postgres.Schema;

/// <summary>
/// Retention test hooks (§12) run after listing, prechecking, checking pg_locks, warming, batches A and B, and transaction 1 COMMIT, and after
/// selecting and locking batches deletion candidates (Part 5 §7.1). PrecheckOidChecked sits between the jobs precheck batches: first OID
/// verification and source (a), then queue and source (b) in one statement. BatchesWindowed runs between selecting a candidate window and
/// locking it.
/// </summary>
internal enum RetentionStep
{
    Listed, Prechecked, LocksChecked, WarmedUp, BatchA, BatchB, Committed, BatchesLocked, PrecheckOidChecked, BatchesWindowed,
}

/// <param name="Step">Retention step that triggered this hook.</param>
/// <param name="Parent">"jobs", "job_events", or "" for Listed.</param>
/// <param name="Days">Partition days for this step: a group, the partition being compacted, or all prechecked partitions.</param>
internal sealed record RetentionHookContext(RetentionStep Step, string Parent, IReadOnlyList<DateOnly> Days);

/// <summary>
/// Internal retention settings (§5.3). K is the detach group size. Timeouts bound the parent lock hold time. Tests use separate instances to
/// avoid shared mutable state.
/// </summary>
internal sealed record RetentionTuning(int PartitionsPerTransaction, TimeSpan HoldStatementTimeout, TimeSpan HoldIdleTimeout)
{
    public static RetentionTuning Default { get; } = new(10, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));

    /// <summary>Test callback invoked at <see cref="RetentionStep"/> points. Throwing aborts the run.</summary>
    public Func<RetentionHookContext, CancellationToken, Task>? Hook { get; init; }

    /// <summary>Test command inserted at the start of batch B, e.g. pg_sleep to exercise statement_timeout.</summary>
    public string? BatchBTestPrefix { get; init; }
}
