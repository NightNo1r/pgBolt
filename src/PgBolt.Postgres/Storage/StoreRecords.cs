namespace PgBolt.Postgres.Storage;

/// <summary>Batch of jobs of one type (§5.1), with per-row arrays and shared scalar values.</summary>
public sealed record EnqueueBatch(
    string Type,
    string Queue,
    short Priority,
    int MaxAttempts,
    int[]? RetryDelaysSeconds,
    string? TraceParent,
    TimeSpan Delay,
    Guid[] Ids,
    string[] ArgsJson,
    DateTime?[] RunAtUtc,
    string?[] UniqueKeys)
{
    /// <summary>Scheduled occurrence time per row, supplied with RecurringIds (Part 4). Null uses the database's now().</summary>
    public DateTime?[]? ScheduledAtUtc { get; init; }

    /// <summary>
    /// Schedule IDs per row (Part 4). Null selects ordinary client enqueue SQL. Otherwise ticks and Trigger use the internal enqueue variant with
    /// recurring_id and scheduled_at (§8).
    /// </summary>
    public string[]? RecurringIds { get; init; }

    // Поля графа (часть 5) задаёт только планировщик workflow (internal init): Awaiting-строка без подписки, notify и учёта
    // total нарушила бы инварианты §4.

    /// <summary>
    /// Parent job or batch per row (Part 5 §6.1, step 2). Code 5 refers to the member's own batch. Supply with the other graph arrays.
    /// </summary>
    public Guid?[]? ParentIds { get; internal init; }

    /// <summary>Per-row jobs.continuation = queue.await_condition, codes 1–5 (Part 5). Non-NULL rows start Awaiting.</summary>
    public short?[]? Continuations { get; internal init; }

    /// <summary>Per-row queue.notify = jobs.notify, true for batch members and jobs with children in the graph (Part 5).</summary>
    public bool[]? Notify { get; internal init; }

    /// <summary>Containing batch per member row in both queue and jobs (Part 5).</summary>
    public Guid?[]? BatchIds { get; internal init; }

    internal void Validate()
    {
        if (Ids.Length == 0)
            throw new ArgumentException("Batch is empty.");
        if (ArgsJson.Length != Ids.Length || RunAtUtc.Length != Ids.Length || UniqueKeys.Length != Ids.Length)
            throw new ArgumentException("Batch arrays must have equal length.");
        if (Delay < TimeSpan.Zero)
            throw new ArgumentException("Delay must be non-negative.");
        if (MaxAttempts < 1)
            throw new ArgumentException("MaxAttempts must be >= 1.");
        foreach (var runAt in RunAtUtc)
            if (runAt is { Kind: not DateTimeKind.Utc })
                throw new ArgumentException("RunAtUtc values must have DateTimeKind.Utc.");
        if (ScheduledAtUtc is { } scheduled && scheduled.Length != Ids.Length || RecurringIds is { } recurring && recurring.Length != Ids.Length)
            throw new ArgumentException("Batch arrays must have equal length.");
        if (ScheduledAtUtc is not null && RecurringIds is null)
            throw new ArgumentException("ScheduledAtUtc requires RecurringIds.");
        if (RecurringIds is not null)
            foreach (var id in RecurringIds)
                if (string.IsNullOrEmpty(id))
                    throw new ArgumentException("RecurringIds values must be non-empty.");
        if (ScheduledAtUtc is not null)
            foreach (var at in ScheduledAtUtc)
                if (at is { Kind: not DateTimeKind.Utc })
                    throw new ArgumentException("ScheduledAtUtc values must have DateTimeKind.Utc.");
        if (ParentIds is not null || Continuations is not null || Notify is not null || BatchIds is not null)
        {
            if (ParentIds is null || Continuations is null || Notify is null || BatchIds is null)
                throw new ArgumentException("Workflow arrays (ParentIds, Continuations, Notify, BatchIds) must be set together.");
            if (ParentIds.Length != Ids.Length || Continuations.Length != Ids.Length || Notify.Length != Ids.Length
                || BatchIds.Length != Ids.Length)
                throw new ArgumentException("Batch arrays must have equal length.");
            if (RecurringIds is not null)
                throw new ArgumentException("Workflow and recurring columns are mutually exclusive.");
        }
    }
}

public readonly record struct EnqueueRowResult(Guid? Id, bool Created);

public sealed record LeasedJob(
    Guid Id,
    Guid LeaseId,
    string Type,
    string ArgsJson,
    int Attempt,
    int MaxAttempts,
    int[]? RetryDelaysSeconds,
    Guid? ParentId,
    Guid? BatchId,
    string? TraceParent,
    string? RecurringId = null,
    DateTime? ScheduledAtUtc = null,
    short? Continuation = null,
    double WaitSeconds = 0);

/// <summary>Dequeue result for a queue with a cluster-wide limit. LimitReached means no capacity remains after dequeue.</summary>
public sealed record LimitedDequeueResult(IReadOnlyList<LeasedJob> Jobs, bool LimitReached);

public readonly record struct LeaseRef(Guid JobId, Guid LeaseId);

public readonly record struct HeartbeatLease(Guid JobId, Guid LeaseId, bool CancelRequested);

/// <summary>Heartbeat response (Part 2 §9.2) indicating whether the server row was updated and which listed leases remain live.</summary>
public sealed record HeartbeatResult(bool ServerPresent, IReadOnlyList<HeartbeatLease> Leases);

public enum FailOutcome { LeaseLost, Retried, Cancelled, Failed }

/// <summary>Release outcome (Part 3 §7.1): returned without consuming an attempt, finalized as cancelled, or lease lost.</summary>
public enum ReleaseOutcome { LeaseLost, Released, Cancelled }

public sealed record PromoteResult(int Promoted, IReadOnlyCollection<string> Queues);

public sealed record JobTypeRecord(
    string Name,
    string ArgsSchemaJson,
    string DefaultQueue,
    int DefaultMaxAttempts,
    short DefaultPriority,
    int[]? DefaultRetryDelaysSeconds);

/// <summary>One row of outcome counters to flush (Part 6 §4.1). The bucket is the start of a UTC minute.</summary>
internal readonly record struct OutcomeStatsRow(DateTime BucketUtc, Guid ServerId, string Queue, int Succeeded, int Failed, int Retried);
