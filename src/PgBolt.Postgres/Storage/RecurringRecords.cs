namespace PgBolt.Postgres.Storage;

internal enum RecurringUpsertResult { Inserted, Updated, OwnedByCodeFirst }

internal enum RecurringRemoveResult { NotFound, Removed, OwnedByCodeFirst }

/// <summary>Schedule row with job_types defaults for Trigger (Part 4 §4.2). UnknownType indicates a missing job_types entry.</summary>
internal sealed record RecurringTriggerRow(
    string Type,
    string ArgsJson,
    string? Queue,
    short? Priority,
    int? MaxAttempts,
    int[]? RetryDelaysSeconds,
    OverlapPolicy Overlap,
    bool UnknownType);

/// <summary>Row error (Part 4 §5.2, step 2). No job is enqueued, the cursor stays put, and wake_at becomes db_now + 1 minute.</summary>
internal sealed record RecurringRowError(string Id, string Error);

/// <summary>
/// Occurrences discarded by the misfire policy when advancing the cursor (warning in §7). As in MissedRange, CatchUp uses [FromUtc, ToUtc),
/// while Skip includes both endpoints.
/// </summary>
internal sealed record RecurringMissed(string Id, DateTime FromUtc, DateTime ToUtc);

/// <summary>
/// Scheduler tick transaction result. Full means batchSize rows were claimed or the occurrence budget was exhausted, so the loop ticks again
/// immediately. OverlapSkipped counts FireOnce/Skip occurrences skipped because a previous run is active. Waiting counts CatchUp + Skip
/// schedules waiting for it. Exhausted means the cron expression has no further occurrences.
/// </summary>
internal sealed record RecurringTickResult(
    int Claimed,
    int Fired,
    bool Full,
    IReadOnlyCollection<string> Queues,
    IReadOnlyList<RecurringRowError> Errors,
    IReadOnlyList<RecurringMissed> Missed,
    IReadOnlyList<string> OverlapSkipped,
    IReadOnlyList<string> Waiting,
    IReadOnlyList<string> Exhausted)
{
    public static readonly RecurringTickResult Empty = new(0, 0, false, [], [], [], [], [], []);
}

internal sealed record RecurringFieldChange(string Id, IReadOnlyList<string> Fields);

/// <summary>Owner synchronization changes (Part 4 §6.2). Collisions are IDs held by another owner or the API. Their rows remain
/// untouched.</summary>
internal sealed record RecurringSyncResult(
    IReadOnlyList<string> Inserted,
    IReadOnlyList<RecurringFieldChange> Updated,
    IReadOnlyList<string> Deleted,
    IReadOnlyList<string> Collisions)
{
    public int Writes => Inserted.Count + Updated.Count + Deleted.Count;
}
