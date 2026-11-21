namespace PgBolt.Postgres.Schema;

/// <summary>Retention run result (Part 2 §5.1). The caller uses the result to log warnings. The core emits no logs.</summary>
/// <param name="DroppedJobsPartitions">Dates of entire jobs partitions dropped, including partitions detached by previous runs.</param>
/// <param name="DroppedEventsPartitions">Dates of job_events partitions dropped, including partitions detached by previous runs.</param>
/// <param name="DroppedCompactionRemnants">Compaction remnants removed from earlier runs. Excludes remnants created in this run.</param>
/// <param name="Pinned">Jobs partitions pinned by at most RetentionCompactionMaxLiveJobs live jobs.</param>
/// <param name="Deferred">Partitions and tables deferred to a future run.</param>
/// <param name="AttachedJobsPartitions">Number of attached jobs partitions after the run (§5.3, Partition Count).</param>
/// <param name="DeletedBatches">Number of batches rows older than JobsRetention that were deleted (Part 5 §7.1).</param>
public sealed record RetentionResult(
    IReadOnlyList<DateOnly> DroppedJobsPartitions,
    IReadOnlyList<DateOnly> DroppedEventsPartitions,
    int DroppedCompactionRemnants,
    IReadOnlyList<PinnedPartition> Pinned,
    IReadOnlyList<DeferredPartition> Deferred,
    int AttachedJobsPartitions,
    int DeletedBatches = 0);

/// <param name="Day">UTC date of the pinned partition.</param>
/// <param name="OverdueDays">Days since the partition became eligible, or 0 if first eligible today.</param>
/// <param name="LiveJobs">Live job count from the precheck.</param>
/// <param name="Compacted">True if compacted in this run. False if the terminal row count was below max(LiveJobs, 100).</param>
public sealed record PinnedPartition(DateOnly Day, int OverdueDays, int LiveJobs, bool Compacted);

/// <param name="Table">"jobs" or "job_events".</param>
/// <param name="Day">UTC date of the deferred partition or table.</param>
/// <param name="Reason">Reason retention deferred this table.</param>
/// <param name="OverdueDays">Days since the partition became eligible for retention.</param>
/// <param name="LiveJobs">For LiveJobs and Replicated only. The precheck reads at most T + 1 live jobs, so T + 1 means more than T.</param>
/// <param name="Error">Exception text for DropFailed only.</param>
public sealed record DeferredPartition(string Table, DateOnly Day, DeferReason Reason, int OverdueDays, int? LiveJobs,
    string? Error = null);

/// <summary>Reasons retention deferred a partition or table (Part 2 §5.1).</summary>
public enum DeferReason
{
    /// <summary>RetentionLockTimeout elapsed before the lock was acquired. The partition is unchanged.</summary>
    LockTimeout,
    /// <summary>statement_timeout or idle_in_transaction_session_timeout expired while the parent lock was held. The transaction rolled
    /// back.</summary>
    HoldTimeout,
    /// <summary>The table was detached but DROP could not obtain a lock. A later run will remove the remnant.</summary>
    DropLockTimeout,
    /// <summary>DROP on the detached table failed for another reason. A later run will remove the remnant.</summary>
    DropFailed,
    /// <summary>The live job count exceeds RetentionCompactionMaxLiveJobs, or compaction is disabled.</summary>
    LiveJobs,
    /// <summary>The jobs table belongs to a logical replication publication, so compaction was skipped.</summary>
    Replicated,
}
