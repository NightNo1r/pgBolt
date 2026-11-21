using PgBolt.Postgres;

namespace PgBolt.Server;

/// <summary>Process-wide maintenance settings (§3.1, §6.3).</summary>
public sealed class PgBoltProcessingOptions
{
    /// <summary>
    /// Runs the promoter, sweeper, and storage maintenance in this process (Part 2 §6.2). Maintenance creates partitions, cleans up server rows, and
    /// applies retention. If all processes using a schema disable maintenance and none calls IPgBoltMaintenance, partitions run out after
    /// PartitionDaysAhead days and enqueue starts failing. Stale server rows and expired data also accumulate. Workflow continuations and batches
    /// require a resolver in a process with AddPgBoltServer and RunMaintenance = true. Without one, signals accumulate and continuations never start
    /// (Part 5 §8).
    /// </summary>
    public bool RunMaintenance { get; set; } = true;

    /// <summary>Delay between promoter passes (±20%).</summary>
    public TimeSpan PromoterInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Delay between sweeper passes (±20%).</summary>
    public TimeSpan SweeperInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>Delay between attempts by this pod to claim storage maintenance work from maintenance_tasks (±20%).</summary>
    public TimeSpan MaintenancePollInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Cluster-wide interval for the partitions task (EnsurePartitionsAsync). Each claim records its interval, so the pod that last claimed the task
    /// determines the interval if settings differ.
    /// </summary>
    public TimeSpan PartitionsInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Cluster-wide interval for the servers task, which removes stale server rows.</summary>
    public TimeSpan ServersCleanupInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Cluster-wide retention interval. After LockTimeout or HoldTimeout, retry only after the next interval.</summary>
    public TimeSpan RetentionInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Removes a server row after this much time without a heartbeat. A live server's next heartbeat restores its row. Leases are independent of
    /// server rows.
    /// </summary>
    public TimeSpan DeadServerTimeout { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>Runs the recurring scheduler in this process (Part 4 §5.1). All pods can run it because it claims rows with SKIP LOCKED.</summary>
    public bool RunScheduler { get; set; } = true;

    /// <summary>Delay between scheduler ticks (±20%). Occurrences can be delayed by roughly this interval.</summary>
    public TimeSpan SchedulerInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// MisfirePolicy.Skip discards occurrences older than this threshold (Part 4 §5.3). Null uses max(1 minute, 5 × SchedulerInterval). An explicit
    /// threshold must be at least 5 × SchedulerInterval. The pod that claims the row supplies the threshold.
    /// </summary>
    public TimeSpan? MisfireThreshold { get; set; }

    /// <summary>Interval for synchronizing code-defined schedules (Part 4 §6.1).</summary>
    public TimeSpan RecurringSyncInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Delay between workflow resolver passes (±20%, Part 5 §6.4). Local finalization wakes the resolver immediately. Also sets retry_after for
    /// signals whose children are locked by other transactions.
    /// </summary>
    public TimeSpan ResolverInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Flush interval for process-wide outcome counters (Part 6 §4.1). Every process with AddPgBoltServer flushes, regardless of RunMaintenance.
    /// </summary>
    public TimeSpan StatsFlushInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Cluster-wide interval for the stats task, which snapshots queue_stats and applies retention to stats_minutes (Part 6 §4.2). The last pod to
    /// claim the task supplies the interval. The claim lasts 0.8 × the interval.
    /// </summary>
    public TimeSpan StatsSnapshotInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Delay between refreshes of queue concurrency limits and weights (±10%). It determines how quickly this process picks up changes from other
    /// processes. Writes through the local manager refresh settings immediately.
    /// </summary>
    public TimeSpan QueueSettingsRefreshInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Retention period for the per-minute counters in stats_minutes.</summary>
    public TimeSpan StatsRetention { get; set; } = TimeSpan.FromDays(7);

    internal TimeSpan EffectiveMisfireThreshold =>
        MisfireThreshold ?? TimeSpan.FromTicks(Math.Max(TimeSpan.FromMinutes(1).Ticks, (5 * SchedulerInterval).Ticks));

    internal void Validate()
    {
        Check(PromoterInterval, nameof(PromoterInterval));
        Check(SweeperInterval, nameof(SweeperInterval));
        Check(MaintenancePollInterval, nameof(MaintenancePollInterval));
        Check(PartitionsInterval, nameof(PartitionsInterval));
        Check(ServersCleanupInterval, nameof(ServersCleanupInterval));
        Check(RetentionInterval, nameof(RetentionInterval));
        Check(DeadServerTimeout, nameof(DeadServerTimeout));
        Check(SchedulerInterval, nameof(SchedulerInterval));
        Check(RecurringSyncInterval, nameof(RecurringSyncInterval));
        Check(ResolverInterval, nameof(ResolverInterval));
        Check(StatsFlushInterval, nameof(StatsFlushInterval));
        Check(StatsSnapshotInterval, nameof(StatsSnapshotInterval));
        Check(QueueSettingsRefreshInterval, nameof(QueueSettingsRefreshInterval));
        if (StatsRetention <= TimeSpan.Zero || StatsRetention > PgBoltOptions.MaxRetention)
            throw new InvalidOperationException("PgBolt processing: StatsRetention must be > 0 and <= 3650 days.");
        if (MisfireThreshold is { } threshold && threshold < 5 * SchedulerInterval)
            throw new InvalidOperationException(
                $"PgBolt processing: MisfireThreshold must be >= 5 × SchedulerInterval ({5 * SchedulerInterval}).");
    }

    private static void Check(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero || value > PgBoltServerOptions.MaxInterval)
            throw new InvalidOperationException($"PgBolt processing: {name} must be > 0 and <= {PgBoltServerOptions.MaxInterval}.");
    }
}
