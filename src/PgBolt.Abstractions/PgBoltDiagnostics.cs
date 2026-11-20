namespace PgBolt;

/// <summary>
/// Имена источника трассировки, <c>Meter</c> и instruments PgBolt. Единственное место, где они записаны литералами:
/// подписчики (OpenTelemetry и т. п.) берут имена и границы бакетов отсюда.
/// </summary>
public static class PgBoltDiagnostics
{
    public const string ActivitySourceName = "PgBolt";
    public const string MeterName = "PgBolt";

    /// <summary>Имена instruments метрик пода и кластерных метрик.</summary>
    public static class Instruments
    {
        public const string JobEnqueued = "pgbolt.job.enqueued";
        public const string RecurringFired = "pgbolt.recurring.fired";
        public const string JobDuration = "pgbolt.job.duration";
        public const string JobWaitTime = "pgbolt.job.wait_time";
        public const string JobOutcomeDropped = "pgbolt.job.outcome.dropped";
        public const string WorkerActive = "pgbolt.worker.active";
        public const string WorkerCapacity = "pgbolt.worker.capacity";
        public const string StorageErrors = "pgbolt.storage.errors";
        public const string MaintenanceRuns = "pgbolt.maintenance.runs";
        public const string MaintenanceDuration = "pgbolt.maintenance.duration";
        public const string RetentionDeferred = "pgbolt.retention.deferred";
        public const string ClusterQueueJobs = "pgbolt.cluster.queue.jobs";
        public const string ClusterQueueOldestReadyAge = "pgbolt.cluster.queue.oldest_ready_age";
        public const string ClusterQueueLimit = "pgbolt.cluster.queue.limit";
        public const string ClusterTableDeadTuples = "pgbolt.cluster.table.dead_tuples";
        public const string ClusterTableSize = "pgbolt.cluster.table.size";
    }

    /// <summary>Границы бакетов гистограмм, секунды.</summary>
    public static class HistogramBuckets
    {
        public static IReadOnlyList<double> Job { get; } = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600];
        public static IReadOnlyList<double> Maintenance { get; } = [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300];
    }
}
