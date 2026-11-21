using System.Globalization;
using Microsoft.Extensions.Logging;
using PgBolt.Postgres;
using PgBolt.Postgres.Schema;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Logs retention results and records deferred partitions. The core does not emit them; this component selects the log
/// levels.
/// </summary>
internal static partial class RetentionReport
{
    internal const int PinnedWarningDays = 7;
    internal const int PartitionCountSlack = 30;

    public static void Write(ILogger logger, RetentionResult result, PgBoltOptions core, PgBoltMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(result);
        ArgumentNullException.ThrowIfNull(core);
        ArgumentNullException.ThrowIfNull(metrics);

        foreach (var (reason, count) in result.Deferred.CountBy(d => d.Reason))
            metrics.RetentionDeferred(reason.ToString(), count);

        if (result.DroppedJobsPartitions.Count + result.DroppedEventsPartitions.Count + result.DroppedCompactionRemnants > 0)
            Log.RetentionPartitionsDropped(logger, result.DroppedJobsPartitions.Count, Days(result.DroppedJobsPartitions),
                result.DroppedEventsPartitions.Count, Days(result.DroppedEventsPartitions), result.DroppedCompactionRemnants);

        if (result.DeletedBatches > 0)
            Log.RetentionBatchesDeleted(logger, result.DeletedBatches);

        var max = core.RetentionCompactionMaxLiveJobs;
        var tooMany = result.Deferred.Where(d => d.Reason == DeferReason.LiveJobs).ToList();
        var replicated = result.Deferred.Where(d => d.Reason == DeferReason.Replicated).ToList();
        var pinned = result.Pinned.Count + tooMany.Count + replicated.Count;
        if (pinned > 0)
        {
            var live = result.Pinned.Sum(p => (long)p.LiveJobs) + tooMany.Concat(replicated).Sum(d => (long)(d.LiveJobs ?? 0));
            var oldest = result.Pinned.Select(p => p.Day).Concat(tooMany.Concat(replicated).Select(d => d.Day)).Min();
            Log.RetentionPinnedPartitions(logger, pinned, result.Pinned.Count(p => p.Compacted), result.Pinned.Count(p => !p.Compacted),
                tooMany.Count, max, replicated.Count, live, Day(oldest));
        }

        foreach (var d in tooMany.Where(d => d.OverdueDays >= PinnedWarningDays))
            Log.RetentionPartitionTooManyLiveJobs(logger, Day(d.Day), LiveText(d.LiveJobs, max), max, d.OverdueDays);

        var oldReplicated = replicated.Where(d => d.OverdueDays >= PinnedWarningDays).ToList();
        if (oldReplicated.Count > 0)
            Log.RetentionCompactionSkippedReplication(logger, oldReplicated.Count, Day(oldReplicated.Min(d => d.Day)));

        if (core.JobsRetention is { } retention)
        {
            var normal = core.PartitionDaysAhead + PartitionCatalog.RetentionDays(retention) + 1;
            if (result.AttachedJobsPartitions > normal + PartitionCountSlack)
                Log.RetentionTooManyAttachedPartitions(logger, result.AttachedJobsPartitions, normal);
        }

        foreach (var table in result.Deferred.Where(d => d.Reason == DeferReason.HoldTimeout).GroupBy(d => d.Table))
            Log.RetentionHoldTimeout(logger, table.Key, table.Count());

        foreach (var table in result.Deferred.Where(d => d.Reason is DeferReason.LockTimeout or DeferReason.DropLockTimeout).GroupBy(d => d.Table))
        {
            var overdue = table.Max(d => d.OverdueDays);
            Log.RetentionDeferredByLockTimeout(logger, overdue >= 1 ? LogLevel.Warning : LogLevel.Debug, table.Count(), table.Key, overdue);
        }

        foreach (var d in result.Deferred.Where(d => d.Reason == DeferReason.DropFailed))
            Log.RetentionRemnantDropFailed(logger, d.Table, Day(d.Day), d.Error);
    }

    private static string Day(DateOnly day) => day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    private static string Days(IEnumerable<DateOnly> days) => string.Join(", ", days.Select(Day));

    /// <summary>LiveJobs = T + 1 means more than T.</summary>
    private static string LiveText(int? live, int max) =>
        live > max ? $">{max}" : (live ?? 0).ToString(CultureInfo.InvariantCulture);

    private static partial class Log
    {
        [LoggerMessage(EventId = 5100, EventName = nameof(RetentionPartitionsDropped), Level = LogLevel.Information,
            Message = "PgBolt retention: dropped {JobsCount} jobs partitions [{JobsDays}], {EventsCount} job_events partitions [{EventsDays}] and {Remnants} compaction remnants.")]
        internal static partial void RetentionPartitionsDropped(ILogger logger, int jobsCount, string jobsDays, int eventsCount, string eventsDays,
            int remnants);

        [LoggerMessage(EventId = 5101, EventName = nameof(RetentionBatchesDeleted), Level = LogLevel.Information,
            Message = "PgBolt retention: deleted {Count} finished batches rows older than JobsRetention.")]
        internal static partial void RetentionBatchesDeleted(ILogger logger, int count);

        [LoggerMessage(EventId = 5102, EventName = nameof(RetentionPinnedPartitions), Level = LogLevel.Information,
            Message = "PgBolt retention: {Count} jobs partitions are pinned by live jobs: {Compacted} compacted now, {Small} left as is (few terminal rows), {TooMany} not compactable (more than {Max} live jobs), {Replicated} skipped (logical replication); {Live} live jobs in total, oldest day {Oldest}.")]
        internal static partial void RetentionPinnedPartitions(ILogger logger, int count, int compacted, int small, int tooMany, int max,
            int replicated, long live, string oldest);

        [LoggerMessage(EventId = 5103, EventName = nameof(RetentionPartitionTooManyLiveJobs), Level = LogLevel.Warning,
            Message = "PgBolt retention: jobs partition {Day} holds {Live} live jobs, more than RetentionCompactionMaxLiveJobs ({Max}); it is not compacted and keeps its whole disk space until they finish (overdue by {Overdue} days). Run, cancel or delete these jobs, or raise the limit.")]
        internal static partial void RetentionPartitionTooManyLiveJobs(ILogger logger, string day, string live, int max, int overdue);

        [LoggerMessage(EventId = 5104, EventName = nameof(RetentionCompactionSkippedReplication), Level = LogLevel.Warning,
            Message = "PgBolt retention: compaction of {Count} pinned jobs partitions is skipped because jobs is in a logical replication publication; they keep their disk space until their live jobs finish (oldest day {Oldest}).")]
        internal static partial void RetentionCompactionSkippedReplication(ILogger logger, int count, string oldest);

        [LoggerMessage(EventId = 5105, EventName = nameof(RetentionTooManyAttachedPartitions), Level = LogLevel.Warning,
            Message = "PgBolt retention: {Attached} jobs partitions are attached, normally {Normal}; days pinned by live jobs stay attached and slow down dequeue planning.")]
        internal static partial void RetentionTooManyAttachedPartitions(ILogger logger, int attached, int normal);

        [LoggerMessage(EventId = 5106, EventName = nameof(RetentionHoldTimeout), Level = LogLevel.Warning,
            Message = "PgBolt retention: holding the lock on {Table} hit statement_timeout or idle_in_transaction_session_timeout ({Count} partitions deferred); the hot path waited up to the hold limit. Likely a slow disk or a process pause (GC).")]
        internal static partial void RetentionHoldTimeout(ILogger logger, string table, int count);

        // Уровень выбирается при вызове: Warning, если отсрочка длится сутки и дольше, иначе Debug.
        [LoggerMessage(EventId = 5107, EventName = nameof(RetentionDeferredByLockTimeout),
            Message = "PgBolt retention: {Count} {Table} partitions were deferred by lock_timeout (overdue by up to {Overdue} days); long queries or a holder of the partition itself (autovacuum, direct query) block their removal.")]
        internal static partial void RetentionDeferredByLockTimeout(ILogger logger, LogLevel level, int count, string table, int overdue);

        [LoggerMessage(EventId = 5108, EventName = nameof(RetentionRemnantDropFailed), Level = LogLevel.Error,
            Message = "PgBolt retention: dropping the detached {Table} table of {Day} failed: {Error}; it will be retried as a remnant.")]
        internal static partial void RetentionRemnantDropFailed(ILogger logger, string table, string day, string? error);
    }
}
