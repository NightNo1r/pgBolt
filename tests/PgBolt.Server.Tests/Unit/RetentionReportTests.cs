using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Logging;
using PgBolt.Postgres;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Schema;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;
using I = PgBolt.PgBoltDiagnostics.Instruments;

namespace PgBolt.Server.Tests.Unit;

public sealed class RetentionReportTests : IDisposable
{
    private static readonly DateOnly D = new(2026, 10, 1);
    private readonly ListLogger _logger = new();
    private readonly PgBoltOptions _core = new();   // PartitionDaysAhead 7, JobsRetention 7 сут, порог 1000
    private readonly PgBoltInstrumentation _instrumentation = TestTelemetry.Instrumentation();

    public void Dispose() => _instrumentation.Dispose();

    private static RetentionResult Result(IReadOnlyList<PinnedPartition>? pinned = null, IReadOnlyList<DeferredPartition>? deferred = null,
        int attached = 15, IReadOnlyList<DateOnly>? jobs = null, IReadOnlyList<DateOnly>? events = null, int remnants = 0) =>
        new(jobs ?? [], events ?? [], remnants, pinned ?? [], deferred ?? [], attached);

    private void Write(RetentionResult r) => RetentionReport.Write(_logger, r, _core, TestTelemetry.Metrics(_instrumentation));

    [Fact]
    public void Quiet_run_writes_nothing()
    {
        Write(Result());
        Assert.Empty(_logger.Entries);
    }

    [Fact]
    public void Dropped_partitions_are_one_information_line()
    {
        Write(Result(jobs: [D, D.AddDays(1)], events: [D], remnants: 2));

        var e = Assert.Single(_logger.Entries);
        Assert.Equal(LogLevel.Information, e.Level);
        Assert.Contains("dropped 2 jobs partitions [2026-10-01, 2026-10-02], 1 job_events partitions [2026-10-01] and 2 compaction remnants", e.Message);
    }

    [Fact]
    public void Pinned_partitions_are_summarised_in_one_information_line()
    {
        Write(Result(
            pinned: [new PinnedPartition(D, 1, 5, true), new PinnedPartition(D.AddDays(1), 0, 3, false)],
            deferred:
            [
                new DeferredPartition("jobs", D.AddDays(-1), DeferReason.LiveJobs, 2, 1001),
                new DeferredPartition("jobs", D.AddDays(2), DeferReason.Replicated, 0, 4),
            ]));

        var e = Assert.Single(_logger.At(LogLevel.Information));
        Assert.Contains("4 jobs partitions are pinned by live jobs: 1 compacted now, 1 left as is", e.Message);
        Assert.Contains("1 not compactable (more than 1000 live jobs), 1 skipped (logical replication); 1013 live jobs", e.Message);
        Assert.Contains("oldest day 2026-09-30", e.Message);
        Assert.Empty(_logger.At(LogLevel.Warning));   // просрочка меньше 7 суток
    }

    [Fact]
    public void Too_many_live_jobs_warn_per_partition_from_seven_overdue_days()
    {
        Write(Result(deferred:
        [
            new DeferredPartition("jobs", D, DeferReason.LiveJobs, 7, 1001),
            new DeferredPartition("jobs", D.AddDays(1), DeferReason.LiveJobs, 6, 1001),
        ]));

        var w = Assert.Single(_logger.At(LogLevel.Warning));
        Assert.Contains("jobs partition 2026-10-01 holds >1000 live jobs", w.Message);
    }

    [Fact]
    public void Replicated_partitions_warn_once_from_seven_overdue_days()
    {
        Write(Result(deferred:
        [
            new DeferredPartition("jobs", D, DeferReason.Replicated, 9, 3),
            new DeferredPartition("jobs", D.AddDays(1), DeferReason.Replicated, 8, 2),
        ]));

        var w = Assert.Single(_logger.At(LogLevel.Warning));
        Assert.Contains("compaction of 2 pinned jobs partitions is skipped because jobs is in a logical replication publication", w.Message);
    }

    [Theory]
    [InlineData(45, false)]
    [InlineData(46, true)]
    public void Attached_partitions_above_normal_plus_slack_warn(int attached, bool warns)
    {
        Write(Result(attached: attached));   // штатно 7 + 7 + 1 = 15, запас 30

        Assert.Equal(warns ? 1 : 0, _logger.At(LogLevel.Warning).Count(e => e.Message.Contains("jobs partitions are attached, normally 15")));
    }

    [Fact]
    public void No_partition_count_warning_without_jobs_retention()
    {
        _core.JobsRetention = null;
        Write(Result(attached: 1000));
        Assert.Empty(_logger.Entries);
    }

    [Fact]
    public void Hold_timeouts_warn_once_per_table()
    {
        Write(Result(deferred:
        [
            new DeferredPartition("jobs", D, DeferReason.HoldTimeout, 0, null),
            new DeferredPartition("jobs", D.AddDays(1), DeferReason.HoldTimeout, 0, null),
            new DeferredPartition("job_events", D, DeferReason.HoldTimeout, 0, null),
        ]));

        Assert.Equal(2, _logger.At(LogLevel.Warning).Count(e => e.Message.Contains("hit statement_timeout or idle_in_transaction_session_timeout")));
    }

    [Fact]
    public void Lock_timeouts_are_one_line_per_table_warning_only_when_overdue()
    {
        Write(Result(deferred:
        [
            new DeferredPartition("jobs", D, DeferReason.LockTimeout, 0, null),
            new DeferredPartition("jobs", D.AddDays(1), DeferReason.DropLockTimeout, 1, null),
            new DeferredPartition("job_events", D, DeferReason.LockTimeout, 0, null),
        ]));

        Assert.Contains("2 jobs partitions were deferred by lock_timeout (overdue by up to 1 days)", Assert.Single(_logger.At(LogLevel.Warning)).Message);
        Assert.Contains("1 job_events partitions were deferred by lock_timeout", Assert.Single(_logger.At(LogLevel.Debug)).Message);
    }

    [Fact]
    public void Drop_failure_is_an_error_with_its_text()
    {
        Write(Result(deferred: [new DeferredPartition("job_events", D, DeferReason.DropFailed, 3, null, "permission denied")]));

        var e = Assert.Single(_logger.At(LogLevel.Error));
        Assert.Contains("dropping the detached job_events table of 2026-10-01 failed: permission denied", e.Message);
    }

    [Fact]
    public void Deleted_batches_are_reported()
    {
        var logger = new ListLogger();

        RetentionReport.Write(logger, new RetentionResult([], [], 0, [], [], 3, DeletedBatches: 7), new PgBoltOptions(),
            TestTelemetry.Metrics(_instrumentation));

        Assert.Contains(logger.At(LogLevel.Information), e => e.Message.Contains("deleted 7 finished batches rows"));
    }

    [Fact]
    public void Deferred_counts_by_reason()
    {
        using var deferred = new MetricCollector<long>(_instrumentation.Meter, I.RetentionDeferred);

        Write(Result(deferred:
        [
            new DeferredPartition("jobs", D, DeferReason.LiveJobs, 0, 1001),
            new DeferredPartition("jobs", D.AddDays(1), DeferReason.LiveJobs, 0, 1001),
            new DeferredPartition("job_events", D, DeferReason.LockTimeout, 0, null),
        ]));

        Assert.Equal(2, TestTelemetry.Sum(deferred, PgBoltTelemetry.Attr.RetentionReason, nameof(DeferReason.LiveJobs)));
        Assert.Equal(1, TestTelemetry.Sum(deferred, PgBoltTelemetry.Attr.RetentionReason, nameof(DeferReason.LockTimeout)));
        Assert.Equal(3, deferred.GetMeasurementSnapshot().Sum(m => m.Value));
    }
}
