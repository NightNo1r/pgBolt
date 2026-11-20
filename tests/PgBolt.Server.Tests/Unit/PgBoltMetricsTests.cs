using System.Collections.Frozen;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;
using I = PgBolt.PgBoltDiagnostics.Instruments;

namespace PgBolt.Server.Tests.Unit;

[Collection(TelemetryCollection.Name)]
public sealed class PgBoltMetricsTests
{
    private sealed class Fixture : IDisposable
    {
        public FakeTimeProvider Time { get; } = new();
        public ListLogger Logs { get; } = new();
        public PgBoltInstrumentation Instrumentation { get; }
        public ClusterQueueStats Cluster { get; }
        public PgBoltMetrics Metrics { get; }

        public Fixture()
        {
            Instrumentation = TestTelemetry.Instrumentation(Time, Logs);
            Cluster = new ClusterQueueStats(new MonoClock(Time), new PgBoltProcessingOptions());
            Metrics = TestTelemetry.Metrics(Instrumentation, Cluster);
        }

        public MetricCollector<T> Collect<T>(string instrument) where T : struct => new(Instrumentation.Meter, instrument);

        /// <summary>Измерения observable-instrument'а за один сбор.</summary>
        public IReadOnlyList<CollectedMeasurement<T>> Observe<T>(string instrument) where T : struct
        {
            using var collector = Collect<T>(instrument);
            collector.RecordObservableInstruments();
            return collector.GetMeasurementSnapshot();
        }

        public void Dispose() => Instrumentation.Dispose();
    }

    private static SlotAllocator Slots(params string[] queues)
    {
        var o = new PgBoltServerOptions { Workers = 1 };
        foreach (var q in queues)
            o.Queue(q);
        return new SlotAllocator(o.Workers, o.Queues);
    }

    private static Func<QueueSettingsSnapshot> Limits(params (string Queue, int Limit)[] limits)
    {
        var snapshot = new QueueSettingsSnapshot(limits.ToFrozenDictionary(l => l.Queue, l => l.Limit, StringComparer.Ordinal),
            FrozenDictionary<string, int>.Empty);
        return () => snapshot;
    }

    private static string Tags<T>(CollectedMeasurement<T> m) where T : struct =>
        string.Join(",", m.Tags.OrderBy(t => t.Key, StringComparer.Ordinal).Select(t => $"{t.Key}={t.Value}"));

    private static string[] Rows<T>(IEnumerable<CollectedMeasurement<T>> measurements) where T : struct =>
        [.. measurements.Select(m => FormattableString.Invariant($"{Tags(m)}:{m.Value}")).Order(StringComparer.Ordinal)];

    private static string[] JobRows(string queue, long scheduled, long enqueued, long processing, long awaiting) =>
    [
        $"pgbolt.job.state=awaiting,pgbolt.queue={queue}:{awaiting}",
        $"pgbolt.job.state=enqueued,pgbolt.queue={queue}:{enqueued}",
        $"pgbolt.job.state=processing,pgbolt.queue={queue}:{processing}",
        $"pgbolt.job.state=scheduled,pgbolt.queue={queue}:{scheduled}",
    ];

    [Fact]
    public void Histograms_use_advised_buckets()
    {
        using var f = new Fixture();
        var seen = new Dictionary<string, Instrument>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, _) =>
        {
            if (instrument.Meter == f.Instrumentation.Meter)
                seen[instrument.Name] = instrument;
        };
        listener.Start();

        foreach (var (name, buckets) in new[]
                 {
                     (I.JobDuration, PgBoltDiagnostics.HistogramBuckets.Job),
                     (I.JobWaitTime, PgBoltDiagnostics.HistogramBuckets.Job),
                     (I.MaintenanceDuration, PgBoltDiagnostics.HistogramBuckets.Maintenance),
                 })
        {
            var histogram = Assert.IsType<Histogram<double>>(seen[name]);
            Assert.Equal("s", histogram.Unit);
            Assert.Equal(buckets, histogram.Advice?.HistogramBucketBoundaries);
        }
    }

    [Fact]
    public void Synchronous_instruments_have_expected_tags()
    {
        using var f = new Fixture();
        using var duration = f.Collect<double>(I.JobDuration);
        using var wait = f.Collect<double>(I.JobWaitTime);
        using var dropped = f.Collect<long>(I.JobOutcomeDropped);
        using var storage = f.Collect<long>(I.StorageErrors);
        using var runs = f.Collect<long>(I.MaintenanceRuns);
        using var maintenance = f.Collect<double>(I.MaintenanceDuration);
        using var fired = f.Collect<long>(I.RecurringFired);
        using var deferred = f.Collect<long>(I.RetentionDeferred);

        f.Metrics.JobDuration("q", "t", PgBoltMetrics.OutcomeName(new JobOutcome.Ack()), TimeSpan.FromSeconds(1.5));
        f.Metrics.JobWaitTime("q", "t", 2);
        f.Metrics.OutcomeDropped("q", DropReasons.LeaseLost);
        f.Metrics.StorageError(StorageOperations.Dequeue);
        f.Metrics.MaintenanceRun("stats", MaintenanceResults.Ok, TimeSpan.FromSeconds(0.25));
        f.Metrics.RecurringFired(4);
        f.Metrics.RetentionDeferred("lock_timeout", 2);

        Assert.Equal(["pgbolt.job.outcome=succeeded,pgbolt.job.type=t,pgbolt.queue=q:1.5"], Rows(duration.GetMeasurementSnapshot()));
        Assert.Equal(["pgbolt.job.type=t,pgbolt.queue=q:2"], Rows(wait.GetMeasurementSnapshot()));
        Assert.Equal(["pgbolt.drop.reason=lease_lost,pgbolt.queue=q:1"], Rows(dropped.GetMeasurementSnapshot()));
        Assert.Equal(["pgbolt.storage.operation=dequeue:1"], Rows(storage.GetMeasurementSnapshot()));
        Assert.Equal(["pgbolt.maintenance.result=ok,pgbolt.maintenance.task=stats:1"], Rows(runs.GetMeasurementSnapshot()));
        Assert.Equal(["pgbolt.maintenance.task=stats:0.25"], Rows(maintenance.GetMeasurementSnapshot()));
        Assert.Equal([":4"], Rows(fired.GetMeasurementSnapshot()));
        Assert.Equal(["pgbolt.retention.reason=lock_timeout:2"], Rows(deferred.GetMeasurementSnapshot()));

        Assert.Equal(["succeeded", "retry", "failed", "cancelled", "released"], new JobOutcome[]
        {
            new JobOutcome.Ack(),
            new JobOutcome.Retry(TimeSpan.Zero, JobEventKind.Retry, null, null),
            new JobOutcome.Fail(JobEventKind.Failed, null, null),
            new JobOutcome.ConfirmCancel(null),
            new JobOutcome.Release(false),
        }.Select(PgBoltMetrics.OutcomeName));
        Assert.Equal(["lease_lost", "abandoned", "stopped", "send_timeout"],
            [DropReasons.LeaseLost, DropReasons.Abandoned, DropReasons.Stopped, DropReasons.SendTimeout]);
        Assert.Equal(["ok", "error", "deferred"], [MaintenanceResults.Ok, MaintenanceResults.Error, MaintenanceResults.Deferred]);
        Assert.Equal(["dequeue", "ack", "outcome", "heartbeat", "settings", "stats_flush", "stats_read", "table_stats_read"],
        [
            StorageOperations.Dequeue, StorageOperations.Ack, StorageOperations.Outcome, StorageOperations.Heartbeat,
            StorageOperations.Settings, StorageOperations.StatsFlush, StorageOperations.StatsRead, StorageOperations.TableStatsRead,
        ]);
    }

    [Fact]
    public void Worker_gauges_follow_slot_allocator()
    {
        using var f = new Fixture();
        var o = new PgBoltServerOptions { Workers = 2 };
        o.Queue("q1", q => q.Reserved = 1).Queue("q2");
        var slots = new SlotAllocator(o.Workers, o.Queues);
        slots.Take("q1");
        using var _ = f.Metrics.RegisterServer("server", slots, 3, ["q1", "q2"], Limits());

        Assert.Equal(["pgbolt.queue=q1,pgbolt.server.name=server:1", "pgbolt.queue=q2,pgbolt.server.name=server:0"],
            Rows(f.Observe<int>(I.WorkerActive)));
        Assert.Equal(["pgbolt.server.name=server:3"], Rows(f.Observe<int>(I.WorkerCapacity)));
    }

    [Fact]
    public void Stopped_server_sources_are_not_published()
    {
        using var f = new Fixture();
        f.Metrics.RegisterServer("server", Slots("a"), 1, ["a"], Limits(("a", 2))).Dispose();
        f.Cluster.Publish(new QueueStatsSnapshot([new("a", 1, 1, 1, 1, 3)], 0));

        Assert.Empty(f.Observe<int>(I.WorkerActive));
        Assert.Empty(f.Observe<int>(I.WorkerCapacity));
        Assert.Empty(f.Observe<long>(I.ClusterQueueJobs));
        Assert.Empty(f.Observe<double>(I.ClusterQueueOldestReadyAge));
        Assert.Empty(f.Observe<int>(I.ClusterQueueLimit));
    }

    [Fact]
    public void Limit_gauge_publishes_effective_limits()
    {
        using var f = new Fixture();
        using var _ = f.Metrics.RegisterServer("server", Slots("a"), 1, ["a"], Limits(("a", 3), ("p", 0)));

        Assert.Equal(["pgbolt.queue=a:3", "pgbolt.queue=p:0"], Rows(f.Observe<int>(I.ClusterQueueLimit)));
    }

    [Fact]
    public void Fresh_empty_snapshot_publishes_zeros()
    {
        using var f = new Fixture();
        using var _ = f.Metrics.RegisterServer("server", Slots("a", "b"), 1, ["a", "b"], Limits());
        Assert.Empty(f.Observe<long>(I.ClusterQueueJobs));   // снимка ещё нет — ряды не публикуются

        f.Cluster.Publish(new QueueStatsSnapshot([], 0));

        Assert.Equal(JobRows("a", 0, 0, 0, 0).Concat(JobRows("b", 0, 0, 0, 0)).Order(StringComparer.Ordinal),
            Rows(f.Observe<long>(I.ClusterQueueJobs)));
        Assert.Empty(f.Observe<double>(I.ClusterQueueOldestReadyAge));
    }

    [Fact]
    public void Snapshot_queue_without_server_is_published()
    {
        using var f = new Fixture();
        using var _ = f.Metrics.RegisterServer("server", Slots("a"), 1, ["a"], Limits());

        f.Cluster.Publish(new QueueStatsSnapshot([new("c", 1, 2, 3, 4, null)], 0));

        Assert.Equal(JobRows("a", 0, 0, 0, 0).Concat(JobRows("c", 1, 2, 3, 4)).Order(StringComparer.Ordinal),
            Rows(f.Observe<long>(I.ClusterQueueJobs)));
    }

    [Fact]
    public void Oldest_ready_age_only_for_rows_with_value()
    {
        using var f = new Fixture();
        using var _ = f.Metrics.RegisterServer("server", Slots("a"), 1, ["a"], Limits());

        f.Cluster.Publish(new QueueStatsSnapshot([new("a", 0, 1, 0, 0, 7.5), new("b", 1, 0, 0, 0, null)], 0));

        Assert.Equal(["pgbolt.queue=a:7.5"], Rows(f.Observe<double>(I.ClusterQueueOldestReadyAge)));
    }

    [Fact]
    public void Shared_queue_of_two_servers_is_published_once()
    {
        using var f = new Fixture();
        var settings = Limits(("q", 2));
        using var s1 = f.Metrics.RegisterServer("s1", Slots("q"), 1, ["q"], settings);
        using var s2 = f.Metrics.RegisterServer("s2", Slots("q"), 1, ["q"], settings);

        f.Cluster.Publish(new QueueStatsSnapshot([], 0));

        Assert.Equal(JobRows("q", 0, 0, 0, 0), Rows(f.Observe<long>(I.ClusterQueueJobs)));
        Assert.Equal(["pgbolt.queue=q:2"], Rows(f.Observe<int>(I.ClusterQueueLimit)));
        Assert.Equal(["pgbolt.queue=q,pgbolt.server.name=s1:0", "pgbolt.queue=q,pgbolt.server.name=s2:0"],
            Rows(f.Observe<int>(I.WorkerActive)));
        Assert.Equal(["pgbolt.server.name=s1:1", "pgbolt.server.name=s2:1"], Rows(f.Observe<int>(I.WorkerCapacity)));
    }

    [Fact]
    public void Callback_exception_reports_8002()
    {
        using var f = new Fixture();
        using var _ = f.Metrics.RegisterServer("server", Slots("a"), 1, ["a"], () => throw new InvalidOperationException("settings"));

        Assert.Empty(f.Observe<int>(I.ClusterQueueLimit));
        Assert.Single(f.Logs.Entries, e => e.EventId.Id == 8002);
    }

    [Fact]
    public void Throwing_meter_listener_does_not_throw()
    {
        using var f = new Fixture();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter == f.Instrumentation.Meter)
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<double>((_, _, _, _) => throw new InvalidOperationException("listener"));
        listener.Start();

        f.Metrics.JobDuration("q", "t", "succeeded", TimeSpan.FromSeconds(1));

        Assert.Single(f.Logs.Entries, e => e.EventId.Id == 8001);
    }
}
