namespace PgBolt.Abstractions.Tests;

public sealed class PgBoltDiagnosticsTests
{
    [Fact]
    public void Names_and_buckets_are_stable()
    {
        Assert.Equal("PgBolt", PgBoltDiagnostics.ActivitySourceName);
        Assert.Equal("PgBolt", PgBoltDiagnostics.MeterName);

        Assert.Equal("pgbolt.job.enqueued", PgBoltDiagnostics.Instruments.JobEnqueued);
        Assert.Equal("pgbolt.recurring.fired", PgBoltDiagnostics.Instruments.RecurringFired);
        Assert.Equal("pgbolt.job.duration", PgBoltDiagnostics.Instruments.JobDuration);
        Assert.Equal("pgbolt.job.wait_time", PgBoltDiagnostics.Instruments.JobWaitTime);
        Assert.Equal("pgbolt.job.outcome.dropped", PgBoltDiagnostics.Instruments.JobOutcomeDropped);
        Assert.Equal("pgbolt.worker.active", PgBoltDiagnostics.Instruments.WorkerActive);
        Assert.Equal("pgbolt.worker.capacity", PgBoltDiagnostics.Instruments.WorkerCapacity);
        Assert.Equal("pgbolt.storage.errors", PgBoltDiagnostics.Instruments.StorageErrors);
        Assert.Equal("pgbolt.maintenance.runs", PgBoltDiagnostics.Instruments.MaintenanceRuns);
        Assert.Equal("pgbolt.maintenance.duration", PgBoltDiagnostics.Instruments.MaintenanceDuration);
        Assert.Equal("pgbolt.retention.deferred", PgBoltDiagnostics.Instruments.RetentionDeferred);
        Assert.Equal("pgbolt.cluster.queue.jobs", PgBoltDiagnostics.Instruments.ClusterQueueJobs);
        Assert.Equal("pgbolt.cluster.queue.oldest_ready_age", PgBoltDiagnostics.Instruments.ClusterQueueOldestReadyAge);
        Assert.Equal("pgbolt.cluster.queue.limit", PgBoltDiagnostics.Instruments.ClusterQueueLimit);
        Assert.Equal("pgbolt.cluster.table.dead_tuples", PgBoltDiagnostics.Instruments.ClusterTableDeadTuples);
        Assert.Equal("pgbolt.cluster.table.size", PgBoltDiagnostics.Instruments.ClusterTableSize);

        Assert.Equal(
            [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600],
            PgBoltDiagnostics.HistogramBuckets.Job);
        Assert.Equal(
            [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300],
            PgBoltDiagnostics.HistogramBuckets.Maintenance);
        AssertStrictlyAscending(PgBoltDiagnostics.HistogramBuckets.Job);
        AssertStrictlyAscending(PgBoltDiagnostics.HistogramBuckets.Maintenance);
    }

    private static void AssertStrictlyAscending(IReadOnlyList<double> values)
    {
        for (var i = 1; i < values.Count; i++)
            Assert.True(values[i] > values[i - 1], $"bucket {i}: {values[i]} <= {values[i - 1]}");
    }
}
