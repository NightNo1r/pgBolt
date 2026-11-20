using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Unit;

public sealed class ClusterQueueStatsTests
{
    [Fact]
    public void Staleness()
    {
        var time = new FakeTimeProvider();
        var stats = new ClusterQueueStats(new MonoClock(time),
            new PgBoltProcessingOptions { StatsSnapshotInterval = TimeSpan.FromSeconds(10) });
        QueueStatsRow[] rows = [new("a", 1, 2, 3, 4, null)];

        Assert.False(stats.TryGetFresh(out _));

        stats.Publish(new QueueStatsSnapshot(rows, null));
        Assert.False(stats.TryGetFresh(out _));

        stats.Publish(new QueueStatsSnapshot(rows, 5));
        time.Advance(TimeSpan.FromSeconds(34));   // возраст 39 с ≤ 4 × 10 с
        Assert.True(stats.TryGetFresh(out var fresh));
        Assert.Same(rows, fresh);

        time.Advance(TimeSpan.FromSeconds(2));    // 41 с
        Assert.False(stats.TryGetFresh(out _));

        stats.Publish(new QueueStatsSnapshot([], 0));
        Assert.True(stats.TryGetFresh(out fresh));
        Assert.Empty(fresh);
    }
}
