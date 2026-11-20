using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Unit;

public sealed class OutcomeStatsTests
{
    private static readonly DateTime Minute0 = new(2026, 10, 5, 12, 0, 0, DateTimeKind.Utc);

    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 10, 5, 12, 0, 30, TimeSpan.Zero));

    [Fact]
    public void Record_buckets_by_utc_minute_of_time_provider()
    {
        var stats = new OutcomeStats(_time);
        var s = Guid.NewGuid();

        stats.Record(s, "q", succeeded: 2);
        _time.Advance(TimeSpan.FromSeconds(40));
        stats.Record(s, "q", failed: 1);

        var rows = stats.Take();
        Assert.Equal(
            [new OutcomeStatsRow(Minute0, s, "q", 2, 0, 0), new OutcomeStatsRow(Minute0.AddMinutes(1), s, "q", 0, 1, 0)],
            rows);
        Assert.All(rows, r => Assert.Equal(DateTimeKind.Utc, r.BucketUtc.Kind));
    }

    [Fact]
    public void Take_empties_and_restore_merges_with_new_counts()
    {
        var stats = new OutcomeStats(_time);
        var s = Guid.NewGuid();

        stats.Record(s, "q", succeeded: 2);
        var rows = stats.Take();
        stats.Record(s, "q", succeeded: 1);
        stats.Restore(rows);

        Assert.Equal([new OutcomeStatsRow(Minute0, s, "q", 3, 0, 0)], stats.Take());
        Assert.Empty(stats.Take());
    }

    [Fact]
    public void Servers_and_queues_are_separate_keys()
    {
        var stats = new OutcomeStats(_time);
        Guid[] servers = [Guid.NewGuid(), Guid.NewGuid()];

        foreach (var server in servers)
        {
            stats.Record(server, "a", succeeded: 1);
            stats.Record(server, "b", retried: 1);
        }

        var rows = stats.Take();
        Assert.Equal(4, rows.Count);
        Assert.Equal(4, rows.Select(r => (r.ServerId, r.Queue)).Distinct().Count());
    }

    [Fact]
    public async Task Record_is_thread_safe()
    {
        var stats = new OutcomeStats(_time);
        var s = Guid.NewGuid();

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            for (var i = 0; i < 10_000; i++)
                stats.Record(s, "q", succeeded: 1);
        }, TestContext.Current.CancellationToken)));

        Assert.Equal(80_000, Assert.Single(stats.Take()).Succeeded);
    }
}
