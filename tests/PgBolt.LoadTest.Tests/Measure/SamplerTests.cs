using PgBolt.LoadTest.Measure;
using PgBolt.Testing;

namespace PgBolt.LoadTest.Tests.Measure;

[Trait("Category", "Load")]
[Collection("Load")]
public sealed class SamplerTests(PostgresFixture pg)
{
    [Fact]
    public async Task Every_query_of_a_sample_runs_on_migrated_schema()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        var sampler = new Sampler(pg.DirectConnectionString, db.Schema, console: null, ["pgbolt-load"], () => 100, () => 5);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(2.5));

        await sampler.RunAsync(cts.Token);

        Assert.Equal(0, sampler.FailedQueries);
        var s = sampler.Samples[0];
        Assert.Equal(0, s.ReadyBacklog);
        Assert.True(s.QueueTotalBytes > 0);
        Assert.NotEmpty(s.QueueIndexBytes!);
        Assert.Equal(0, s.Connections);
        Assert.Equal(0, s.LockManagerWaits);
        Assert.NotNull(s.AutovacuumWorkers);
        Assert.Equal(5, s.Completed);
        Assert.NotNull(s.Queue);
        Assert.NotNull(s.Completions);
        Assert.NotNull(s.WalBytes);
        Assert.True(s.DatabaseBytes > 0);
        Assert.NotNull(s.Checkpointer);
        Assert.NotNull(s.Vacuums);
        Assert.Equal(0, s.Delayed);
        Assert.Equal(0, s.Leased);
        Assert.NotNull(s.QueryTime);
        Assert.NotNull(s.CountsTime);
        Assert.Null(s.MaxWaitMicros);
    }
}
