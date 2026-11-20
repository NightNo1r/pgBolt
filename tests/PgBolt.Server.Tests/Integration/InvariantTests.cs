using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

public abstract class InvariantTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    [Fact]
    public async Task Three_processes_with_two_servers_each_execute_every_job_exactly_once()
    {
        const int perQueue = 1500;
        for (var p = 0; p < 3; p++)
        {
            var name = $"P{p}";
            await StartHostAsync(h =>
            {
                h.Name = name;
                h.Configure = ServerTestHost.SlowHeartbeat;
                h.AddServer("a", s => s.Queue("q1", ServerTestHost.FastQueue).Queue("q2", ServerTestHost.FastQueue));
                h.AddServer("b", s => s.Queue("q2", ServerTestHost.FastQueue).Queue("q1", ServerTestHost.FastQueue));
            });
        }

        foreach (var queue in new[] { "q1", "q2" })
            for (var i = 0; i < perQueue; i += 500)
                await Client.EnqueueManyAsync(Enumerable.Range(i, 500).Select(n => new SuccessArgs(n)).ToList(),
                    new EnqueueOptions { Queue = queue }, null, Ct);

        await WaitUntilAsync(async () => await CountAsync("SELECT count(*) FROM {schema}.jobs WHERE state = 2") == 2 * perQueue,
            TimeSpan.FromSeconds(120), "all jobs succeed");

        var counts = Probe.RunCounts();
        Assert.Equal(2 * perQueue, counts.Count);
        Assert.All(counts, c => Assert.Equal(1, c.Value));
        Assert.Equal(0L, await CountAsync("SELECT count(*) FROM {schema}.queue"));
        Assert.Equal(0L, await CountAsync("SELECT count(*) FROM {schema}.job_events"));
        Assert.Equal((long)(2 * perQueue), await CountAsync("SELECT count(*) FROM {schema}.jobs"));
        Assert.True(await CountAsync("SELECT count(DISTINCT server_id) FROM {schema}.jobs") >= 2,
            "jobs must be spread over several servers for the invariant to be exercised");
    }
}

public sealed class InvariantTests_Direct(PostgresFixture pg) : InvariantTests(pg, ConnectionMode.Direct);
public sealed class InvariantTests_Bouncer(PostgresFixture pg) : InvariantTests(pg, ConnectionMode.Bouncer);
