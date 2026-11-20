using System.Collections.Concurrent;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class CancelTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Pending_job_is_cancelled_immediately()
    {
        var id = await EnqueueAsync();
        Assert.Equal(CancelResult.Cancelled, await Store.CancelAsync(id, "no longer needed", Ct));
        Assert.False(await InQueueAsync(id));
        Assert.Equal((short)4, await JobStateAsync(id));
        Assert.Equal(new[] { (short)JobEventKind.Cancelled }, await EventKindsAsync(id));
        Assert.Equal("no longer needed", await Db.ScalarAsync<string>("SELECT message FROM {schema}.job_events WHERE job_id = @id", ("id", id)));
    }

    [Fact]
    public async Task Delayed_job_is_cancelled_immediately()
    {
        var id = await EnqueueAsync(delay: TimeSpan.FromHours(1));
        Assert.Equal(CancelResult.Cancelled, await Store.CancelAsync(id, null, Ct));
        Assert.Equal((short)4, await JobStateAsync(id));
    }

    [Fact]
    public async Task Leased_job_gets_cancellation_request()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        Assert.Equal(CancelResult.CancellationRequested, await Store.CancelAsync(id, null, Ct));
        Assert.True(await Db.ScalarAsync<bool>("SELECT cancel_requested FROM {schema}.queue WHERE job_id = @id", ("id", id)));
        Assert.True(Assert.Single((await Store.HeartbeatAsync(ServerA, [new LeaseRef(id, job.LeaseId)], Lease, Ct)).Leases).CancelRequested);
    }

    [Fact]
    public async Task Finished_job_reports_already_finished()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        await Store.AckAsync(ServerA, [new LeaseRef(id, job.LeaseId)], Ct);
        Assert.Equal(CancelResult.AlreadyFinished, await Store.CancelAsync(id, null, Ct));
    }

    [Fact]
    public async Task Unknown_job_reports_not_found()
    {
        Assert.Equal(CancelResult.NotFound, await Store.CancelAsync(Guid.CreateVersion7(), null, Ct));
    }

    [Fact]
    public async Task Cancel_racing_dequeue_never_loses_a_leased_job()
    {
        var ids = new List<Guid>();
        for (var i = 0; i < 100; i++)
            ids.Add(await EnqueueAsync());

        var leased = new ConcurrentDictionary<Guid, bool>();
        var dequeuer = Task.Run(async () =>
        {
            for (var empty = 0; empty < 3;)
            {
                var batch = await Store.DequeueAsync("q", 5, ServerA, Lease, Ct);
                if (batch.Count == 0) { empty++; continue; }
                foreach (var j in batch)
                    leased[j.Id] = true;
            }
        }, Ct);
        var results = await Task.WhenAll(ids.Select(id => Store.CancelAsync(id, null, Ct)));
        await dequeuer;

        for (var i = 0; i < ids.Count; i++)
        {
            if (leased.ContainsKey(ids[i]))
            {
                Assert.Equal(CancelResult.CancellationRequested, results[i]);
                Assert.True(await Db.ScalarAsync<bool>("SELECT cancel_requested FROM {schema}.queue WHERE job_id = @id", ("id", ids[i])));
            }
            else
            {
                Assert.Equal(CancelResult.Cancelled, results[i]);
                Assert.Equal((short)4, await JobStateAsync(ids[i]));
            }
        }
    }
}

public sealed class CancelTests_Direct(PostgresFixture pg) : CancelTests(pg, ConnectionMode.Direct);
public sealed class CancelTests_Bouncer(PostgresFixture pg) : CancelTests(pg, ConnectionMode.Bouncer);
