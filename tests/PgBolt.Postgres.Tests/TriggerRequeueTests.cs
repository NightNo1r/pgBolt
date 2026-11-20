using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class TriggerRequeueTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private async Task<Guid> FailedJobAsync(string? uniqueKey = null)
    {
        var id = await EnqueueAsync(maxAttempts: 1, uniqueKey: uniqueKey);
        var job = await LeaseOneAsync();
        Assert.Equal(FailOutcome.Failed, await Store.FailAsync(id, job.LeaseId, ServerA, JobEventKind.Failed, null, null, Ct));
        return id;
    }

    [Fact]
    public async Task Trigger_makes_scheduled_job_ready_now()
    {
        var id = await EnqueueAsync(queue: "mail", delay: TimeSpan.FromHours(1));
        Assert.Equal("mail", await Store.TriggerAsync(id, Ct));
        Assert.True(await Db.ScalarAsync<bool>("SELECT ready AND abs(extract(epoch FROM run_at - now())) < 5 FROM {schema}.queue WHERE job_id = @id", ("id", id)));
        Assert.Equal(new[] { (short)JobEventKind.Triggered }, await EventKindsAsync(id));
        Assert.Single(await Store.DequeueAsync("mail", 1, ServerA, Lease, Ct));
    }

    [Fact]
    public async Task Trigger_on_ready_leased_or_missing_job_returns_null()
    {
        var ready = await EnqueueAsync();
        Assert.Null(await Store.TriggerAsync(ready, Ct));
        await LeaseOneAsync();
        Assert.Null(await Store.TriggerAsync(ready, Ct));
        Assert.Null(await Store.TriggerAsync(Guid.CreateVersion7(), Ct));
        Assert.Empty(await EventKindsAsync(ready));
    }

    [Fact]
    public async Task Requeue_failed_job()
    {
        var id = await FailedJobAsync();
        Assert.Equal((RequeueResult.Requeued, "q"), await Store.RequeueAsync(id, Ct));
        Assert.True(await Db.ScalarAsync<bool>("SELECT ready AND attempt = 0 AND lease_owner IS NULL FROM {schema}.queue WHERE job_id = @id", ("id", id)));
        Assert.True(await Db.ScalarAsync<bool>("SELECT state = 0 AND finished_at IS NULL FROM {schema}.jobs WHERE id = @id", ("id", id)));
        Assert.Equal(new[] { (short)JobEventKind.Failed, (short)JobEventKind.Requeued }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Requeue_cancelled_job()
    {
        var id = await EnqueueAsync();
        await Store.CancelAsync(id, null, Ct);
        Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(id, Ct)).Result);
    }

    [Fact]
    public async Task Requeue_non_terminal_or_unknown()
    {
        var pending = await EnqueueAsync();
        Assert.Equal(RequeueResult.NotTerminal, (await Store.RequeueAsync(pending, Ct)).Result);
        Assert.Equal(RequeueResult.NotFound, (await Store.RequeueAsync(Guid.CreateVersion7(), Ct)).Result);
    }

    [Fact]
    public async Task Requeue_conflicts_with_active_unique_key()
    {
        var failed = await FailedJobAsync(uniqueKey: "k");
        await EnqueueAsync(uniqueKey: "k");
        Assert.Equal(RequeueResult.Conflict, (await Store.RequeueAsync(failed, Ct)).Result);
        Assert.Equal((short)3, await JobStateAsync(failed));
    }

    [Fact]
    public async Task Succeeded_after_requeue_is_not_requeued_again()
    {
        var id = await FailedJobAsync();
        await Store.RequeueAsync(id, Ct);
        var job = await LeaseOneAsync();
        await Store.AckAsync(ServerA, [new LeaseRef(id, job.LeaseId)], Ct);
        Assert.Equal(RequeueResult.NotTerminal, (await Store.RequeueAsync(id, Ct)).Result);
        Assert.False(await InQueueAsync(id));
    }

    [Fact]
    public async Task Concurrent_requeues_requeue_exactly_once()
    {
        for (var i = 0; i < 20; i++)
        {
            var id = await FailedJobAsync();
            var results = await Task.WhenAll(Store.RequeueAsync(id, Ct), Store.RequeueAsync(id, Ct));
            Assert.Single(results, r => r.Result == RequeueResult.Requeued);
            Assert.Single(results, r => r.Result == RequeueResult.NotTerminal);
            await Db.ExecAsync("DELETE FROM {schema}.queue WHERE job_id = @id", ("id", id));
        }
    }
}

public sealed class TriggerRequeueTests_Direct(PostgresFixture pg) : TriggerRequeueTests(pg, ConnectionMode.Direct);
public sealed class TriggerRequeueTests_Bouncer(PostgresFixture pg) : TriggerRequeueTests(pg, ConnectionMode.Bouncer);
