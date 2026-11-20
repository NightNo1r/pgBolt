using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class SweeperTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Expired_lease_with_attempts_left_returns_job_to_queue()
    {
        var id = await EnqueueAsync(maxAttempts: 3);
        await LeaseOneAsync();
        await ExpireLeaseAsync(id);

        Assert.Equal(1, await Store.SweepExpiredLeasesAsync(1000, Ct));

        Assert.True(await Db.ScalarAsync<bool>("SELECT lease_owner IS NULL AND lease_id IS NULL AND ready AND attempt = 1 FROM {schema}.queue WHERE job_id = @id", ("id", id)));
        Assert.Equal(new[] { (short)JobEventKind.LeaseExpired }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Expired_lease_with_exhausted_attempts_fails_job()
    {
        var id = await EnqueueAsync(maxAttempts: 1);
        await LeaseOneAsync();
        await ExpireLeaseAsync(id);

        await Store.SweepExpiredLeasesAsync(1000, Ct);

        Assert.False(await InQueueAsync(id));
        Assert.Equal((short)3, await JobStateAsync(id));
    }

    [Fact]
    public async Task Expired_lease_with_cancel_request_cancels_job()
    {
        var id = await EnqueueAsync(maxAttempts: 5);
        await LeaseOneAsync();
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id));
        await ExpireLeaseAsync(id);

        await Store.SweepExpiredLeasesAsync(1000, Ct);

        Assert.False(await InQueueAsync(id));
        Assert.Equal((short)4, await JobStateAsync(id));
        Assert.Equal(new[] { (short)JobEventKind.LeaseExpired, (short)JobEventKind.Cancelled }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Live_leases_are_untouched()
    {
        await EnqueueAsync();
        await LeaseOneAsync();
        Assert.Equal(0, await Store.SweepExpiredLeasesAsync(1000, Ct));
    }

    [Fact]
    public async Task Fencing_after_sweep_and_re_lease_by_same_server()
    {
        var id = await EnqueueAsync(maxAttempts: 5);
        var first = await LeaseOneAsync();
        await ExpireLeaseAsync(id);
        await Store.SweepExpiredLeasesAsync(1000, Ct);
        var second = await LeaseOneAsync();

        var hb = Assert.Single((await Store.HeartbeatAsync(ServerA, [new LeaseRef(id, first.LeaseId), new LeaseRef(id, second.LeaseId)], Lease, Ct)).Leases);
        Assert.Equal(second.LeaseId, hb.LeaseId);
        Assert.Equal(FailOutcome.LeaseLost, await Store.RetryAsync(id, first.LeaseId, ServerA, TimeSpan.Zero, JobEventKind.Retry, null, null, Ct));
        Assert.Empty(await Store.AckAsync(ServerA, [new LeaseRef(id, first.LeaseId)], Ct));
        Assert.Single(await Store.AckAsync(ServerA, [new LeaseRef(id, second.LeaseId)], Ct));
    }
}

public sealed class SweeperTests_Direct(PostgresFixture pg) : SweeperTests(pg, ConnectionMode.Direct);
public sealed class SweeperTests_Bouncer(PostgresFixture pg) : SweeperTests(pg, ConnectionMode.Bouncer);
