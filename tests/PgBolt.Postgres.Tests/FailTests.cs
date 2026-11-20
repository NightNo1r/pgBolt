using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class FailTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private Task RequestCancelAsync(Guid id) =>
        Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id));

    [Fact]
    public async Task Retry_without_delay_returns_job_ready_with_event()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();

        var outcome = await Store.RetryAsync(id, job.LeaseId, ServerA, TimeSpan.Zero, JobEventKind.Retry, "boom", "stack", Ct);

        Assert.Equal(FailOutcome.Retried, outcome);
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT ready AND lease_owner IS NULL AND lease_id IS NULL AND attempt = 1 FROM {schema}.queue WHERE job_id = @id
            """, ("id", id)));
        Assert.Equal(new[] { (short)JobEventKind.Retry }, await EventKindsAsync(id));
        Assert.Equal("stack", await Db.ScalarAsync<string>("SELECT exception FROM {schema}.job_events WHERE job_id = @id", ("id", id)));
        Assert.Equal((short)0, await JobStateAsync(id));
    }

    [Fact]
    public async Task Retry_with_delay_resets_ready()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        await Store.RetryAsync(id, job.LeaseId, ServerA, TimeSpan.FromMinutes(10), JobEventKind.Retry, null, null, Ct);
        Assert.False(await Db.ScalarAsync<bool>("SELECT ready FROM {schema}.queue WHERE job_id = @id", ("id", id)));
        Assert.Empty(await Store.DequeueAsync("q", 1, ServerA, Lease, Ct));
    }

    [Fact]
    public async Task Retry_with_lost_lease_does_nothing()
    {
        var id = await EnqueueAsync();
        await LeaseOneAsync();
        Assert.Equal(FailOutcome.LeaseLost, await Store.RetryAsync(id, Guid.NewGuid(), ServerA, TimeSpan.Zero, JobEventKind.Retry, null, null, Ct));
        Assert.Empty(await EventKindsAsync(id));
    }

    [Fact]
    public async Task Retry_when_cancel_requested_finalizes_as_cancelled()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        await RequestCancelAsync(id);

        Assert.Equal(FailOutcome.Cancelled, await Store.RetryAsync(id, job.LeaseId, ServerA, TimeSpan.Zero, JobEventKind.Retry, "x", null, Ct));
        Assert.False(await InQueueAsync(id));
        Assert.Equal((short)4, await JobStateAsync(id));
        Assert.Equal(new[] { (short)JobEventKind.Cancelled }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task NoExecutor_retry_writes_its_kind()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        await Store.RetryAsync(id, job.LeaseId, ServerA, TimeSpan.FromSeconds(30), JobEventKind.NoExecutor, null, null, Ct);
        Assert.Equal(new[] { (short)JobEventKind.NoExecutor }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Final_fail_marks_failed()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();

        Assert.Equal(FailOutcome.Failed, await Store.FailAsync(id, job.LeaseId, ServerA, JobEventKind.Failed, "bad", "trace", Ct));
        Assert.False(await InQueueAsync(id));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT state = 3 AND attempt = 1 AND started_at IS NOT NULL AND finished_at IS NOT NULL AND server_id = @s
            FROM {schema}.jobs WHERE id = @id
            """, ("s", ServerA), ("id", id)));
        Assert.Equal(new[] { (short)JobEventKind.Failed }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Final_fail_when_cancel_requested_marks_cancelled()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        await RequestCancelAsync(id);
        Assert.Equal(FailOutcome.Cancelled, await Store.FailAsync(id, job.LeaseId, ServerA, JobEventKind.Failed, null, null, Ct));
        Assert.Equal((short)4, await JobStateAsync(id));
        Assert.Equal(new[] { (short)JobEventKind.Cancelled }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Final_NoExecutor_marks_failed_with_its_kind()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        Assert.Equal(FailOutcome.Failed, await Store.FailAsync(id, job.LeaseId, ServerA, JobEventKind.NoExecutor, null, null, Ct));
        Assert.Equal((short)3, await JobStateAsync(id));
        Assert.Equal(new[] { (short)JobEventKind.NoExecutor }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task ConfirmCancel_marks_cancelled()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        Assert.Equal(FailOutcome.Cancelled, await Store.ConfirmCancelAsync(id, job.LeaseId, ServerA, "stopped", Ct));
        Assert.Equal((short)4, await JobStateAsync(id));
    }

    [Fact]
    public async Task Invalid_kinds_are_rejected()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            Store.RetryAsync(Guid.NewGuid(), Guid.NewGuid(), ServerA, TimeSpan.Zero, JobEventKind.Failed, null, null, Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            Store.FailAsync(Guid.NewGuid(), Guid.NewGuid(), ServerA, JobEventKind.Retry, null, null, Ct));
    }
}

public sealed class FailTests_Direct(PostgresFixture pg) : FailTests(pg, ConnectionMode.Direct);
public sealed class FailTests_Bouncer(PostgresFixture pg) : FailTests(pg, ConnectionMode.Bouncer);
