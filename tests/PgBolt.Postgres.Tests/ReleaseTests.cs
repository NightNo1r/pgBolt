using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class ReleaseTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private Task RequestCancelAsync(Guid id) =>
        Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id));

    private Task<DateTime> LeasedAtAsync(Guid id) =>
        Db.ScalarAsync<DateTime>("SELECT leased_at FROM {schema}.queue WHERE job_id = @id", ("id", id));

    /// <summary>Вторая попытка: attempt до dequeue = 1, после dequeue = 2.</summary>
    private async Task<(Guid Id, LeasedJob First, LeasedJob Second)> SecondAttemptAsync()
    {
        var id = await EnqueueAsync(maxAttempts: 5);
        var first = await LeaseOneAsync();
        Assert.Equal(FailOutcome.Retried, await Store.RetryAsync(id, first.LeaseId, ServerA, TimeSpan.Zero, JobEventKind.Retry, "boom", null, Ct));
        var second = await LeaseOneAsync();
        Assert.Equal(2, second.Attempt);
        return (id, first, second);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Release_returns_job_without_spending_attempt(bool ran)
    {
        var (id, _, second) = await SecondAttemptAsync();
        var runAt = await Db.ScalarAsync<DateTime>("SELECT run_at FROM {schema}.queue WHERE job_id = @id", ("id", id));

        Assert.Equal(ReleaseOutcome.Released, await Store.ReleaseAsync(id, second.LeaseId, ServerA, ran, "stopping", Ct));

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT attempt = 1 AND ready AND lease_owner IS NULL AND lease_id IS NULL AND lease_until IS NULL
                   AND leased_at IS NULL AND NOT cancel_requested AND run_at = @run_at
            FROM {schema}.queue WHERE job_id = @id
            """, ("id", id), ("run_at", runAt)));
        Assert.Equal((short)0, await JobStateAsync(id));                                // jobs не тронута: Pending
        Assert.Equal(new[] { (short)JobEventKind.Retry }, await EventKindsAsync(id));   // release событий не пишет

        var again = await LeaseOneAsync(server: ServerB);                               // сразу доступна другому серверу
        Assert.Equal((id, 2), (again.Id, again.Attempt));
    }

    [Fact]
    public async Task Release_after_run_of_cancel_requested_job_cancels_with_interrupted_attempt()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        var leasedAt = await LeasedAtAsync(id);
        await RequestCancelAsync(id);

        Assert.Equal(ReleaseOutcome.Cancelled, await Store.ReleaseAsync(id, job.LeaseId, ServerA, ran: true, "stopping", Ct));

        Assert.False(await InQueueAsync(id));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT state = 4 AND attempt = 1 AND started_at = @leased_at AND finished_at IS NOT NULL AND server_id = @s
            FROM {schema}.jobs WHERE id = @id
            """, ("id", id), ("leased_at", leasedAt), ("s", ServerA)));
        Assert.Equal(new[] { (short)JobEventKind.Cancelled }, await EventKindsAsync(id));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT attempt = 1 AND server_id = @s AND message = 'stopping' AND exception IS NULL
            FROM {schema}.job_events WHERE job_id = @id
            """, ("id", id), ("s", ServerA)));
    }

    [Fact]
    public async Task Release_of_unstarted_cancel_requested_job_cancels_without_phantom_attempt()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        Assert.Equal(1, job.Attempt);
        await RequestCancelAsync(id);

        Assert.Equal(ReleaseOutcome.Cancelled, await Store.ReleaseAsync(id, job.LeaseId, ServerA, ran: false, null, Ct));

        Assert.False(await InQueueAsync(id));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT state = 4 AND attempt = 0 AND started_at IS NULL AND finished_at IS NOT NULL AND server_id = @s
            FROM {schema}.jobs WHERE id = @id
            """, ("id", id), ("s", ServerA)));
        Assert.Equal(new[] { (short)JobEventKind.Cancelled }, await EventKindsAsync(id));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT attempt = 0 AND server_id = @s AND message IS NULL
            FROM {schema}.job_events WHERE job_id = @id
            """, ("id", id), ("s", ServerA)));
    }

    [Fact]
    public async Task Release_of_unstarted_second_attempt_keeps_previous_attempt_and_started_at()
    {
        var (id, _, second) = await SecondAttemptAsync();
        await RequestCancelAsync(id);

        Assert.Equal(ReleaseOutcome.Cancelled, await Store.ReleaseAsync(id, second.LeaseId, ServerA, ran: false, null, Ct));

        // attempt − 1 = 1 (последняя реально запускавшаяся попытка); started_at не меняется (retry его не пишет — NULL)
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT state = 4 AND attempt = 1 AND started_at IS NULL FROM {schema}.jobs WHERE id = @id
            """, ("id", id)));
        Assert.Equal(new[] { (short)JobEventKind.Retry, (short)JobEventKind.Cancelled }, await EventKindsAsync(id));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Stale_or_foreign_lease_is_lease_lost_and_row_is_untouched(bool cancelRequested)
    {
        var (id, first, second) = await SecondAttemptAsync();
        if (cancelRequested)
            await RequestCancelAsync(id);

        Assert.Equal(ReleaseOutcome.LeaseLost, await Store.ReleaseAsync(id, first.LeaseId, ServerA, ran: true, null, Ct));
        Assert.Equal(ReleaseOutcome.LeaseLost, await Store.ReleaseAsync(id, Guid.NewGuid(), ServerA, ran: false, null, Ct));

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT lease_owner = @s AND lease_id = @lease AND attempt = 2 AND lease_until IS NOT NULL
            FROM {schema}.queue WHERE job_id = @id
            """, ("id", id), ("s", ServerA), ("lease", second.LeaseId)));
        Assert.Equal((short)0, await JobStateAsync(id));
        Assert.Equal(new[] { (short)JobEventKind.Retry }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Unknown_job_is_lease_lost()
    {
        Assert.Equal(ReleaseOutcome.LeaseLost, await Store.ReleaseAsync(Guid.CreateVersion7(), Guid.NewGuid(), ServerA, ran: true, null, Ct));
    }
}

public sealed class ReleaseTests_Direct(PostgresFixture pg) : ReleaseTests(pg, ConnectionMode.Direct);
public sealed class ReleaseTests_Bouncer(PostgresFixture pg) : ReleaseTests(pg, ConnectionMode.Bouncer);
