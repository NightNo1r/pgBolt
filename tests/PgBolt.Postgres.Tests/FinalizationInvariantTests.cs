using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 6 §6.1, §12: каждый путь финализации Failed / Cancelled пишет jobs.finished_at = job_events.at.</summary>
public abstract class FinalizationInvariantTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    /// <summary>jobs.state = state и есть событие из kinds с at = jobs.finished_at.</summary>
    private Task<bool> TerminalEventMatchesAsync(Guid id, short state, params short[] kinds) =>
        Db.ScalarAsync<bool>("""
            SELECT j.state = @state AND j.finished_at IS NOT NULL
                   AND EXISTS (SELECT 1 FROM {schema}.job_events e WHERE e.job_id = j.id AND e.kind = ANY(@kinds) AND e.at = j.finished_at)
            FROM {schema}.jobs j WHERE j.id = @id
            """, ("id", id), ("state", state), ("kinds", kinds));

    /// <summary>Lease задачи и запрос отмены (cancel_requested); возвращает lease_id.</summary>
    private async Task<Guid> LeaseAndRequestCancelAsync(Guid id)
    {
        var job = await LeaseOneAsync();
        Assert.Equal(CancelResult.CancellationRequested, await Store.CancelAsync(id, null, Ct));
        return job.LeaseId;
    }

    [Fact]
    public async Task Fail_writes_finished_at_and_failed_event_together()
    {
        var id = await EnqueueAsync();
        await FailFinallyAsync(id);
        Assert.True(await TerminalEventMatchesAsync(id, 3, 2));
    }

    [Fact]
    public async Task No_executor_fail_matches_event_7()
    {
        var id = await EnqueueAsync();
        var lease = await LeaseAsync(id);
        Assert.Equal(FailOutcome.Failed, await Flow.FailAsync(id, lease, ServerA, JobEventKind.NoExecutor, "no executor", null, Ct));
        Assert.True(await TerminalEventMatchesAsync(id, 3, 7));
    }

    [Fact]
    public async Task Sweeper_on_last_attempt_matches_event_3()
    {
        var id = await EnqueueAsync(maxAttempts: 1);
        await LeaseOneAsync();
        await ExpireLeaseAsync(id);

        Assert.Equal(1, await Store.SweepExpiredLeasesAsync(100, Ct));
        Assert.True(await TerminalEventMatchesAsync(id, 3, 3));
    }

    [Fact]
    public async Task Sweeper_with_cancel_request_matches_event_4_and_3_shares_at()
    {
        var id = await EnqueueAsync();
        await LeaseAndRequestCancelAsync(id);
        await ExpireLeaseAsync(id);

        Assert.Equal(1, await Store.SweepExpiredLeasesAsync(100, Ct));
        Assert.True(await TerminalEventMatchesAsync(id, 4, 4));
        Assert.True(await TerminalEventMatchesAsync(id, 4, 3));
    }

    [Fact]
    public async Task Retry_with_cancel_request_matches_event_4()
    {
        var id = await EnqueueAsync();
        var lease = await LeaseAndRequestCancelAsync(id);

        Assert.Equal(FailOutcome.Cancelled, await Store.RetryAsync(id, lease, ServerA, TimeSpan.Zero, JobEventKind.Retry, "boom", null, Ct));
        Assert.True(await TerminalEventMatchesAsync(id, 4, 4));
    }

    [Fact]
    public async Task Cancel_of_pending_job_matches_event_4()
    {
        var id = await EnqueueAsync();
        await CancelJobAsync(id);
        Assert.True(await TerminalEventMatchesAsync(id, 4, 4));
    }

    [Fact]
    public async Task Release_with_cancel_request_matches_event_4()
    {
        var id = await EnqueueAsync();
        var lease = await LeaseAndRequestCancelAsync(id);

        Assert.Equal(ReleaseOutcome.Cancelled, await Store.ReleaseAsync(id, lease, ServerA, ran: true, null, Ct));
        Assert.True(await TerminalEventMatchesAsync(id, 4, 4));
    }

    [Fact]
    public async Task Batch_cancel_matches_event_4_for_members()
    {
        IJobStep? first = null, second = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            first = b.Enqueue(A(1));
            second = b.Enqueue(A(2));
        });
        await batch.SubmitAsync(Ct);

        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch.Id, Ct));
        await ResolveAllAsync();

        Assert.True(await TerminalEventMatchesAsync(first!.Id.Value, 4, 4));
        Assert.True(await TerminalEventMatchesAsync(second!.Id.Value, 4, 4));
    }

    [Fact]
    public async Task Resolver_cascade_cancel_matches_event_4()
    {
        var parent = Client.Workflow().Enqueue(A(1));
        var continuation = parent.ContinueWith(A(2));
        await parent.SubmitAsync(Ct);

        await CancelJobAsync(parent.Id.Value);   // каскад — только при Cancelled-родителе (часть 5 §2.2)
        await ResolveAllAsync();

        Assert.True(await TerminalEventMatchesAsync(continuation.Id.Value, 4, 4));
    }

    [Fact]
    public async Task Requeue_resets_finished_at()
    {
        var id = await EnqueueAsync();
        await FailFinallyAsync(id);

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(new JobId(id), Ct));
        Assert.True(await Db.ScalarAsync<bool>("SELECT finished_at IS NULL AND state = 0 FROM {schema}.jobs WHERE id = @id", ("id", id)));

        await FailFinallyAsync(id);
        Assert.True(await TerminalEventMatchesAsync(id, 3, 2));
        Assert.Equal(1L, await Db.ScalarAsync<long>("""
            SELECT count(*) FROM {schema}.job_events e JOIN {schema}.jobs j ON j.id = e.job_id
            WHERE e.job_id = @id AND e.kind = 2 AND e.at = j.finished_at
            """, ("id", id)));
    }
}

public sealed class FinalizationInvariantTests_Direct(PostgresFixture pg) : FinalizationInvariantTests(pg, ConnectionMode.Direct);
public sealed class FinalizationInvariantTests_Bouncer(PostgresFixture pg) : FinalizationInvariantTests(pg, ConnectionMode.Bouncer);
