using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §6.3: дополнение финализации, Awaiting в dequeue / promoter / trigger / GetJob / cancel.</summary>
public abstract class WorkflowHotPathTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    /// <summary>Pending-задача с подписчиками: queue.notify = true; batch — jobs.batch_id и queue.batch_id.</summary>
    private async Task<Guid> SubscribedAsync(Guid? batch = null, int maxAttempts = 3)
    {
        var id = await EnqueueAsync(maxAttempts: maxAttempts);
        await Db.ExecAsync("UPDATE {schema}.queue SET notify = true, batch_id = @b WHERE job_id = @id", ("id", id), ("b", batch));
        await Db.ExecAsync("UPDATE {schema}.jobs SET batch_id = @b WHERE id = @id", ("id", id), ("b", batch));
        return id;
    }

    private Task<string?> SignalRowsAsync() =>
        Db.ScalarAsync<string?>("""
            SELECT string_agg(node_id || ':' || kind || ':' || coalesce(batch_id::text, '-') || ':' || counted || ':' || stage, ',' ORDER BY id)
            FROM {schema}.completions
            """);

    [Fact]
    public async Task Ack_with_notify_writes_signal_with_batch_and_wakes_resolver()
    {
        var batch = Guid.CreateVersion7();
        var id = await SubscribedAsync(batch);
        var lease = await LeaseAsync(id);

        Assert.Single(await Flow.AckAsync(ServerA, [new LeaseRef(id, lease)], Ct));

        Assert.Equal($"{id}:2:{batch}:false:0", await SignalRowsAsync());
        Assert.True(await Db.ScalarAsync<bool>("SELECT notify FROM {schema}.jobs WHERE id = @id", ("id", id)));
        Assert.Equal(1, Signal.Count);
    }

    [Fact]
    public async Task Finalization_without_notify_writes_no_signal()   // §12 «Горячий путь»
    {
        var acked = await EnqueueAsync();
        var failed = await EnqueueAsync(maxAttempts: 1);
        var cancelled = await EnqueueAsync(delay: TimeSpan.FromHours(1));
        var a = await LeaseAsync(acked);
        var f = await LeaseAsync(failed);

        Assert.Single(await Flow.AckAsync(ServerA, [new LeaseRef(acked, a)], Ct));
        Assert.Equal(FailOutcome.Failed, await Flow.FailAsync(failed, f, ServerA, JobEventKind.Failed, "x", null, Ct));
        Assert.Equal(CancelResult.Cancelled, await Flow.CancelAsync(cancelled, null, Ct));

        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions"));
        Assert.Equal(0, Signal.Count);
    }

    [Theory]
    [InlineData(false, false, 3)]   // окончательный fail
    [InlineData(true, false, 4)]    // fail при запрошенной отмене
    [InlineData(false, true, 4)]    // подтверждение отмены
    public async Task Final_fail_and_confirm_cancel_signal_final_state(bool cancelRequested, bool confirm, short kind)
    {
        var id = await SubscribedAsync();
        var lease = await LeaseAsync(id);
        if (cancelRequested)
            await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id));

        _ = confirm
            ? await Flow.ConfirmCancelAsync(id, lease, ServerA, "stop", Ct)
            : await Flow.FailAsync(id, lease, ServerA, JobEventKind.Failed, "x", null, Ct);

        Assert.Equal($"{id}:{kind}:-:false:0", await SignalRowsAsync());
        Assert.Equal(kind, await JobStateAsync(id));
        Assert.Equal(1, Signal.Count);
    }

    [Fact]
    public async Task Retry_signals_only_its_cancel_branch()
    {
        var retried = await SubscribedAsync();
        var r = await LeaseAsync(retried);
        Assert.Equal(FailOutcome.Retried, await Flow.RetryAsync(retried, r, ServerA, TimeSpan.Zero, JobEventKind.Retry, "x", null, Ct));
        Assert.Null(await SignalRowsAsync());
        Assert.True(await Db.ScalarAsync<bool>("SELECT notify FROM {schema}.queue WHERE job_id = @id", ("id", retried)));   // строка осталась вместе с notify

        var c = await LeaseAsync(retried);
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", retried));
        Assert.Equal(FailOutcome.Cancelled, await Flow.RetryAsync(retried, c, ServerA, TimeSpan.Zero, JobEventKind.Retry, "x", null, Ct));

        Assert.Equal($"{retried}:4:-:false:0", await SignalRowsAsync());
    }

    [Fact]
    public async Task Sweeper_signals_final_and_cancelled_expiry_but_not_reset()
    {
        var exhausted = await SubscribedAsync(maxAttempts: 1);
        var cancelled = await SubscribedAsync();
        var reset = await SubscribedAsync();
        foreach (var id in new[] { exhausted, cancelled, reset })
        {
            await LeaseAsync(id);
            await ExpireLeaseAsync(id);
        }
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", cancelled));

        Assert.Equal(3, await Flow.SweepExpiredLeasesAsync(100, Ct));

        Assert.Equal(new[] { $"{exhausted}:3", $"{cancelled}:4" }.Order(),
            (await Db.ScalarAsync<string>("SELECT string_agg(node_id || ':' || kind, ',') FROM {schema}.completions")).Split(',').Order());
        Assert.True(await InQueueAsync(reset));
        Assert.Equal(1, Signal.Count);
    }

    [Fact]
    public async Task Cancel_of_unleased_job_signals_cancelled()
    {
        var id = await SubscribedAsync();

        Assert.Equal(CancelResult.Cancelled, await Flow.CancelAsync(id, "stop", Ct));

        Assert.Equal($"{id}:4:-:false:0", await SignalRowsAsync());
        Assert.Equal(1, Signal.Count);
    }

    [Fact]
    public async Task Release_signals_only_when_cancelled()
    {
        var id = await SubscribedAsync();
        var first = await LeaseAsync(id);
        Assert.Equal(ReleaseOutcome.Released, await Flow.ReleaseAsync(id, first, ServerA, ran: true, null, Ct));
        Assert.Null(await SignalRowsAsync());

        var second = await LeaseAsync(id);
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id));
        Assert.Equal(ReleaseOutcome.Cancelled, await Flow.ReleaseAsync(id, second, ServerA, ran: true, "shutdown", Ct));

        Assert.Equal($"{id}:4:-:false:0", await SignalRowsAsync());
    }

    [Fact]
    public async Task Dequeue_returns_continuation_code()
    {
        var parent = Guid.CreateVersion7();
        var id = await EnqueueAsync();
        await Db.ExecAsync("UPDATE {schema}.jobs SET continuation = 3, parent_id = @p WHERE id = @id", ("id", id), ("p", parent));

        var job = await LeaseOneAsync();

        Assert.Equal(((short?)3, (Guid?)parent), (job.Continuation, job.ParentId));
    }

    /// <summary>Awaiting-строка в обход API (инвариант §4): ready = false, run_at = infinity, await_condition = code.</summary>
    private async Task<Guid> AwaitingAsync(Guid parent, short code = 1, Guid? batch = null)
    {
        var id = Guid.CreateVersion7();
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition, batch_id)
            VALUES (@id, 'q', 0, 'infinity', false, 3, @p, @c, @b);
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at, parent_id, batch_id, continuation)
            VALUES (@id, 'test.job', 'q', 0, '{}', 0, 3, now(), @p, @b, @c);
            """, ("id", id), ("p", parent), ("c", code), ("b", batch));
        return id;
    }

    [Fact]
    public async Task Awaiting_row_is_not_dequeued_promoted_or_triggered_and_reads_as_awaiting()   // §12 SQL-контракт
    {
        var parent = Guid.CreateVersion7();
        var id = await AwaitingAsync(parent);

        Assert.Empty(await Flow.DequeueAsync("q", 10, ServerA, Lease, Ct));
        Assert.Equal(0, (await Flow.PromoteDueAsync(100, Ct)).Promoted);
        Assert.Null(await Flow.TriggerAsync(id, Ct));
        Assert.Equal(0L, await Db.ScalarAsync<long>(
            "SELECT count(*) FROM {schema}.queue WHERE lease_owner IS NULL AND ready AND await_condition IS NOT NULL"));   // queue_fetch

        var info = await Flow.GetJobAsync(id, Ct);
        Assert.NotNull(info);
        Assert.Equal(JobState.Awaiting, info.State);
        Assert.Null(info.RunAt);
        Assert.Equal(new JobId(parent), info.ParentId);
        Assert.Equal(ContinuationCondition.OnSuccess, info.Continuation);
    }

    [Fact]
    public async Task Cancel_of_awaiting_job_cancels_it()   // §6.7
    {
        var id = await AwaitingAsync(Guid.CreateVersion7());

        Assert.Equal(CancelResult.Cancelled, await Flow.CancelAsync(id, "no", Ct));

        Assert.Equal((short)4, await JobStateAsync(id));
        Assert.False(await InQueueAsync(id));
    }

    [Theory]
    [InlineData((short)1, true, false, ContinuationCondition.OnSuccess)]
    [InlineData((short)2, true, false, ContinuationCondition.OnAnyFinished)]
    [InlineData((short)3, false, true, ContinuationCondition.OnSuccess)]
    [InlineData((short)4, false, true, ContinuationCondition.OnAnyFinished)]
    [InlineData((short)5, false, false, null)]
    public async Task JobInfo_maps_continuation_codes(short code, bool jobParent, bool batchParent, ContinuationCondition? condition)   // §5.3
    {
        var parent = Guid.CreateVersion7();
        var batch = Guid.CreateVersion7();
        var id = await AwaitingAsync(code == 5 ? batch : parent, code, batch);

        var info = (await Flow.GetJobAsync(id, Ct))!;

        Assert.Equal(jobParent ? new JobId(parent) : null, info.ParentId);
        Assert.Equal(batchParent ? new BatchId(parent) : null, info.ParentBatchId);
        Assert.Equal(new BatchId(batch), info.BatchId);
        Assert.Equal(condition, info.Continuation);
    }

    [Fact]
    public async Task Scheduled_job_is_still_scheduled()
    {
        var id = await EnqueueAsync(delay: TimeSpan.FromHours(1));

        Assert.Equal(JobState.Scheduled, (await Flow.GetJobAsync(id, Ct))!.State);
    }
}

public sealed class WorkflowHotPathTests_Direct(PostgresFixture pg) : WorkflowHotPathTests(pg, ConnectionMode.Direct);
public sealed class WorkflowHotPathTests_Bouncer(PostgresFixture pg) : WorkflowHotPathTests(pg, ConnectionMode.Bouncer);
