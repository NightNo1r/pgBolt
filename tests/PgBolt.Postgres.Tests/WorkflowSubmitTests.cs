using System.Data;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §6.1–§6.2 через клиента: форма графа в queue/jobs/batches, After в каждом состоянии, транзакции пользователя.</summary>
public abstract class WorkflowSubmitTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    public enum ParentKind { Pending, Awaiting, Succeeded, Failed, Cancelled }

    private sealed record QueueState(bool Ready, short? Await, Guid? Parent, Guid? Batch, bool Notify, bool Infinity);

    private async Task<QueueState?> QueueOf(Guid id)
    {
        await using var cmd = Db.Admin.CreateCommand(Db.Sql(
            "SELECT ready, await_condition, parent_id, batch_id, notify, run_at = 'infinity' FROM {schema}.queue WHERE job_id = @id"));
        cmd.Parameters.AddWithValue("id", id);
        await using var r = await cmd.ExecuteReaderAsync(Ct);
        if (!await r.ReadAsync(Ct))
            return null;
        return new QueueState(r.GetBoolean(0), r.IsDBNull(1) ? null : r.GetInt16(1), r.IsDBNull(2) ? null : r.GetGuid(2),
            r.IsDBNull(3) ? null : r.GetGuid(3), r.GetBoolean(4), r.GetBoolean(5));
    }

    /// <summary>state:total:condition:parent_id:parent_batch_id.</summary>
    private Task<string> BatchOf(Guid id) =>
        Db.ScalarAsync<string>("""
            SELECT state || ':' || total || ':' || coalesce(condition::text, '-') || ':' || coalesce(parent_id::text, '-')
                   || ':' || coalesce(parent_batch_id::text, '-')
            FROM {schema}.batches WHERE id = @id
            """, ("id", id));

    private Task<string?> CompletionsAsync() =>
        Db.ScalarAsync<string?>("SELECT string_agg(node_id || ':' || kind, ',' ORDER BY id) FROM {schema}.completions");

    [Fact]
    public async Task Chain_with_fan_out_writes_awaiting_graph()   // §5.1, пример 1
    {
        var fetch = Client.Workflow().Enqueue(A(1));
        var parse = fetch.ContinueWith(A(2));
        IReadOnlyList<IJobStep> items = [];
        var fan = parse.ContinueWithBatch(b => items = b.EnqueueMany(new[] { A(3), A(4) }));
        var report = fan.ContinueWith(A(5), ContinuationCondition.OnAnyFinished);

        var result = await report.SubmitAsync(Ct);

        Assert.Equal([fetch.Id, parse.Id, items[0].Id, items[1].Id, report.Id], result.Jobs);
        Assert.Equal([fan.Id], result.Batches);
        Assert.Equal(new QueueState(true, null, null, null, true, false), await QueueOf(fetch.Id.Value));
        Assert.Equal(new QueueState(false, 1, fetch.Id.Value, null, true, true), await QueueOf(parse.Id.Value));
        Assert.Equal($"1:2:1:{parse.Id}:-", await BatchOf(fan.Id.Value));
        foreach (var item in items)
            Assert.Equal(new QueueState(false, 5, fan.Id.Value, fan.Id.Value, true, true), await QueueOf(item.Id.Value));
        Assert.Equal(new QueueState(false, 4, fan.Id.Value, null, false, true), await QueueOf(report.Id.Value));
        Assert.Equal(["client"], Wakeup.Signals);
        Assert.Equal(0, Signal.Count);
    }

    [Fact]
    public async Task Root_batch_with_inner_chain_and_nested_batch_is_active()   // §5.1, пример 3
    {
        IJobStep? fetch = null, merge = null, resize = null;
        IBatchStep? inner = null;
        var outer = Client.Workflow().StartBatch(b =>
        {
            fetch = b.Enqueue(A(1));
            merge = fetch.ContinueWith(A(2));
            inner = b.StartBatch(n => resize = n.Enqueue(A(3)));
        });
        var report = outer.ContinueWith(A(4));

        await report.SubmitAsync(Ct);

        Assert.Equal("0:3:-:-:-", await BatchOf(outer.Id.Value));
        Assert.Equal($"0:1:-:-:{outer.Id}", await BatchOf(inner!.Id.Value));
        Assert.Equal(new QueueState(true, null, null, outer.Id.Value, true, false), await QueueOf(fetch!.Id.Value));
        Assert.Equal(new QueueState(false, 1, fetch.Id.Value, outer.Id.Value, true, true), await QueueOf(merge!.Id.Value));
        Assert.Equal(new QueueState(true, null, null, inner.Id.Value, true, false), await QueueOf(resize!.Id.Value));
        Assert.Equal(new QueueState(false, 3, outer.Id.Value, null, false, true), await QueueOf(report.Id.Value));
    }

    [Fact]
    public async Task Batches_inside_batch_continuation_await_activation_or_their_parent()   // §2.1
    {
        var x = Client.Workflow().Enqueue(A(1));
        IBatchStep? nested = null, chained = null;
        IJobStep? y = null, z = null, w = null;
        var outer = x.ContinueWithBatch(b =>
        {
            nested = b.StartBatch(n => y = n.Enqueue(A(2)));
            z = b.Enqueue(A(3));
            chained = z.ContinueWithBatch(c => w = c.Enqueue(A(4)));
        });

        await outer.SubmitAsync(Ct);

        Assert.Equal($"1:3:1:{x.Id}:-", await BatchOf(outer.Id.Value));
        Assert.Equal($"1:1:-:-:{outer.Id}", await BatchOf(nested!.Id.Value));
        Assert.Equal($"1:1:1:{z!.Id}:{outer.Id}", await BatchOf(chained!.Id.Value));
        Assert.Equal(new QueueState(false, 5, nested.Id.Value, nested.Id.Value, true, true), await QueueOf(y!.Id.Value));
        Assert.Equal(new QueueState(false, 5, outer.Id.Value, outer.Id.Value, true, true), await QueueOf(z.Id.Value));
        Assert.Equal(new QueueState(false, 5, chained.Id.Value, chained.Id.Value, true, true), await QueueOf(w!.Id.Value));
    }

    private async Task<Guid> ParentAsync(ParentKind kind)
    {
        if (kind == ParentKind.Awaiting)
            return (await Client.Workflow().Enqueue(A(0)).ContinueWith(A(0)).SubmitAsync(Ct)).Jobs[1].Value;
        var id = (await Client.Workflow().Enqueue(A(0)).SubmitAsync(Ct)).Jobs[0].Value;
        switch (kind)
        {
            case ParentKind.Succeeded:
                await SucceedAsync(id);
                break;
            case ParentKind.Failed:
                await FailFinallyAsync(id);
                break;
            case ParentKind.Cancelled:
                await CancelJobAsync(id);
                break;
        }
        return id;
    }

    [Theory]
    [InlineData(ParentKind.Pending, null)]
    [InlineData(ParentKind.Awaiting, null)]
    [InlineData(ParentKind.Succeeded, (short)2)]
    [InlineData(ParentKind.Failed, (short)3)]
    [InlineData(ParentKind.Cancelled, (short)4)]
    public async Task After_job_in_each_state(ParentKind kind, short? signal)   // §12 SQL-контракт
    {
        var parent = await ParentAsync(kind);
        var before = Signal.Count;

        var child = (await Client.Workflow().After(new JobId(parent)).ContinueWith(A(1)).SubmitAsync(Ct)).Jobs[0].Value;

        Assert.Equal((short?)1, await AwaitAsync(child));
        Assert.Equal(signal is { } k ? $"{parent}:{k}" : null, await CompletionsAsync());
        if (signal is null)
            Assert.True(await Db.ScalarAsync<bool>("SELECT notify FROM {schema}.queue WHERE job_id = @id", ("id", parent)));
        Assert.Equal(signal is null ? before : before + 1, Signal.Count);
    }

    [Theory]
    [InlineData((short)0)]
    [InlineData((short)1)]
    [InlineData((short)2)]
    [InlineData((short)3)]
    [InlineData((short)4)]
    public async Task After_batch_in_each_state_writes_subscribed_signal(short state)   // §12 SQL-контракт
    {
        var batch = (await Client.Workflow().StartBatch(b => b.Enqueue(A(0))).SubmitAsync(Ct)).Batches[0].Value;
        await Db.ExecAsync("UPDATE {schema}.batches SET state = @s WHERE id = @id", ("id", batch), ("s", state));

        var child = (await Client.Workflow().After(new BatchId(batch)).ContinueWith(A(1)).SubmitAsync(Ct)).Jobs[0].Value;

        Assert.Equal((short?)3, await AwaitAsync(child));
        Assert.Equal($"{batch}:7", await CompletionsAsync());
    }

    [Fact]
    public async Task After_missing_or_other_kind_throws_ParentNotFound_and_writes_nothing()   // §12 SQL-контракт
    {
        var job = (await Client.Workflow().Enqueue(A(0)).SubmitAsync(Ct)).Jobs[0];
        var batch = (await Client.Workflow().StartBatch(b => b.Enqueue(A(0))).SubmitAsync(Ct)).Batches[0];
        var before = await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs");

        await Assert.ThrowsAsync<ParentNotFoundException>(async () =>
            await Client.Workflow().After(JobId.New()).ContinueWith(A(1)).SubmitAsync(Ct));
        await Assert.ThrowsAsync<ParentNotFoundException>(async () =>
            await Client.Workflow().After(new JobId(batch.Value)).ContinueWith(A(1)).SubmitAsync(Ct));
        await Assert.ThrowsAsync<ParentNotFoundException>(async () =>
            await Client.Workflow().After(new BatchId(job.Value)).ContinueWith(A(1)).SubmitAsync(Ct));
        await Assert.ThrowsAsync<ParentNotFoundException>(async () =>
            await Client.Workflow().After(BatchId.New()).ContinueWithBatch(b => b.Enqueue(A(1))).SubmitAsync(Ct));

        Assert.Equal(before, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.batches"));
        Assert.Null(await CompletionsAsync());
    }

    [Fact]
    public async Task Awaiting_job_holds_its_unique_key()   // §12 unique_key
    {
        var r = await Client.Workflow().Enqueue(A(1)).ContinueWith(A(2), options: new StepOptions { UniqueKey = "u" }).SubmitAsync(Ct);

        var again = await Client.EnqueueAsync(A(3), new EnqueueOptions { UniqueKey = "u" }, Ct);

        Assert.Equal(new EnqueueResult(r.Jobs[1], false), again);
    }

    [Fact]
    public async Task Unique_conflict_throws_and_writes_no_graph_rows()   // §12 unique_key
    {
        var existing = await Client.EnqueueAsync(A(1), new EnqueueOptions { UniqueKey = "u" }, Ct);
        var wf = Client.Workflow();
        wf.StartBatch(b => b.EnqueueMany(new[] { A(2), A(3) })).ContinueWith(A(4), options: new StepOptions { UniqueKey = "u" });

        var e = await Assert.ThrowsAsync<UniqueKeyConflictException>(async () => await wf.SubmitAsync(Ct));

        Assert.Equal("u", e.UniqueKey);
        Assert.Equal(existing.Id, e.ExistingId);
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.batches"));
    }

    [Fact]
    public async Task Single_root_job_matches_enqueue_but_conflict_throws()   // §5.2
    {
        var r = await Client.Workflow().Enqueue(A(1), new StepOptions { UniqueKey = "k", Priority = 7 }).SubmitAsync(Ct);

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT q.ready AND q.priority = 7 AND q.queue = 'client' AND q.unique_key = 'k' AND NOT q.notify
                   AND j.continuation IS NULL AND j.max_attempts = 4 AND j.retry_delays = ARRAY[1,5] AND j.args = '{"Order":1}'::jsonb
            FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id WHERE q.job_id = @id
            """, ("id", r.Jobs[0].Value)));
        await Assert.ThrowsAsync<UniqueKeyConflictException>(async () =>
            await Client.Workflow().Enqueue(A(2), new StepOptions { UniqueKey = "k" }).SubmitAsync(Ct));
    }

    [Fact]
    public async Task User_transaction_keeps_working_after_workflow_exceptions()   // §12, Review Focus 1
    {
        await Client.EnqueueAsync(A(0), new EnqueueOptions { UniqueKey = "taken" }, Ct);
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        var mine = await Client.EnqueueAsync(A(1), new EnqueueOptions { Transaction = tx }, Ct);
        var options = new WorkflowOptions { Transaction = tx };

        await Assert.ThrowsAsync<UniqueKeyConflictException>(async () =>
            await Client.Workflow(options).StartBatch(b => b.Enqueue(A(2)))
                .ContinueWith(A(3), options: new StepOptions { UniqueKey = "taken" }).SubmitAsync(Ct));
        var wf = Client.Workflow(options);
        wf.Enqueue(A(4));                               // вставлена до подписки, откатывается вместе с графом
        wf.After(JobId.New()).ContinueWith(A(5));
        await Assert.ThrowsAsync<ParentNotFoundException>(async () => await wf.SubmitAsync(Ct));
        await tx.CommitAsync(Ct);

        Assert.True(await InQueueAsync(mine.Id!.Value.Value));
        Assert.Equal(2L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.batches"));
    }

    [Fact]
    public async Task Workflow_in_user_transaction_is_written_on_commit_without_wakeups()
    {
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);

        var r = await Client.Workflow(new WorkflowOptions { Transaction = tx }).Enqueue(A(1)).ContinueWith(A(2)).SubmitAsync(Ct);
        Assert.False(await InQueueAsync(r.Jobs[0].Value));
        await tx.CommitAsync(Ct);

        Assert.True(await InQueueAsync(r.Jobs[0].Value));
        Assert.Empty(Wakeup.Signals);
    }

    [Fact]
    public async Task Repeatable_read_transaction_is_rejected()   // §5.2: требования core §5.1
    {
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(IsolationLevel.RepeatableRead, Ct);

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await Client.Workflow(new WorkflowOptions { Transaction = tx }).Enqueue(A(1)).SubmitAsync(Ct));
    }

    [Fact]
    public async Task Awaiting_job_is_not_dequeued_promoted_or_triggered_and_cancels()   // §12 SQL-контракт
    {
        var r = await Client.Workflow().Enqueue(A(1)).ContinueWith(A(2)).SubmitAsync(Ct);
        var child = r.Jobs[1];

        Assert.Equal(r.Jobs[0].Value, Assert.Single(await Flow.DequeueAsync("client", 10, ServerA, Lease, Ct)).Id);
        Assert.Equal(0, (await Flow.PromoteDueAsync(100, Ct)).Promoted);
        Assert.False(await Client.TriggerAsync(child, Ct));
        Assert.Equal(JobState.Awaiting, (await Client.GetAsync(child, Ct))!.State);
        Assert.Equal(CancelResult.Cancelled, await Client.CancelAsync(child, ct: Ct));
    }

    [Fact]
    public async Task Root_job_with_delay_is_scheduled_without_wakeup()
    {
        var r = await Client.Workflow().Enqueue(A(1), new StepOptions { Delay = TimeSpan.FromMinutes(5) }).SubmitAsync(Ct);

        Assert.Equal(JobState.Scheduled, (await Client.GetAsync(r.Jobs[0], Ct))!.State);
        Assert.Empty(Wakeup.Signals);
    }
}

public sealed class WorkflowSubmitTests_Direct(PostgresFixture pg) : WorkflowSubmitTests(pg, ConnectionMode.Direct);
public sealed class WorkflowSubmitTests_Bouncer(PostgresFixture pg) : WorkflowSubmitTests(pg, ConnectionMode.Bouncer);
