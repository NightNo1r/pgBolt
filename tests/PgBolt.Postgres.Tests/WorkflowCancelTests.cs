using Npgsql;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §6.5, §2.3: CancelBatchAsync, CancelMembers, отмена ветки, GetBatchAsync.</summary>
public abstract class WorkflowCancelTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    [Fact]
    public async Task Cancel_batch_reports_not_found_requested_and_already_finished()   // §12 CancelBatchAsync
    {
        Assert.Equal(BatchCancelResult.NotFound, await Client.CancelBatchAsync(BatchId.New(), Ct));
        var batch = (await Client.Workflow().StartBatch(b => b.Enqueue(A(1))).SubmitAsync(Ct)).Batches[0];

        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch, Ct));
        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch, Ct));   // повтор — без нового сигнала
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions WHERE kind = 6"));
        Assert.Equal(1, Signal.Count);

        await ResolveAllAsync();

        Assert.Equal((short?)4, await BatchStateAsync(batch.Value));
        Assert.Equal(BatchCancelResult.AlreadyFinished, await Client.CancelBatchAsync(batch, Ct));
    }

    [Fact]
    public async Task Cancel_cancels_waiting_members_now_running_through_heartbeat_and_nested_recursively()   // §12 CancelBatchAsync
    {
        IJobStep? pending = null, running = null, chained = null, inner = null;
        IBatchStep? nested = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            pending = b.Enqueue(A(1));
            running = b.Enqueue(A(2));
            chained = running.ContinueWith(A(3));
            nested = b.StartBatch(n => inner = n.Enqueue(A(4)));
        });
        var after = batch.ContinueWith(A(5), ContinuationCondition.OnAnyFinished);
        await batch.SubmitAsync(Ct);
        var lease = await LeaseAsync(running!.Id.Value);

        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch.Id, Ct));
        await ResolveAllAsync();

        Assert.Equal("cancelled", await FateAsync(pending!.Id.Value));
        Assert.Equal($"batch {batch.Id} cancelled", await EventMessageAsync(pending.Id.Value));
        Assert.Equal("cancelled", await FateAsync(chained!.Id.Value));
        Assert.Equal("cancelled", await FateAsync(inner!.Id.Value));
        Assert.Equal($"batch {nested!.Id} cancelled", await EventMessageAsync(inner.Id.Value));
        Assert.True(Assert.Single((await Flow.HeartbeatAsync(ServerA, [new LeaseRef(running.Id.Value, lease)], Lease, Ct)).Leases).CancelRequested);
        Assert.Equal((short?)0, await BatchStateAsync(batch.Id.Value));   // выполняющийся член ещё не завершён

        Assert.Equal(FailOutcome.Cancelled, await Flow.ConfirmCancelAsync(running.Id.Value, lease, ServerA, "stopped", Ct));
        await ResolveAllAsync();

        Assert.Equal((short?)4, await BatchStateAsync(batch.Id.Value));
        Assert.Equal((short?)4, await BatchStateAsync(nested.Id.Value));
        Assert.Equal("launched", await FateAsync(after.Id.Value));
    }

    [Fact]
    public async Task Member_finishing_successfully_after_cancel_request_counts_as_succeeded()   // §2.3
    {
        IJobStep? running = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            running = b.Enqueue(A(1));
            b.Enqueue(A(2));
        });
        await batch.SubmitAsync(Ct);
        var lease = await LeaseAsync(running!.Id.Value);
        await Client.CancelBatchAsync(batch.Id, Ct);
        await ResolveAllAsync();

        Assert.Single(await Flow.AckAsync(ServerA, [new LeaseRef(running.Id.Value, lease)], Ct));
        await ResolveAllAsync();

        var info = (await Client.GetBatchAsync(batch.Id, Ct))!;
        Assert.Equal((BatchState.Cancelled, 1, 0, 1), (info.State, info.Succeeded, info.Failed, info.Cancelled));
        Assert.True(info.CancellationRequested);
        Assert.NotNull(info.FinishedAt);
    }

    [Fact]
    public async Task Get_batch_reports_links_and_conditions()   // §12 API: BatchInfo
    {
        var job = Client.Workflow().Enqueue(A(0));
        IBatchStep? nested = null;
        var ofJob = job.ContinueWithBatch(b => nested = b.StartBatch(n => n.Enqueue(A(1))), ContinuationCondition.OnAnyFinished);
        var ofBatch = ofJob.ContinueWithBatch(b => b.Enqueue(A(2)));
        await job.SubmitAsync(Ct);

        var a = (await Client.GetBatchAsync(ofJob.Id, Ct))!;
        var b2 = (await Client.GetBatchAsync(ofBatch.Id, Ct))!;
        var n = (await Client.GetBatchAsync(nested!.Id, Ct))!;

        Assert.Equal((BatchState.Awaiting, 1, (JobId?)job.Id, (BatchId?)null, (ContinuationCondition?)ContinuationCondition.OnAnyFinished),
            (a.State, a.Total, a.ParentId, a.ParentBatchId, a.Condition));
        Assert.Null(a.OuterBatchId);
        Assert.Equal(((JobId?)null, (BatchId?)ofJob.Id, (ContinuationCondition?)ContinuationCondition.OnSuccess),
            (b2.ParentId, b2.ParentBatchId, b2.Condition));
        Assert.Equal(((BatchId?)ofJob.Id, (JobId?)null, (ContinuationCondition?)null, BatchState.Awaiting),
            (n.OuterBatchId, n.ParentId, n.Condition, n.State));
        Assert.False(a.CancellationRequested);
        Assert.Null(a.FinishedAt);
        Assert.Null(await Client.GetBatchAsync(BatchId.New(), Ct));
    }

    [Fact]
    public async Task Cancelled_parent_cancels_batch_continuation_entirely()   // §12 Batches
    {
        var p = Client.Workflow().Enqueue(A(0));
        IJobStep? m = null, x = null, y = null, n1 = null;
        var bc = p.ContinueWithBatch(b =>
        {
            m = b.Enqueue(A(1));
            b.StartBatch(n => n1 = n.Enqueue(A(2)));
            x = b.Enqueue(A(3));
            y = x.ContinueWith(A(4), ContinuationCondition.OnAnyFinished);
        });
        await p.SubmitAsync(Ct);
        await CancelJobAsync(p.Id.Value);

        await ResolveAllAsync();

        Assert.Equal((short?)4, await BatchStateAsync(bc.Id.Value));
        foreach (var s in new[] { m!, n1!, x!, y! })
            Assert.Equal("cancelled", await FateAsync(s.Id.Value));
    }

    [Fact]
    public async Task Batch_continuation_with_running_cancel_is_not_activated()   // §12 Batches
    {
        var p = Client.Workflow().Enqueue(A(0));
        IJobStep? m = null;
        var bc = p.ContinueWithBatch(b => m = b.Enqueue(A(1)));
        await p.SubmitAsync(Ct);
        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(bc.Id, Ct));   // Awaiting — не терминален

        await SucceedAsync(p.Id.Value);
        await ResolveOnceAsync();                                         // batch с cancel_requested Т2 не трогает
        Assert.Equal((short?)1, await BatchStateAsync(bc.Id.Value));
        await ResolveAllAsync();

        Assert.Equal((short?)4, await BatchStateAsync(bc.Id.Value));
        Assert.Equal("cancelled", await FateAsync(m!.Id.Value));
    }

    [Theory]
    [InlineData(ContinuationCondition.OnAnyFinished, false)]
    [InlineData(ContinuationCondition.OnSuccess, false)]
    [InlineData(ContinuationCondition.OnAnyFinished, true)]
    [InlineData(ContinuationCondition.OnSuccess, true)]
    public async Task Branch_cancel_beats_continuation_of_running_member(ContinuationCondition condition, bool samePortion)   // §12 «Отмена ветки»
    {
        IJobStep? x = null, y = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            x = b.Enqueue(A(0));
            NextMillisecond();
            b.EnqueueMany(Enumerable.Range(1, samePortion ? 10 : 1200).Select(A).ToArray());
            NextMillisecond();
            y = x.ContinueWith(A(9999), condition);                       // id больше всех членов: при 1200 — за первой порцией
        });
        await batch.SubmitAsync(Ct);
        var lease = await LeaseAsync(x!.Id.Value);
        await Client.CancelBatchAsync(batch.Id, Ct);

        Assert.Equal(1, (await Flow.CancelMembersAsync(1000, 1000, 1, TimeSpan.FromSeconds(30), Ct)).Portions);   // одна порция: X выполняется
        Assert.Equal(samePortion ? "cancelled" : "awaiting", await FateAsync(y!.Id.Value));
        Assert.Single(await Flow.AckAsync(ServerA, [new LeaseRef(x.Id.Value, lease)], Ct));                    // X успел завершиться
        await ResolveWithoutCancelMembersAsync();                         // Y решает Т2 по сигналу X, а не следующая порция CancelMembers

        Assert.Equal("cancelled", await FateAsync(y.Id.Value));
        Assert.Equal($"batch {batch.Id} cancelled", await EventMessageAsync(y.Id.Value));
    }

    [Fact]
    public async Task Nested_batch_continuation_of_member_in_cancelled_batch_is_not_activated()   // §12 «Отмена ветки»
    {
        IJobStep? x = null, z = null;
        IBatchStep? chained = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            x = b.Enqueue(A(0));
            chained = x.ContinueWithBatch(c => z = c.Enqueue(A(1)));
        });
        await batch.SubmitAsync(Ct);
        var lease = await LeaseAsync(x!.Id.Value);
        await Client.CancelBatchAsync(batch.Id, Ct);
        Assert.Equal(1, (await Flow.CancelMembersAsync(1, 1, 1, TimeSpan.FromSeconds(30), Ct)).Rows);   // порция из одной строки: X

        Assert.Single(await Flow.AckAsync(ServerA, [new LeaseRef(x.Id.Value, lease)], Ct));
        await ResolveOnceAsync();

        Assert.True(await Db.ScalarAsync<bool>("SELECT state = 1 AND cancel_requested FROM {schema}.batches WHERE id = @id",
            ("id", chained!.Id.Value)));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions WHERE kind = 5"));
        Assert.Equal("awaiting", await FateAsync(z!.Id.Value));

        await ResolveAllAsync();

        Assert.Equal("cancelled", await FateAsync(z.Id.Value));
        Assert.Equal((short?)4, await BatchStateAsync(batch.Id.Value));
    }

    [Fact]
    public async Task Activated_batch_with_cancel_requested_does_not_launch_members()   // §12 «Отмена ветки»
    {
        var p = Client.Workflow().Enqueue(A(0));
        IJobStep? m = null;
        var bc = p.ContinueWithBatch(b => m = b.Enqueue(A(1)));
        await p.SubmitAsync(Ct);
        await SucceedAsync(p.Id.Value);
        await ResolveOnceAsync();                                                 // активация: state 0 и сигнал Activated
        Assert.Equal((short?)0, await BatchStateAsync(bc.Id.Value));
        Assert.Equal("awaiting", await FateAsync(m!.Id.Value));

        await Client.CancelBatchAsync(bc.Id, Ct);
        await ResolveOnceAsync();                                                 // Т2 по Activated: отмена вместо запуска

        Assert.Equal("cancelled", await FateAsync(m.Id.Value));
        Assert.Equal($"batch {bc.Id} cancelled", await EventMessageAsync(m.Id.Value));
    }

    [Fact]
    public async Task Member_row_held_longer_than_lock_timeout_defers_only_its_batch()   // §12 CancelBatchAsync, §6.5
    {
        IJobStep? held = null, other = null, elsewhere = null;
        var first = (await Client.Workflow().StartBatch(b =>
        {
            held = b.Enqueue(A(1));
            other = b.Enqueue(A(2));
        }).SubmitAsync(Ct)).Batches[0];
        var second = (await Client.Workflow().StartBatch(b => elsewhere = b.Enqueue(A(3))).SubmitAsync(Ct)).Batches[0];
        await using var holder = await Db.Admin.OpenConnectionAsync(Ct);
        await using (var hold = new NpgsqlCommand(Db.Sql("BEGIN; SELECT 1 FROM {schema}.queue WHERE job_id = @id FOR UPDATE"), holder))
        {
            hold.Parameters.AddWithValue("id", held!.Id.Value);
            await hold.ExecuteNonQueryAsync(Ct);
        }
        await Client.CancelBatchAsync(first, Ct);
        await Client.CancelBatchAsync(second, Ct);

        var result = await Flow.CancelMembersAsync(1000, 5000, 100, TimeSpan.FromSeconds(30), Ct);

        Assert.Equal(1, result.TimedOut);
        Assert.Equal("cancelled", await FateAsync(elsewhere!.Id.Value));      // отмена другого batch идёт
        Assert.Equal("launched", await FateAsync(other!.Id.Value));           // порция первого откатана целиком
        Assert.True(await Db.ScalarAsync<bool>("SELECT retry_after > now() FROM {schema}.completions WHERE node_id = @b AND kind = 6",
            ("b", first.Value)));

        await holder.CloseAsync();                                             // строка освобождена
        await Db.ExecAsync("UPDATE {schema}.completions SET retry_after = NULL");
        await Flow.CancelMembersAsync(1000, 5000, 100, TimeSpan.FromSeconds(30), Ct);

        Assert.Equal("cancelled", await FateAsync(held.Id.Value));
        Assert.Equal("cancelled", await FateAsync(other.Id.Value));
    }
}

public sealed class WorkflowCancelTests_Direct(PostgresFixture pg) : WorkflowCancelTests(pg, ConnectionMode.Direct);
public sealed class WorkflowCancelTests_Bouncer(PostgresFixture pg) : WorkflowCancelTests(pg, ConnectionMode.Bouncer);
