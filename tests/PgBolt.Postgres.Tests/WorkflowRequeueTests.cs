using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §6.6: requeue родителей и членов, синхронный учёт, requeue при идущей отмене, устаревший CancelMembers.</summary>
public abstract class WorkflowRequeueTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    private Task<bool> BatchIsAsync(Guid id, string condition) =>
        Db.ScalarAsync<bool>($"SELECT {condition} FROM {{schema}}.batches WHERE id = @id", ("id", id));

    private Task<long> CancelledEventsAsync(Guid id) =>
        Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.job_events WHERE job_id = @id AND kind = 4", ("id", id));

    [Fact]
    public async Task Requeue_writes_notify_and_batch_id_into_queue()
    {
        IJobStep? member = null;
        var batch = (await Client.Workflow().StartBatch(b => member = b.Enqueue(A(1))).SubmitAsync(Ct)).Batches[0];
        var lone = (await Client.Workflow().Enqueue(A(2)).SubmitAsync(Ct)).Jobs[0].Value;
        await FailFinallyAsync(member!.Id.Value);
        await FailFinallyAsync(lone);
        await Db.ExecAsync("UPDATE {schema}.jobs SET notify = true WHERE id = @id", ("id", lone));   // подписка §6.2 п. 2 после финализации

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(member.Id, Ct));
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(new JobId(lone), Ct));

        Assert.True(await Db.ScalarAsync<bool>("SELECT notify AND batch_id = @b FROM {schema}.queue WHERE job_id = @id",
            ("id", member.Id.Value), ("b", batch.Value)));
        Assert.True(await Db.ScalarAsync<bool>("SELECT notify AND batch_id IS NULL FROM {schema}.queue WHERE job_id = @id", ("id", lone)));
    }

    [Fact]
    public async Task Requeued_failed_parent_runs_its_waiting_OnSuccess_child_after_success()   // §12 SQL-контракт
    {
        var r = await Client.Workflow().Enqueue(A(1)).ContinueWith(A(2)).SubmitAsync(Ct);
        var (parent, child) = (r.Jobs[0].Value, r.Jobs[1].Value);
        await FailFinallyAsync(parent);
        await ResolveAllAsync();
        Assert.Equal("awaiting", await FateAsync(child));

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(r.Jobs[0], Ct));
        await SucceedAsync(parent);
        await ResolveAllAsync();

        Assert.Equal("launched", await FateAsync(child));
    }

    [Fact]
    public async Task Requeue_of_failed_member_returns_batch_and_outer_to_active()   // §12 Batches
    {
        IJobStep? x = null;
        IBatchStep? inner = null;
        var outer = Client.Workflow().StartBatch(o => inner = o.StartBatch(b => x = b.Enqueue(A(1))));
        var next = outer.ContinueWith(A(2));
        await outer.SubmitAsync(Ct);
        await FailFinallyAsync(x!.Id.Value);
        await ResolveAllAsync();
        Assert.Equal((short?)3, await BatchStateAsync(outer.Id.Value));

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(x.Id, Ct));

        foreach (var b in new[] { inner!.Id.Value, outer.Id.Value })
            Assert.True(await BatchIsAsync(b, "state = 0 AND failed = 0 AND finished_at IS NULL AND NOT cancel_requested"));
        Assert.Equal("awaiting", await FateAsync(next.Id.Value));
        await SucceedAsync(x.Id.Value);
        await ResolveAllAsync();

        Assert.Equal((short?)2, await BatchStateAsync(inner.Id.Value));
        Assert.Equal((short?)2, await BatchStateAsync(outer.Id.Value));
        Assert.Equal("launched", await FateAsync(next.Id.Value));
    }

    [Fact]
    public async Task Requeue_of_member_of_cancelled_batch_allows_cancelling_again()   // §12 Batches
    {
        IReadOnlyList<IJobStep> members = [];
        var batch = (await Client.Workflow().StartBatch(b => members = b.EnqueueMany(new[] { A(1), A(2) })).SubmitAsync(Ct)).Batches[0];
        await Client.CancelBatchAsync(batch, Ct);
        await ResolveAllAsync();
        Assert.Equal((short?)4, await BatchStateAsync(batch.Value));

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(members[0].Id, Ct));
        Assert.True(await BatchIsAsync(batch.Value, "state = 0 AND cancelled = 1 AND NOT cancel_requested"));

        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch, Ct));
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions WHERE kind = 6"));
        await ResolveAllAsync();

        Assert.Equal("cancelled", await FateAsync(members[0].Id.Value));
        Assert.Equal((short?)4, await BatchStateAsync(batch.Value));
    }

    [Fact]
    public async Task Requeue_during_cancel_behind_and_ahead_of_cursor()   // §12 Batches, Review Focus 4
    {
        IJobStep? x = null, y = null;
        IReadOnlyList<IJobStep> others = [];
        var batch = Client.Workflow().StartBatch(b =>
        {
            x = b.Enqueue(A(0));
            NextMillisecond();
            others = b.EnqueueMany(Enumerable.Range(1, 2998).Select(A).ToArray());
            NextMillisecond();
            y = x.ContinueWith(A(9999), ContinuationCondition.OnAnyFinished);   // ждущее продолжение X, последнее по id
        });
        await batch.SubmitAsync(Ct);
        var ahead = others.Select(s => s.Id.Value).Order().ElementAt(1500);   // за первой порцией (X + 999)
        await Client.CancelBatchAsync(batch.Id, Ct);
        Assert.Equal(1000, (await Flow.CancelMembersAsync(1000, 1000, 1, TimeSpan.FromSeconds(30), Ct)).Rows);
        Assert.Equal("cancelled", await FateAsync(x!.Id.Value));
        await CancelJobAsync(ahead);

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(x.Id, Ct));
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(new JobId(ahead), Ct));

        Assert.True(await BatchIsAsync(batch.Id.Value, "state = 0 AND cancel_requested"));   // флаг не сброшен
        await SucceedAsync(x.Id.Value);                                                          // позади курсора — выполняется
        await ResolveWithoutCancelMembersAsync();
        Assert.Equal("cancelled", await FateAsync(y!.Id.Value));                                // продолжение ветки отменено, не запущено
        Assert.Equal($"batch {batch.Id} cancelled", await EventMessageAsync(y.Id.Value));

        await ResolveAllAsync();

        Assert.Equal("succeeded", await FateAsync(x.Id.Value));
        Assert.Equal("cancelled", await FateAsync(ahead));                                      // впереди курсора — отменён снова
        Assert.Equal(2L, await CancelledEventsAsync(ahead));
        Assert.True(await BatchIsAsync(batch.Id.Value, "state = 4 AND succeeded = 1 AND cancelled = 2999 AND failed = 0"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Nested_batch_requeued_during_outer_cancel(bool beforeStageOne)   // §12 Batches, §6.6
    {
        IJobStep? x = null, nm = null;
        IBatchStep? nested = null;
        var outer = Client.Workflow().StartBatch(b =>
        {
            x = b.Enqueue(A(0));
            nested = b.StartBatch(n => nm = n.Enqueue(A(1)));
        });
        await outer.SubmitAsync(Ct);
        var lease = await LeaseAsync(x!.Id.Value);                       // X выполняется: внешний не станет терминальным
        await CancelJobAsync(nm!.Id.Value);
        await ResolveWithoutCancelMembersAsync();
        Assert.Equal((short?)4, await BatchStateAsync(nested!.Id.Value));
        await Client.CancelBatchAsync(outer.Id, Ct);
        if (beforeStageOne)
            Assert.Equal(1, (await Flow.CancelMembersAsync(1, 1, 1, TimeSpan.FromSeconds(30), Ct)).Rows);   // этап 0: только X
        else
            await Flow.CancelMembersAsync(1000, 5000, 100, TimeSpan.FromSeconds(30), Ct);                   // этап 1 прошёл терминальный вложенный

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(nm.Id, Ct));
        Assert.True(await BatchIsAsync(nested.Id.Value, "state = 0 AND NOT cancel_requested AND cancelled = 0"));
        Assert.True(await BatchIsAsync(outer.Id.Value, "state = 0 AND cancel_requested"));

        await Flow.CancelMembersAsync(1000, 5000, 100, TimeSpan.FromSeconds(30), Ct);
        if (beforeStageOne)
        {
            Assert.Equal("cancelled", await FateAsync(nm.Id.Value));              // этап 1 внешнего снова пометил вложенный
            Assert.Equal(2L, await CancelledEventsAsync(nm.Id.Value));
        }
        else
        {
            Assert.True(await BatchIsAsync(nested.Id.Value, "state = 0 AND NOT cancel_requested"));   // остаётся Active
            await SucceedAsync(nm.Id.Value);                                                          // и выполняется
            await ResolveWithoutCancelMembersAsync();
            Assert.Equal((short?)2, await BatchStateAsync(nested.Id.Value));
        }
        Assert.Equal(FailOutcome.Cancelled, await Flow.ConfirmCancelAsync(x.Id.Value, lease, ServerA, "stop", Ct));
        await ResolveAllAsync();
        Assert.Equal((short?)4, await BatchStateAsync(outer.Id.Value));
    }

    /// <summary>Ждёт, пока порция отмены завершится или встанет в ожидание блокировки второй сессией (после ждущего requeue).</summary>
    private async Task CompletedOrWaitingAsync(Task cancel)
    {
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        while (!cancel.IsCompleted && await Db.ScalarAsync<long>(
                   "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE @p", ("p", $"%{Db.Schema}.%")) < 2)
        {
            if (System.Diagnostics.Stopwatch.GetElapsedTime(started) > TimeSpan.FromSeconds(10))
                throw new TimeoutException("CancelMembers neither finished nor waited for a lock.");
            await Task.Delay(20, Ct);
        }
    }

    /// <summary>
    /// Правило снимка этапа 1 (§6.5, §6.6): вложенный, вернувшийся в Active requeue'ом после снимка порции, пройден обходом.
    /// outerTerminal: requeue поднимается до внешнего и сбрасывает его флаг; иначе внешний Active с cancel_requested — подъём
    /// на нём останавливается и флаг остаётся (§6.6 п. 2), от повторной пометки вложенный защищает только снимок.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Stage_one_does_not_flag_nested_batch_requeued_after_its_snapshot(bool outerTerminal)   // §6.5, §6.6
    {
        IJobStep? x = null, nm = null;
        IBatchStep? nested = null;
        var outer = Client.Workflow().StartBatch(b =>
        {
            x = b.Enqueue(A(0));
            nested = b.StartBatch(n => nm = n.Enqueue(A(1)));
        });
        await outer.SubmitAsync(Ct);
        var lease = await LeaseAsync(x!.Id.Value);                                     // X выполняется: внешний не терминален
        await Client.CancelBatchAsync(outer.Id, Ct);
        Assert.Equal(1, (await Flow.CancelMembersAsync(1, 1, 1, TimeSpan.FromSeconds(30), Ct)).Rows);   // этап 0: только X
        if (outerTerminal)
            Assert.Equal(FailOutcome.Cancelled, await Flow.ConfirmCancelAsync(x.Id.Value, lease, ServerA, "stop", Ct));
        await CancelJobAsync(nm!.Id.Value);
        await ResolveWithoutCancelMembersAsync();
        Assert.Equal((short?)4, await BatchStateAsync(nested!.Id.Value));
        Assert.True(await BatchIsAsync(outer.Id.Value,                                  // сигнал внешнего — перед этапом 1
            outerTerminal ? "state = 4" : "state IN (0, 1) AND cancel_requested"));

        await using (var holder = await HoldRowsAsync("SELECT 1 FROM {schema}.batches WHERE id = @id FOR NO KEY UPDATE",
                         ("id", nested.Id.Value)))
        {
            var requeue = Client.RequeueAsync(nm.Id, Ct);
            await WaitForLockWaitersAsync(1);                                          // requeue ждёт вложенный
            var cancel = Flow.CancelMembersAsync(1000, 5000, 1, TimeSpan.FromSeconds(30), Ct);
            await CompletedOrWaitingAsync(cancel);                                     // этап 1: снимок взят до коммита requeue
            await holder.CloseAsync();
            Assert.Equal(RequeueResult.Requeued, await requeue);
            Assert.Equal(0, (await cancel).TimedOut);
        }

        Assert.True(await BatchIsAsync(nested.Id.Value, "state = 0 AND NOT cancel_requested"),
            "nested batch re-flagged by stale CancelMembers stage 1");
        Assert.True(await BatchIsAsync(outer.Id.Value, outerTerminal ? "state = 0 AND NOT cancel_requested" : "state = 0 AND cancel_requested"));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions WHERE kind = 6 AND node_id = @id",
            ("id", nested.Id.Value)));
        Assert.Equal("launched", await FateAsync(nm.Id.Value));
        await SucceedAsync(nm.Id.Value);
        await ResolveAllAsync();
        Assert.Equal((short?)2, await BatchStateAsync(nested.Id.Value));
        if (!outerTerminal)
        {
            Assert.Equal(FailOutcome.Cancelled, await Flow.ConfirmCancelAsync(x.Id.Value, lease, ServerA, "stop", Ct));
            await ResolveAllAsync();
            Assert.Equal((short?)4, await BatchStateAsync(outer.Id.Value));
        }
    }

    [Fact]
    public async Task Stale_cancel_members_after_batch_finished_is_dropped()   // §12 «Устаревший CancelMembers»
    {
        IReadOnlyList<IJobStep> first = [], last = [];
        IJobStep? nm = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            first = b.EnqueueMany(Enumerable.Range(0, 1000).Select(A).ToArray());
            NextMillisecond();
            b.StartBatch(n => nm = n.Enqueue(A(1000)));
            NextMillisecond();
            last = b.EnqueueMany(Enumerable.Range(1001, 1999).Select(A).ToArray());
        });
        await batch.SubmitAsync(Ct);
        await SucceedManyAsync([.. last.Select(s => s.Id.Value)]);
        await Client.CancelBatchAsync(batch.Id, Ct);
        Assert.Equal(1000, (await Flow.CancelMembersAsync(1000, 1000, 1, TimeSpan.FromSeconds(30), Ct)).Rows);   // курсор — после first
        await CancelJobAsync(nm!.Id.Value);
        await ResolveWithoutCancelMembersAsync();
        Assert.Equal((short?)4, await BatchStateAsync(batch.Id.Value));                    // Cancelled до конца обхода

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(first[0].Id, Ct));   // за курсором
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(nm.Id, Ct));         // член вложенного
        Assert.True(await BatchIsAsync(batch.Id.Value, "state = 0 AND NOT cancel_requested"));

        var next = await Flow.CancelMembersAsync(1000, 5000, 100, TimeSpan.FromSeconds(30), Ct);

        Assert.Equal((1, 0), (next.Portions, next.Rows));                                    // порция удалила сигнал, ничего не изменив
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions WHERE kind = 6"));
        Assert.Equal("launched", await FateAsync(first[0].Id.Value));
        Assert.Equal("launched", await FateAsync(nm.Id.Value));
    }

    [Fact]
    public async Task Batch_counters_read_as_non_negative_between_requeue_and_count()   // §12 Batches
    {
        IJobStep? x = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            x = b.Enqueue(A(1));
            b.Enqueue(A(2));
        });
        await batch.SubmitAsync(Ct);
        await FailFinallyAsync(x!.Id.Value);                                       // сигнал Failed ещё не учтён
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(x.Id, Ct));

        Assert.Equal(-1, await Db.ScalarAsync<int>("SELECT failed FROM {schema}.batches WHERE id = @id", ("id", batch.Id.Value)));
        var info = (await Client.GetBatchAsync(batch.Id, Ct))!;
        Assert.Equal((0, 0, 0, BatchState.Active), (info.Succeeded, info.Failed, info.Cancelled, info.State));

        await ResolveAllAsync();
        Assert.Equal(0, (await Client.GetBatchAsync(batch.Id, Ct))!.Failed);
    }

    [Fact]
    public async Task Requeue_of_member_of_deleted_batch_succeeds()   // Review Focus 5, §6.6
    {
        IJobStep? x = null;
        var batch = (await Client.Workflow().StartBatch(b => x = b.Enqueue(A(1))).SubmitAsync(Ct)).Batches[0];
        await FailFinallyAsync(x!.Id.Value);
        await ResolveAllAsync();
        await Db.ExecAsync("DELETE FROM {schema}.batches WHERE id = @id", ("id", batch.Value));   // удалён retention'ом (§7.1)

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(x.Id, Ct));
        await SucceedAsync(x.Id.Value);
        var counted = await Flow.CountCompletionsAsync(500, Ct);

        Assert.Equal([batch.Value], counted.MissingBatches);
        Assert.Equal(0L, await SignalsAsync());
    }

    [Fact]
    public async Task Requeued_cancelled_continuation_becomes_pending()   // §6.6
    {
        var r = await Client.Workflow().Enqueue(A(1)).ContinueWith(A(2)).SubmitAsync(Ct);
        await CancelJobAsync(r.Jobs[0].Value);
        await ResolveAllAsync();
        Assert.Equal("cancelled", await FateAsync(r.Jobs[1].Value));

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(r.Jobs[1], Ct));

        Assert.Equal("launched", await FateAsync(r.Jobs[1].Value));
    }
}

public sealed class WorkflowRequeueTests_Direct(PostgresFixture pg) : WorkflowRequeueTests(pg, ConnectionMode.Direct);
public sealed class WorkflowRequeueTests_Bouncer(PostgresFixture pg) : WorkflowRequeueTests(pg, ConnectionMode.Bouncer);
