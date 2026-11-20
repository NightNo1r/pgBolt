using Npgsql;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §2.2, §2.3, §6.4: таблица исходов, каскады, исход batch, активация, протокол Т1/Т2.</summary>
public abstract class WorkflowResolverTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    [Theory]
    [InlineData(WorkflowOutcome.Succeeded, ContinuationCondition.OnSuccess, "launched")]
    [InlineData(WorkflowOutcome.Succeeded, ContinuationCondition.OnAnyFinished, "launched")]
    [InlineData(WorkflowOutcome.Failed, ContinuationCondition.OnSuccess, "awaiting")]
    [InlineData(WorkflowOutcome.Failed, ContinuationCondition.OnAnyFinished, "launched")]
    [InlineData(WorkflowOutcome.Cancelled, ContinuationCondition.OnSuccess, "cancelled")]
    [InlineData(WorkflowOutcome.Cancelled, ContinuationCondition.OnAnyFinished, "launched")]
    public async Task Parent_outcome_table(WorkflowOutcome outcome, ContinuationCondition condition, string expected)   // §2.2
    {
        var r = await Client.Workflow().Enqueue(A(1)).ContinueWith(A(2), condition).SubmitAsync(Ct);
        var (parent, child) = (r.Jobs[0].Value, r.Jobs[1].Value);
        await FinishAsync(parent, outcome);

        await ResolveAllAsync();

        Assert.Equal(expected, await FateAsync(child));
        if (expected == "cancelled")
            Assert.Equal($"parent {parent} cancelled", await EventMessageAsync(child));
        Assert.Equal(0L, await SignalsAsync());
    }

    [Fact]
    public async Task Cancel_cascades_through_chain_of_three()   // §12 SQL-контракт
    {
        var root = Client.Workflow().Enqueue(A(0));
        var c1 = root.ContinueWith(A(1));
        var c2 = c1.ContinueWith(A(2));
        var c3 = c2.ContinueWith(A(3));
        await c3.SubmitAsync(Ct);
        await CancelJobAsync(root.Id.Value);

        await ResolveAllAsync();

        foreach (var c in new[] { c1, c2, c3 })
            Assert.Equal("cancelled", await FateAsync(c.Id.Value));
        Assert.Equal($"parent {c2.Id} cancelled", await EventMessageAsync(c3.Id.Value));
    }

    [Fact]
    public async Task Cancelling_awaiting_job_cascades_to_its_continuations()   // §12: CancelAsync → Cancelled и каскад
    {
        var root = Client.Workflow().Enqueue(A(0));
        var c1 = root.ContinueWith(A(1));
        var any = c1.ContinueWith(A(2), ContinuationCondition.OnAnyFinished);
        var success = c1.ContinueWith(A(3));
        await root.SubmitAsync(Ct);

        Assert.Equal(CancelResult.Cancelled, await Client.CancelAsync(c1.Id, ct: Ct));
        await ResolveAllAsync();

        Assert.Equal("launched", await FateAsync(any.Id.Value));
        Assert.Equal("cancelled", await FateAsync(success.Id.Value));
        Assert.Equal("launched", await FateAsync(root.Id.Value));   // родитель не затронут
    }

    [Theory]
    [InlineData(new[] { WorkflowOutcome.Succeeded, WorkflowOutcome.Succeeded }, (short)2)]
    [InlineData(new[] { WorkflowOutcome.Succeeded, WorkflowOutcome.Failed }, (short)3)]
    [InlineData(new[] { WorkflowOutcome.Failed, WorkflowOutcome.Cancelled }, (short)4)]
    [InlineData(new[] { WorkflowOutcome.Succeeded, WorkflowOutcome.Cancelled }, (short)4)]
    public async Task Batch_outcome_prefers_cancelled_then_failed(WorkflowOutcome[] members, short state)   // §2.3
    {
        IReadOnlyList<IJobStep> items = [];
        var batch = Client.Workflow().StartBatch(b => items = b.EnqueueMany(new[] { A(1), A(2) }));
        var any = batch.ContinueWith(A(3), ContinuationCondition.OnAnyFinished);
        var success = batch.ContinueWith(A(4));
        await batch.SubmitAsync(Ct);
        for (var i = 0; i < members.Length; i++)
            await FinishAsync(items[i].Id.Value, members[i]);
        Assert.Equal((short?)0, await BatchStateAsync(batch.Id.Value));   // до учёта resolver'ом

        await ResolveAllAsync();

        Assert.Equal((short?)state, await BatchStateAsync(batch.Id.Value));
        Assert.True(await Db.ScalarAsync<bool>(
            "SELECT finished_at IS NOT NULL AND succeeded + failed + cancelled = total FROM {schema}.batches WHERE id = @id",
            ("id", batch.Id.Value)));
        Assert.Equal("launched", await FateAsync(any.Id.Value));
        Assert.Equal(state switch { 2 => "launched", 3 => "awaiting", _ => "cancelled" }, await FateAsync(success.Id.Value));
    }

    [Fact]
    public async Task Member_waiting_for_failed_parent_keeps_batch_active()   // §2.3
    {
        IJobStep? x = null, y = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            x = b.Enqueue(A(1));
            y = x.ContinueWith(A(2));
        });
        await batch.SubmitAsync(Ct);
        await FailFinallyAsync(x!.Id.Value);

        await ResolveAllAsync();

        Assert.Equal((short?)0, await BatchStateAsync(batch.Id.Value));
        Assert.Equal("awaiting", await FateAsync(y!.Id.Value));
    }

    [Fact]
    public async Task Outcome_rises_through_three_levels_of_nesting()   // §12 Batches
    {
        IJobStep? x = null;
        IBatchStep? mid = null, inner = null;
        var outer = Client.Workflow().StartBatch(o => mid = o.StartBatch(m => inner = m.StartBatch(i => x = i.Enqueue(A(1)))));
        var next = outer.ContinueWith(A(2));
        await outer.SubmitAsync(Ct);
        await FailFinallyAsync(x!.Id.Value);

        await ResolveAllAsync();

        foreach (var b in new[] { inner!, mid!, outer })
            Assert.True(await Db.ScalarAsync<bool>("SELECT state = 3 AND failed = 1 FROM {schema}.batches WHERE id = @id", ("id", b.Id.Value)));
        Assert.Equal("awaiting", await FateAsync(next.Id.Value));
    }

    [Fact]
    public async Task Batch_continuation_activates_members_and_nested_batches()   // §12 Batches
    {
        var p = Client.Workflow().Enqueue(A(0));
        IJobStep? m = null, n1 = null;
        IBatchStep? nested = null;
        var bc = p.ContinueWithBatch(b =>
        {
            m = b.Enqueue(A(1));
            nested = b.StartBatch(n => n1 = n.Enqueue(A(2)));
        });
        await p.SubmitAsync(Ct);

        await SucceedAsync(p.Id.Value);
        await ResolveAllAsync();

        Assert.Equal((short?)0, await BatchStateAsync(bc.Id.Value));
        Assert.Equal((short?)0, await BatchStateAsync(nested!.Id.Value));
        Assert.Equal("launched", await FateAsync(m!.Id.Value));
        Assert.Equal("launched", await FateAsync(n1!.Id.Value));
    }

    [Fact]
    public async Task Nested_batch_continuation_of_member_waits_for_the_member()   // §12 Batches
    {
        var p = Client.Workflow().Enqueue(A(0));
        IJobStep? x = null, y = null;
        IBatchStep? chained = null;
        var bc = p.ContinueWithBatch(b =>
        {
            x = b.Enqueue(A(1));
            chained = x.ContinueWithBatch(c => y = c.Enqueue(A(2)));
        });
        await p.SubmitAsync(Ct);

        await SucceedAsync(p.Id.Value);
        await ResolveAllAsync();
        Assert.Equal("launched", await FateAsync(x!.Id.Value));
        Assert.Equal((short?)1, await BatchStateAsync(chained!.Id.Value));   // не активирован вместе с внешним
        Assert.Equal("awaiting", await FateAsync(y!.Id.Value));

        await SucceedAsync(x.Id.Value);
        await ResolveAllAsync();
        Assert.Equal("launched", await FateAsync(y.Id.Value));

        await SucceedAsync(y.Id.Value);
        await ResolveAllAsync();
        Assert.Equal((short?)2, await BatchStateAsync(chained.Id.Value));
        Assert.Equal((short?)2, await BatchStateAsync(bc.Id.Value));
    }

    [Fact]
    public async Task Branch_cancel_by_outer_batch_names_nearest_cancelled_batch()   // §6.4 Т2.3: отмена ветки по внешнему batch
    {
        IJobStep? x = null, y = null;
        var outer = Client.Workflow().StartBatch(o => o.StartBatch(n =>
        {
            x = n.Enqueue(A(0));
            y = x.ContinueWith(A(1), ContinuationCondition.OnAnyFinished);
        }));
        await outer.SubmitAsync(Ct);
        // Отмена внешнего запрошена, а CancelMembers (Task 7) до вложенного ещё не дошёл: флаг есть только у внешнего.
        await Db.ExecAsync("UPDATE {schema}.batches SET cancel_requested = true WHERE id = @id", ("id", outer.Id.Value));

        await SucceedAsync(x!.Id.Value);
        await ResolveAllAsync();

        Assert.Equal("cancelled", await FateAsync(y!.Id.Value));
        Assert.Equal($"batch {outer.Id} cancelled", await EventMessageAsync(y.Id.Value));   // не id собственного (вложенного) batch
    }

    [Fact]
    public async Task Subscribed_signal_applies_terminal_batch_outcome_or_is_dropped()   // §6.2, §6.4 Т2.2
    {
        IJobStep? m = null;
        var batch = (await Client.Workflow().StartBatch(b => m = b.Enqueue(A(1))).SubmitAsync(Ct)).Batches[0];
        var early = (await Client.Workflow().After(batch).ContinueWith(A(2)).SubmitAsync(Ct)).Jobs[0];
        await ResolveAllAsync();                                       // batch Active: Subscribed удалён, ребёнок ждёт
        Assert.Equal("awaiting", await FateAsync(early.Value));
        Assert.Equal(0L, await SignalsAsync());

        await SucceedAsync(m!.Id.Value);
        await ResolveAllAsync();
        var late = (await Client.Workflow().After(batch).ContinueWith(A(3)).SubmitAsync(Ct)).Jobs[0];
        await ResolveAllAsync();

        Assert.Equal("launched", await FateAsync(early.Value));
        Assert.Equal("launched", await FateAsync(late.Value));
    }

    [Fact]
    public async Task Count_deletes_500_leaf_member_signals_in_one_statement()   // §12 Batches
    {
        var batch = Guid.CreateVersion7();
        await Db.ExecAsync("""
            INSERT INTO {schema}.batches (id, state, total, created_at) VALUES (@b, 0, 1000, now());
            INSERT INTO {schema}.completions (node_id, kind, batch_id) SELECT gen_random_uuid(), 2, @b FROM generate_series(1, 500);
            """, ("b", batch));

        var counted = await Flow.CountCompletionsAsync(500, Ct);

        Assert.Equal((500, 0, 0), (counted.Captured, counted.Finished, counted.Walking));
        Assert.Equal(0L, await SignalsAsync());
        Assert.Equal(500, await Db.ScalarAsync<int>("SELECT succeeded FROM {schema}.batches WHERE id = @b", ("b", batch)));
    }

    [Fact]
    public async Task Counted_fan_out_signals_are_not_recaptured_by_count()   // §12 Batches
    {
        await Db.ExecAsync("""
            WITH p AS (SELECT gen_random_uuid() AS id FROM generate_series(1, 500)),
            c AS (INSERT INTO {schema}.completions (node_id, kind) SELECT id, 2 FROM p RETURNING node_id)
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition)
            SELECT gen_random_uuid(), 'client', 0, 'infinity', false, 3, node_id, 1 FROM c;
            INSERT INTO {schema}.completions (node_id, kind) SELECT gen_random_uuid(), 2 FROM generate_series(1, 10);
            """);

        var first = await Flow.CountCompletionsAsync(500, Ct);
        var second = await Flow.CountCompletionsAsync(500, Ct);

        Assert.Equal((500, 500), (first.Captured, first.Walking));
        Assert.Equal((10, 0), (second.Captured, second.Walking));
        Assert.Equal(500L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions WHERE counted"));
    }

    [Fact]
    public async Task Count_reports_signals_ready_for_walk_and_cancel_members()   // §6.4: пропуск этапов без работы
    {
        var empty = await Flow.CountCompletionsAsync(500, Ct);
        Assert.Equal((false, false), (empty.WalkReady, empty.CancelReady));

        // Отложенные (retry_after в будущем) захваты Т2 и CancelMembers не взяли бы.
        await Db.ExecAsync("""
            INSERT INTO {schema}.completions (node_id, kind, counted, retry_after) VALUES (gen_random_uuid(), 2, true, now() + interval '1 hour');
            INSERT INTO {schema}.completions (node_id, kind, retry_after) VALUES (gen_random_uuid(), 6, now() + interval '1 hour');
            """);
        var deferred = await Flow.CountCompletionsAsync(500, Ct);
        Assert.Equal((false, false), (deferred.WalkReady, deferred.CancelReady));

        // Сигнал с ребёнком: этот Т1 передаёт его Т2 (walking), но в своём снимке ещё не видит учтённым.
        var parent = Guid.CreateVersion7();
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition)
            VALUES (gen_random_uuid(), 'client', 0, 'infinity', false, 3, @p, 1);
            INSERT INTO {schema}.completions (node_id, kind) VALUES (@p, 2);
            """, ("p", parent));
        var passed = await Flow.CountCompletionsAsync(500, Ct);
        Assert.Equal((1, false, false), (passed.Walking, passed.WalkReady, passed.CancelReady));

        await Db.ExecAsync("INSERT INTO {schema}.completions (node_id, kind) VALUES (gen_random_uuid(), 6)");
        var ready = await Flow.CountCompletionsAsync(500, Ct);
        Assert.Equal((0, true, true), (ready.Walking, ready.WalkReady, ready.CancelReady));
    }

    /// <summary>Терминальные (Succeeded) родители в jobs, у каждого — ждущий OnSuccess-ребёнок; сигналы учтены Т1.</summary>
    private async Task<Guid[]> CountedParentsAsync(Guid[] parents, Guid[] children)
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at, notify)
            SELECT id, 'client.test', 'client', 0, '{}', 2, 3, now(), true FROM unnest(@p) id;
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition)
            SELECT c, 'client', 0, 'infinity', false, 3, p, 1 FROM unnest(@c, @p) AS t(c, p);
            INSERT INTO {schema}.completions (node_id, kind) SELECT id, 2 FROM unnest(@p) WITH ORDINALITY AS t(id, n) ORDER BY n;
            """, ("p", parents), ("c", children));
        var walking = 0;
        for (var i = 0; i < 10 && walking < parents.Length; i++)
            walking += (await Flow.CountCompletionsAsync(500, Ct)).Walking;
        Assert.Equal(parents.Length, walking);
        return children;
    }

    private static Guid[] NewIds(int n) => [.. Enumerable.Range(0, n).Select(_ => Guid.CreateVersion7())];

    [Theory]
    [InlineData(5)]
    [InlineData(500)]
    public async Task Walk_statement_count_does_not_depend_on_node_count(int nodes)   // §12 Batches: счётчик команд
    {
        await CountedParentsAsync(NewIds(nodes), NewIds(nodes));

        var walked = await Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);

        Assert.Equal(6, walked.Statements);   // захват, этап 0, остаток этапа 0, этап 1, остаток этапа 1, завершение
        Assert.Equal(nodes, walked.Rows);
        Assert.Equal((long)nodes, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.queue WHERE ready AND await_condition IS NULL"));
        Assert.Equal(0L, await SignalsAsync());
    }

    [Fact]
    public async Task Signals_with_locked_children_are_deferred_and_do_not_block_next()   // §12 Batches, Review Focus 3
    {
        var children = await CountedParentsAsync(NewIds(501), NewIds(501));
        await using var holder = await Db.Admin.OpenConnectionAsync(Ct);
        await using (var hold = new NpgsqlCommand(Db.Sql("BEGIN; SELECT 1 FROM {schema}.queue WHERE job_id = ANY(@ids) FOR UPDATE"), holder))
        {
            hold.Parameters.AddWithValue("ids", children[..500]);
            await hold.ExecuteNonQueryAsync(Ct);
        }

        var blocked = await Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);
        var next = await Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);

        Assert.Equal((500, 500, 0), (blocked.Captured, blocked.Deferred, blocked.Rows));
        Assert.Equal((1, 0, 1), (next.Captured, next.Deferred, next.Rows));
        Assert.True(await ReadyAsync(children[500]));
        Assert.Equal(500L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions WHERE retry_after > now()"));

        await holder.CloseAsync();                                                   // откат держателя
        await Db.ExecAsync("UPDATE {schema}.completions SET retry_after = NULL");    // прошёл ResolverInterval
        Assert.Equal(500, (await Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct)).Rows);
        Assert.Equal(0L, await SignalsAsync());
    }

    [Theory]
    [InlineData(2, false)]   // исход: учёт Т1
    [InlineData(2, true)]    // учтённый: обход Т2
    [InlineData(6, false)]   // CancelMembers
    public async Task Ready_completions_check_sees_signals_that_a_capture_would_take(short kind, bool counted)   // §6.4 простой
    {
        Assert.False(await Flow.HasReadyCompletionsAsync(Ct));
        await Db.ExecAsync("INSERT INTO {schema}.completions (node_id, kind, counted, retry_after) VALUES (gen_random_uuid(), @k, @c, now() + interval '1 minute')",
            ("k", kind), ("c", counted));
        Assert.False(await Flow.HasReadyCompletionsAsync(Ct));                       // отложенный (retry_after) — работы нет

        await Db.ExecAsync("UPDATE {schema}.completions SET retry_after = now() - interval '1 second'");

        Assert.True(await Flow.HasReadyCompletionsAsync(Ct));
        await Db.ExecAsync("UPDATE {schema}.completions SET retry_after = NULL");
        Assert.True(await Flow.HasReadyCompletionsAsync(Ct));
    }

    [Fact]
    public async Task Oldest_completion_age_reads_head_of_queue()
    {
        Assert.Null(await Flow.OldestCompletionAgeAsync(Ct));
        await Db.ExecAsync("INSERT INTO {schema}.completions (node_id, kind, created_at) VALUES (gen_random_uuid(), 2, now() - interval '1 minute')");

        Assert.InRange((await Flow.OldestCompletionAgeAsync(Ct))!.Value, TimeSpan.FromSeconds(59), TimeSpan.FromMinutes(5));
    }
}

public sealed class WorkflowResolverTests_Direct(PostgresFixture pg) : WorkflowResolverTests(pg, ConnectionMode.Direct);
public sealed class WorkflowResolverTests_Bouncer(PostgresFixture pg) : WorkflowResolverTests(pg, ConnectionMode.Bouncer);
