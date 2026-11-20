using System.Text.Json;
using Npgsql;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §7.1: живые строки (а)/(б), повторная проверка под блокировкой, удаление строк batches; гонки подписки с удалением.</summary>
public abstract class RetentionWorkflowTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    private static readonly TimeSpan Days3 = TimeSpan.FromDays(3);
    private IJobClient _client = null!;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        ClientMetadata.EnsureRegistered();
        _client = ClientTelemetry.Client(Store, new RecordingWakeup());
    }

    /// <summary>Терминальная задача суток day (notify — у неё были подписчики или она член batch).</summary>
    private async Task<Guid> TerminalAsync(DateOnly day, short state, Guid? batch = null)
    {
        var id = IdOn(day);
        await Db.ExecAsync("""
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, attempt, max_attempts, created_at, finished_at, batch_id, notify)
            VALUES (@id, 'test.job', 'q', 0, '{}', @s, 1, 3, now(), now(), @b, true)
            """, ("id", id), ("s", state), ("b", batch));
        return id;
    }

    /// <summary>Ждущий ребёнок (сегодняшний) родителя parent с кодом continuation code.</summary>
    private async Task<Guid> AwaitingChildAsync(Guid parent, short code)
    {
        var id = Guid.CreateVersion7();
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition)
            VALUES (@id, 'q', 0, 'infinity', false, 3, @p, @c);
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at, parent_id, continuation)
            VALUES (@id, 'test.job', 'q', 0, '{}', 0, 3, now(), @p, @c);
            """, ("id", id), ("p", parent), ("c", code));
        return id;
    }

    /// <summary>Строка batches; finishedDaysAgo — finished_at = now() − N суток (терминальный), null — не завершён.</summary>
    private async Task<Guid> BatchAsync(short state, int? finishedDaysAgo = null, Guid? outer = null, Guid? parent = null,
        short? condition = null, Guid? id = null)
    {
        var batch = id ?? Guid.CreateVersion7();
        await Db.ExecAsync("""
            INSERT INTO {schema}.batches (id, parent_batch_id, parent_id, condition, state, total, created_at, finished_at)
            VALUES (@id, @o, @p, @c, @s, 1, now(), now() - make_interval(days => @d::int))
            """, ("id", batch), ("o", outer), ("p", parent), ("c", condition), ("s", state), ("d", finishedDaysAgo));
        return batch;
    }

    private Task<bool> JobExistsAsync(Guid id) =>
        Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM {schema}.jobs WHERE id = @id)", ("id", id));

    private Task<bool> BatchExistsAsync(Guid id) =>
        Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM {schema}.batches WHERE id = @id)", ("id", id));

    private Task ResolveAllAsync() => ResolverPasses.SettleAsync(Store, cancelMembers: true, Ct);

    [Fact]
    public async Task Partition_with_workflow_live_rows_is_compacted_and_keeps_them()   // §12 Retention
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var failedParent = await TerminalAsync(day, 3);                       // (а): ждущий OnSuccess-ребёнок
        await AwaitingChildAsync(failedParent, 1);
        var batchParent = await TerminalAsync(day, 3);                        // (а): единственный ребёнок — batch-продолжение
        await BatchAsync(state: 1, parent: batchParent, condition: 1);
        var failedBatch = await BatchAsync(state: 3, finishedDaysAgo: 0);
        var failedMember = await TerminalAsync(day, 3, failedBatch);          // (б)
        var cancelledMember = await TerminalAsync(day, 4, failedBatch);       // (б)
        var cancelledBatch = await BatchAsync(state: 4, finishedDaysAgo: 0);
        var memberOfCancelled = await TerminalAsync(day, 4, cancelledBatch);  // (б): Cancelled-член Cancelled-batch
        await InsertTerminalAsync(day, 150);

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        var pinned = Assert.Single(r.Pinned);
        Assert.Equal((day, 5, true), (pinned.Day, pinned.LiveJobs, pinned.Compacted));
        Assert.Equal(5L, await CountAsync(J(day)));
        foreach (var id in new[] { failedParent, batchParent, failedMember, cancelledMember, memberOfCancelled })
            Assert.True(await JobExistsAsync(id));
    }

    [Fact]
    public async Task Batch_ids_with_waiting_children_are_not_live_jobs()   // §12 Retention: id batch не попадают в LiveJobs
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        await InsertTerminalAsync(day, 3);
        var batch = await BatchAsync(state: 3, finishedDaysAgo: 0, id: IdOn(day));   // id batch в диапазоне суток
        await AwaitingChildAsync(batch, 3);
        await BatchAsync(state: 1, parent: batch, condition: 3);

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Contains(day, r.DroppedJobsPartitions);
    }

    [Fact]
    public async Task Subscription_to_old_terminal_job_between_precheck_and_lock_keeps_parent()   // §12 Retention (хук)
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var parent = await TerminalAsync(day, 2);
        await InsertTerminalAsync(day, 3);
        var hooks = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                if (c.Step == RetentionStep.LocksChecked && c.Parent == "jobs")
                    await _client.Workflow().After(new JobId(parent)).ContinueWith(new ClientArgs(1)).SubmitAsync(ct);
            },
        };

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.DoesNotContain(day, r.DroppedJobsPartitions);
        Assert.True(await AttachedAsync(J(day)));
        Assert.True(await JobExistsAsync(parent));
    }

    [Fact]
    public async Task Requeue_and_final_fail_of_cancelled_member_between_precheck_and_lock_keeps_member()   // §12 Retention (хук)
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var batch = await BatchAsync(state: 4, finishedDaysAgo: 0);
        var member = await TerminalAsync(day, 4, batch);
        await InsertTerminalAsync(day, 150);
        var hooks = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                if (c.Step != RetentionStep.WarmedUp)
                    return;
                Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(member, ct)).Result);
                var leased = Assert.Single(await Store.DequeueAsync("q", 1, ServerA, Lease, ct));
                Assert.Equal(FailOutcome.Failed, await Store.FailAsync(member, leased.LeaseId, ServerA, JobEventKind.Failed, "x", null, ct));
            },
        };

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.True(Assert.Single(r.Pinned).Compacted);
        Assert.True(await JobExistsAsync(member));                            // объединение с набором предпроверки
        Assert.Equal((short?)3, await JobStateAsync(member));
    }

    [Fact]
    public async Task Cancel_of_pending_member_between_precheck_and_lock_keeps_member()   // §12 Retention (хук), §7.1
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var batch = await BatchAsync(state: 0);                                // существующий незавершённый batch
        var member = IdOn(day);                                                // Pending-член: в предпроверке — строка queue, не (б)
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, batch_id, notify)
            VALUES (@id, 'q', 0, now(), true, 3, @b, true);
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at, batch_id, notify)
            VALUES (@id, 'test.job', 'q', 0, '{}', 0, 3, now(), @b, true);
            """, ("id", member), ("b", batch));
        await InsertTerminalAsync(day, 150);
        var hooks = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                if (c.Step == RetentionStep.WarmedUp)                         // член стал живым по (б); queue и (а) под блокировкой пусты
                    Assert.Equal(CancelResult.Cancelled, await Store.CancelAsync(member, null, ct));
            },
        };

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.DoesNotContain(day, r.DroppedJobsPartitions);                  // не отсоединена: объединение с набором предпроверки
        var pinned = Assert.Single(r.Pinned);
        Assert.Equal((day, 1, true), (pinned.Day, pinned.LiveJobs, pinned.Compacted));
        Assert.Equal(1L, await CountAsync(J(day)));
        Assert.Equal((short?)4, await JobStateAsync(member));
    }

    [Fact]
    public async Task Requeue_between_precheck_batches_and_cancel_before_lock_keeps_member()   // §7.1: queue и (б) — один снимок
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var batch = await BatchAsync(state: 3, finishedDaysAgo: 0);
        await Db.ExecAsync("UPDATE {schema}.batches SET failed = 1 WHERE id = @id", ("id", batch));
        var member = await TerminalAsync(day, 3, batch);                     // Failed-член: в начале предпроверки не в queue
        await InsertTerminalAsync(day, 150);
        var hooks = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                // requeue после первой сверки oid: член уходит из (б) в queue; отмена после предпроверки — обратно в (б),
                // которой под блокировкой нет. Член должен попасть в набор предпроверки хотя бы одним из источников.
                if (c.Step == RetentionStep.PrecheckOidChecked)
                    Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(member, ct)).Result);
                else if (c.Step == RetentionStep.Prechecked)
                    Assert.Equal(CancelResult.Cancelled, await Store.CancelAsync(member, null, ct));
            },
        };

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.Equal(1, hooks.Count(RetentionStep.PrecheckOidChecked, "jobs"));
        Assert.DoesNotContain(day, r.DroppedJobsPartitions);
        Assert.True(Assert.Single(r.Pinned).Compacted);
        Assert.True(await JobExistsAsync(member));
        Assert.Equal((short?)4, await JobStateAsync(member));
    }

    [Fact]
    public async Task Batch_rows_are_deleted_top_down_by_jobs_retention()   // §12 Retention
    {
        var outer = await BatchAsync(state: 2, finishedDaysAgo: 30);
        var nested = await BatchAsync(state: 2, finishedDaysAgo: 31, outer: outer);   // завершён раньше внешнего
        var recent = await BatchAsync(state: 3, finishedDaysAgo: 0);
        var active = await BatchAsync(state: 0);

        Assert.Equal(0, (await Runner(o => o.JobsRetention = null).ApplyAsync(Ct)).DeletedBatches);
        var first = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);
        Assert.Equal(1, first.DeletedBatches);
        Assert.False(await BatchExistsAsync(outer));
        Assert.True(await BatchExistsAsync(nested));                          // внешний существовал при проверке
        var second = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal(1, second.DeletedBatches);
        Assert.False(await BatchExistsAsync(nested));
        Assert.True(await BatchExistsAsync(recent));
        Assert.True(await BatchExistsAsync(active));
    }

    [Fact]
    public async Task Held_batches_are_read_once_per_run()   // §12 Retention
    {
        var outer = await BatchAsync(state: 0);
        await Db.ExecAsync("""
            INSERT INTO {schema}.batches (id, parent_batch_id, state, total, created_at, finished_at)
            SELECT gen_random_uuid(), CASE WHEN g % 2 = 0 THEN @o END, 2, 1, now(), now() - interval '30 days' - g * interval '1 second'
            FROM generate_series(1, 10000) g
            """, ("o", outer));
        var hooks = new HookRecorder();

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.Equal(5000, r.DeletedBatches);                                 // удерживаемые внешним 5000 не удалены
        // Окно — 1000 строк порядка (finished_at, id) независимо от отсева: 10 полных окон и пустое; курсор не возвращается назад.
        Assert.Equal(11, hooks.Count(RetentionStep.BatchesLocked, "batches"));
        Assert.Equal(5001L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.batches"));
        // Прямое измерение «число прочитанных строк не растёт с числом пакетов» — EXPLAIN (ANALYZE) statement'а 1 в начале и в
        // конце диапазона: RetentionWorkflowPlanTests.Held_batches_scan_does_not_grow_with_cursor_position (Direct, Slow).
    }

    [Fact]
    public async Task Batch_refinished_between_window_and_lock_is_not_deleted()   // §7.1: порог — в statement'е с FOR UPDATE
    {
        var batch = await BatchAsync(state: 3, finishedDaysAgo: 30);
        await Db.ExecAsync("UPDATE {schema}.batches SET failed = 1 WHERE id = @id", ("id", batch));
        var member = Guid.CreateVersion7();                                    // сегодняшний Failed-член
        await Db.ExecAsync("""
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, attempt, max_attempts, created_at, finished_at, batch_id, notify)
            VALUES (@id, 'test.job', 'q', 0, '{}', 3, 1, 3, now(), now(), @b, true)
            """, ("id", member), ("b", batch));
        var refinished = false;
        var hooks = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                // Окно уже выбрало batch по старому finished_at; requeue члена и его повторный fail завершают batch заново.
                if (c.Step != RetentionStep.BatchesWindowed || refinished)
                    return;
                refinished = true;
                Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(member, ct)).Result);
                var leased = Assert.Single(await Store.DequeueAsync("q", 1, ServerA, Lease, ct));
                Assert.Equal(FailOutcome.Failed, await Store.FailAsync(member, leased.LeaseId, ServerA, JobEventKind.Failed, "x", null, ct));
                await ResolveAllAsync();
                Assert.True(await Db.ScalarAsync<bool>(
                    "SELECT state = 3 AND finished_at > now() - interval '1 hour' FROM {schema}.batches WHERE id = @id", ("id", batch)));
            },
        };

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.True(refinished);
        Assert.Equal(0, r.DeletedBatches);                                     // моложе JobsRetention
        Assert.True(await BatchExistsAsync(batch));
    }

    [Fact]
    public async Task Failed_member_of_deleted_batch_is_removed_by_next_run()   // §12 Retention
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var batch = await BatchAsync(state: 3, finishedDaysAgo: 30);
        await TerminalAsync(day, 3, batch);
        await InsertTerminalAsync(day, 3);

        var first = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);
        Assert.DoesNotContain(day, first.DroppedJobsPartitions);              // член жив по (б), пока есть строка batch
        Assert.Equal(1, first.DeletedBatches);
        var second = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Contains(day, second.DroppedJobsPartitions);
    }

    [Fact]
    public async Task Continuation_of_old_failed_batch_waits_for_member_requeue()   // §12 Retention
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var batch = await BatchAsync(state: 3, finishedDaysAgo: 30);
        await Db.ExecAsync("UPDATE {schema}.batches SET failed = 1 WHERE id = @id", ("id", batch));
        var member = await TerminalAsync(day, 3, batch);
        await InsertTerminalAsync(day, 150);
        var child = (await _client.Workflow().After(new BatchId(batch)).ContinueWith(new ClientArgs(1)).SubmitAsync(Ct)).Jobs[0].Value;
        await ResolveAllAsync();

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal(0, r.DeletedBatches);                                     // ждущий ребёнок держит строку batch
        Assert.True(await JobExistsAsync(member));
        Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(member, Ct)).Result);
        var leased = Assert.Single(await Store.DequeueAsync("q", 1, ServerA, Lease, Ct));
        Assert.Single(await Store.AckAsync(ServerA, [new LeaseRef(member, leased.LeaseId)], Ct));
        await ResolveAllAsync();

        Assert.True(await Db.ScalarAsync<bool>("SELECT ready AND await_condition IS NULL FROM {schema}.queue WHERE job_id = @id", ("id", child)));
    }

    [Fact]
    public async Task Retention_deleted_batch_references_are_harmless()   // Review Focus 5, §7.1
    {
        var batch = await BatchAsync(state: 3, finishedDaysAgo: 30);
        var member = Guid.CreateVersion7();                                    // сегодняшний Failed-член
        await Db.ExecAsync("""
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, attempt, max_attempts, created_at, finished_at, batch_id, notify)
            VALUES (@id, 'test.job', 'q', 0, '{}', 3, 1, 3, now(), now(), @b, true)
            """, ("id", member), ("b", batch));
        Assert.Equal(1, (await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct)).DeletedBatches);

        Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(member, Ct)).Result);
        var leased = Assert.Single(await Store.DequeueAsync("q", 1, ServerA, Lease, Ct));
        Assert.Single(await Store.AckAsync(ServerA, [new LeaseRef(member, leased.LeaseId)], Ct));

        Assert.Equal([batch], (await Store.CountCompletionsAsync(500, Ct)).MissingBatches);
    }

    [Fact]
    public async Task Batch_subscription_versus_batch_deletion()   // §12 Гонки: подписка на batch против удаления retention'ом
    {
        // (а) подписка первой: удаление пропускает строку (SKIP LOCKED) и не ждёт; ребёнок разрешается.
        var first = await BatchAsync(state: 2, finishedDaysAgo: 30);
        Guid child;
        await using (var conn = await Db.DataSource.OpenConnectionAsync(Ct))
        await using (var tx = await conn.BeginTransactionAsync(Ct))
        {
            child = (await _client.Workflow(new WorkflowOptions { Transaction = tx }).After(new BatchId(first))
                .ContinueWith(new ClientArgs(1)).SubmitAsync(Ct)).Jobs[0].Value;
            Assert.Equal(0, (await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct)).DeletedBatches);
            await tx.CommitAsync(Ct);
        }
        await ResolveAllAsync();
        Assert.True(await Db.ScalarAsync<bool>("SELECT ready AND await_condition IS NULL FROM {schema}.queue WHERE job_id = @id", ("id", child)));
        Assert.Equal(1, (await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct)).DeletedBatches);   // ребёнок запущен — строка не держится

        // (б) удаление первым: подписка ждёт FOR UPDATE и не находит строку.
        var second = await BatchAsync(state: 2, finishedDaysAgo: 30);
        Task<WorkflowResult>? subscribe = null;
        var deleting = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                if (c.Step != RetentionStep.BatchesLocked || subscribe is not null)
                    return;
                subscribe = _client.Workflow().After(new BatchId(second)).ContinueWith(new ClientArgs(2)).SubmitAsync(ct).AsTask();
                await Db.WaitForLockWaitersAsync("batches", ct: Ct);
            },
        };
        Assert.Equal(1, (await Runner(o => o.JobsRetention = Days3, deleting.Tuning()).ApplyAsync(Ct)).DeletedBatches);
        Assert.Equal(second, (await Assert.ThrowsAsync<ParentNotFoundException>(() => subscribe!)).ParentId);

        // (в) ребёнок закоммичен между блокировкой кандидатов и проверкой удаления (хук BatchesLocked, решение плана 4): строка
        // не удаляется. Настоящая подписка между окном и блокировкой — Subscription_between_batch_window_and_lock_keeps_batch.
        var third = await BatchAsync(state: 2, finishedDaysAgo: 30);
        var committed = false;
        var racing = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step != RetentionStep.BatchesLocked || committed)
                    return;
                committed = true;
                await AwaitingChildAsync(third, 3);
            },
        };
        Assert.Equal(0, (await Runner(o => o.JobsRetention = Days3, racing.Tuning()).ApplyAsync(Ct)).DeletedBatches);
        Assert.True(await BatchExistsAsync(third));
    }

    /// <summary>
    /// §12 Гонки, §6.2 п. 4, §7.1: настоящая подписка между окном кандидатов и их блокировкой (хук BatchesWindowed). Открытая —
    /// держит KEY SHARE, блокировка удаления (FOR UPDATE SKIP LOCKED) строку пропускает и не ждёт; закоммиченная до блокировки —
    /// её ждущего ребёнка видит перепроверка statement'а блокировки. В обоих случаях строка цела и ребёнок разрешается.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Subscription_between_batch_window_and_lock_keeps_batch(bool commitBeforeLock)
    {
        var batch = await BatchAsync(state: 2, finishedDaysAgo: 30);
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        Guid? child = null;
        var hooks = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                if (c.Step != RetentionStep.BatchesWindowed || child is not null)
                    return;
                child = (await _client.Workflow(new WorkflowOptions { Transaction = tx }).After(new BatchId(batch))
                    .ContinueWith(new ClientArgs(1)).SubmitAsync(ct)).Jobs[0].Value;
                if (commitBeforeLock)
                    await tx.CommitAsync(ct);
            },
        };

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct);
        if (!commitBeforeLock)
            await tx.CommitAsync(Ct);                                          // удаление завершилось при открытой подписке

        Assert.Equal(0, r.DeletedBatches);
        Assert.True(await BatchExistsAsync(batch));
        await ResolveAllAsync();
        Assert.True(await Db.ScalarAsync<bool>("SELECT ready AND await_condition IS NULL FROM {schema}.queue WHERE job_id = @id",
            ("id", child!.Value)));
        Assert.Equal(1, (await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct)).DeletedBatches);   // строка — кандидат удаления
    }
}

public sealed class RetentionWorkflowTests_Direct(PostgresFixture pg) : RetentionWorkflowTests(pg, ConnectionMode.Direct);
public sealed class RetentionWorkflowTests_Bouncer(PostgresFixture pg) : RetentionWorkflowTests(pg, ConnectionMode.Bouncer);

/// <summary>§12 Retention: стоимость под блокировкой и план удаления batches. Только Direct (EXPLAIN), Slow.</summary>
[Trait("Category", "Slow")]
public sealed class RetentionWorkflowPlanTests(PostgresFixture pg) : RetentionTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Awaiting_parents_scan_does_not_grow_with_children_of_one_parent()
    {
        var day = Ago(10);
        var parent = IdOn(day);
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition)
            SELECT gen_random_uuid(), 'q', 0, 'infinity', false, 3, @p, 1 FROM generate_series(1, 200000);   -- обычный скан читал бы ~2800 буферов
            ANALYZE {schema}.queue;
            """, ("p", parent));
        var sql = Runner().AwaitingParentsSql(batches: false, "$1", "$2", "$3");
        await using var cmd = Db.Admin.CreateCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql);
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = UuidV7.LowerBound(day) });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = UuidV7.LowerBound(day.AddDays(1)) });
        cmd.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 1001 });

        using var doc = JsonDocument.Parse((string)(await cmd.ExecuteScalarAsync(Ct))!);
        var root = doc.RootElement[0].GetProperty("Plan");

        Assert.Equal(1L, root.GetProperty("Actual Rows").GetInt64());
        var blocks = root.GetProperty("Shared Hit Blocks").GetInt64() + root.GetProperty("Shared Read Blocks").GetInt64();
        Assert.True(blocks < 100, $"(a) scan touched {blocks} buffers for one parent with 200 000 children");
    }

    [Fact]
    public async Task Batch_candidates_are_a_range_scan_without_sort()
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.batches (id, state, total, created_at, finished_at)
            SELECT gen_random_uuid(), 2, 1, now(), now() - interval '30 days' - g * interval '1 second' FROM generate_series(1, 20000) g;
            ANALYZE {schema}.batches;
            """);
        await using var cmd = Db.Admin.CreateCommand("EXPLAIN " + Runner().BatchCandidatesSql(withCursor: true));
        cmd.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.UtcNow.AddDays(-7) });
        cmd.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.UtcNow.AddDays(-60) });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = Guid.Empty });
        var lines = new List<string>();
        await using (var r = await cmd.ExecuteReaderAsync(Ct))
            while (await r.ReadAsync(Ct))
                lines.Add(r.GetString(0));
        var plan = string.Join('\n', lines);

        Assert.Contains("batches_finished", plan);
        Assert.DoesNotContain("Sort", plan);
    }

    [Fact]
    public async Task Held_batches_scan_does_not_grow_with_cursor_position()   // §12 Retention: строки читаются запуском один раз
    {
        var outer = Guid.CreateVersion7();
        await Db.ExecAsync("""
            INSERT INTO {schema}.batches (id, state, total, created_at) VALUES (@o, 0, 5000, now());
            INSERT INTO {schema}.batches (id, parent_batch_id, state, total, created_at, finished_at)
            SELECT gen_random_uuid(), CASE WHEN g % 2 = 0 THEN @o END, 2, 1, now(), now() - interval '30 days' - g * interval '1 second'
            FROM generate_series(1, 10000) g;
            ANALYZE {schema}.batches;
            """, ("o", outer));
        // Удерживаемые внешним (чётные g) чередуются с кандидатами; окно statement'а 1 их не отсеивает и читает ровно
        // BatchDeleteChunk строк в любом месте диапазона. Курсор «в конце» — 7000-я строка порядка (finished_at, id): за ним
        // 3000 строк, больше окна.
        DateTime lateAt;
        Guid lateId;
        await using (var position = Db.Admin.CreateCommand(Db.Sql(
                         "SELECT finished_at, id FROM {schema}.batches WHERE finished_at IS NOT NULL ORDER BY finished_at, id OFFSET 6999 LIMIT 1")))
        await using (var r = await position.ExecuteReaderAsync(Ct))
        {
            Assert.True(await r.ReadAsync(Ct));
            (lateAt, lateId) = (r.GetDateTime(0), r.GetGuid(1));
        }

        var early = await CandidateRowsReadAsync(DateTime.UtcNow.AddYears(-1), Guid.Empty);   // курсор до начала диапазона
        var late = await CandidateRowsReadAsync(lateAt, lateId);

        Assert.InRange(early, RetentionRunner.BatchDeleteChunk, RetentionRunner.BatchDeleteChunk + 2);
        Assert.InRange(late, RetentionRunner.BatchDeleteChunk, RetentionRunner.BatchDeleteChunk + 2);
    }

    /// <summary>
    /// Строки, прочитанные узлом Index Scan по batches_finished в EXPLAIN (ANALYZE, BUFFERS) окна statement'а 1 с курсором:
    /// Actual Rows × Loops + Rows Removed by Filter; в плане нет Sort. Транзакция откатывается.
    /// </summary>
    private async Task<long> CandidateRowsReadAsync(DateTime cursorAt, Guid cursorId)
    {
        await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + Runner().BatchCandidatesSql(withCursor: true),
            conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.UtcNow.AddDays(-7) });
        cmd.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cursorAt, DateTimeKind.Utc) });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = cursorId });
        var plan = (string)(await cmd.ExecuteScalarAsync(Ct))!;
        await tx.RollbackAsync(Ct);
        using var doc = JsonDocument.Parse(plan);
        var nodes = Flatten(doc.RootElement[0].GetProperty("Plan")).ToList();
        Assert.DoesNotContain(nodes, n => n.GetProperty("Node Type").GetString() == "Sort");
        var scan = Assert.Single(nodes, n => n.TryGetProperty("Index Name", out var name) && name.GetString() == "batches_finished");
        Assert.Contains(scan.GetProperty("Node Type").GetString(), new[] { "Index Scan", "Index Only Scan" });   // упорядоченный, не Bitmap
        var removed = scan.TryGetProperty("Rows Removed by Filter", out var f) ? f.GetInt64() : 0;
        return scan.GetProperty("Actual Rows").GetInt64() * scan.GetProperty("Actual Loops").GetInt64() + removed;
    }

    private static IEnumerable<JsonElement> Flatten(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var children))
            foreach (var child in children.EnumerateArray())
                foreach (var n in Flatten(child))
                    yield return n;
    }
}
