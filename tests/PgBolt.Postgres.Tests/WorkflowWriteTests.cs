using System.Data;
using Npgsql;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §6.1–§6.2 на уровне хранилища: план графа собирается вручную.</summary>
public abstract class WorkflowWriteTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    private sealed record Row(Guid Id, Guid? Parent = null, short? Code = null, bool Notify = false, Guid? Batch = null, string? Key = null);

    private static EnqueueBatch Rows(params Row[] rows) =>
        new("wf.job", "wf", 0, 3, null, null, TimeSpan.Zero, [.. rows.Select(r => r.Id)], [.. rows.Select(_ => "{}")],
            new DateTime?[rows.Length], [.. rows.Select(r => r.Key)])
        {
            ParentIds = [.. rows.Select(r => r.Parent)],
            Continuations = [.. rows.Select(r => r.Code)],
            Notify = [.. rows.Select(r => r.Notify)],
            BatchIds = [.. rows.Select(r => r.Batch)],
        };

    private static WorkflowPlan Plan(EnqueueBatch jobs, WorkflowBatchRow[]? batches = null, WorkflowParent[]? external = null) =>
        new(batches ?? [], [jobs], external ?? []);


    private Task<string?> SignalRowsAsync() =>
        Db.ScalarAsync<string?>("SELECT string_agg(node_id || ':' || kind, ',' ORDER BY id) FROM {schema}.completions");

    [Fact]
    public async Task Plan_rows_are_written_with_workflow_columns()
    {
        var (batch, root, member, child) = (Guid.CreateVersion7(), Guid.CreateVersion7(), Guid.CreateVersion7(), Guid.CreateVersion7());
        var plan = Plan(Rows(new Row(root, Notify: true), new Row(member, Notify: true, Batch: batch), new Row(child, root, 1)),
            [new WorkflowBatchRow(batch, null, null, null, WorkflowCodes.BatchActive, 1)]);

        Assert.False((await Flow.SubmitWorkflowAsync(plan, null, Ct)).SignalResolver);

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT q.ready AND q.await_condition IS NULL AND q.notify AND j.notify AND j.continuation IS NULL
            FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id WHERE q.job_id = @id
            """, ("id", root)));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT q.ready AND q.batch_id = @b AND j.batch_id = @b AND q.notify AND j.notify
            FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id WHERE q.job_id = @id
            """, ("id", member), ("b", batch)));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT NOT q.ready AND q.run_at = 'infinity' AND q.await_condition = 1 AND q.parent_id = @p AND NOT q.notify
                   AND j.continuation = 1 AND j.parent_id = @p AND NOT j.notify AND j.state = 0
            FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id WHERE q.job_id = @id
            """, ("id", child), ("p", root)));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT state = 0 AND total = 1 AND succeeded = 0 AND NOT cancel_requested AND finished_at IS NULL AND parent_id IS NULL
            FROM {schema}.batches WHERE id = @b
            """, ("b", batch)));
        Assert.Null(await SignalRowsAsync());
    }

    [Fact]
    public async Task Subscription_to_live_parent_sets_queue_notify_without_signal()   // §6.2 п. 1
    {
        var parent = await EnqueueAsync();
        var child = Guid.CreateVersion7();

        await Flow.SubmitWorkflowAsync(Plan(Rows(new Row(child, parent, 1)), external: [new WorkflowParent(parent, false)]), null, Ct);

        Assert.True(await Db.ScalarAsync<bool>("SELECT notify FROM {schema}.queue WHERE job_id = @id", ("id", parent)));
        Assert.Null(await SignalRowsAsync());
        Assert.Equal(0, Signal.Count);
    }

    [Theory]
    [InlineData((short)2)]
    [InlineData((short)3)]
    [InlineData((short)4)]
    public async Task Subscription_to_terminal_parent_writes_synthetic_signal(short state)   // §6.2 п. 2
    {
        var parent = await EnqueueAsync();
        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE job_id = @id; UPDATE {schema}.jobs SET state = @s WHERE id = @id",
            ("id", parent), ("s", state));
        var child = Guid.CreateVersion7();

        Assert.True((await Flow.SubmitWorkflowAsync(Plan(Rows(new Row(child, parent, 2)), external: [new WorkflowParent(parent, false)]),
            null, Ct)).SignalResolver);

        Assert.Equal($"{parent}:{state}", await SignalRowsAsync());
        Assert.True(await Db.ScalarAsync<bool>("SELECT notify FROM {schema}.jobs WHERE id = @id", ("id", parent)));
        Assert.Equal(1, Signal.Count);
    }

    [Fact]
    public async Task Subscription_to_batch_writes_subscribed_signal()   // §6.2 п. 4
    {
        var batch = Guid.CreateVersion7();
        await Db.ExecAsync("INSERT INTO {schema}.batches (id, state, total, created_at) VALUES (@b, 0, 1, now())", ("b", batch));
        var child = Guid.CreateVersion7();

        await Flow.SubmitWorkflowAsync(Plan(Rows(new Row(child, batch, 3)), external: [new WorkflowParent(batch, true)]), null, Ct);

        Assert.Equal($"{batch}:7", await SignalRowsAsync());
    }

    [Theory]
    [InlineData(false, false)]   // нет задачи
    [InlineData(true, false)]    // After(JobId) с id batch
    [InlineData(false, true)]    // After(BatchId) с id задачи
    public async Task Missing_or_wrong_kind_parent_throws_and_writes_nothing(bool batchExists, bool asBatch)   // §6.2 п. 3, 4
    {
        Guid parent;
        if (batchExists)
        {
            parent = Guid.CreateVersion7();
            await Db.ExecAsync("INSERT INTO {schema}.batches (id, state, total, created_at) VALUES (@b, 0, 1, now())", ("b", parent));
        }
        else
        {
            parent = asBatch ? await EnqueueAsync() : Guid.CreateVersion7();
        }
        var jobsBefore = await CountAsync("jobs");

        var e = await Assert.ThrowsAsync<ParentNotFoundException>(() => Flow.SubmitWorkflowAsync(
            Plan(Rows(new Row(Guid.CreateVersion7(), parent, asBatch ? (short)3 : (short)1)), external: [new WorkflowParent(parent, asBatch)]),
            null, Ct));

        Assert.Equal(parent, e.ParentId);
        Assert.Equal(jobsBefore, await CountAsync("jobs"));
        Assert.Null(await SignalRowsAsync());
    }

    [Fact]
    public async Task Unique_conflict_throws_with_existing_id_and_writes_no_graph_rows()
    {
        var existing = await EnqueueAsync(uniqueKey: "u");
        var batch = Guid.CreateVersion7();
        var plan = Plan(Rows(new Row(Guid.CreateVersion7(), Key: "u"), new Row(Guid.CreateVersion7(), Batch: batch, Notify: true)),
            [new WorkflowBatchRow(batch, null, null, null, WorkflowCodes.BatchActive, 1)]);

        var e = await Assert.ThrowsAsync<UniqueKeyConflictException>(() => Flow.SubmitWorkflowAsync(plan, null, Ct));

        Assert.Equal(("u", (JobId?)new JobId(existing)), (e.UniqueKey, e.ExistingId));
        Assert.Equal(1L, await CountAsync("jobs"));
        Assert.Equal(0L, await CountAsync("batches"));
    }

    [Fact]
    public async Task User_transaction_survives_sql_error_inside_submit()   // Review Focus 1, §6.1
    {
        var mine = Guid.CreateVersion7();
        var batch = Guid.CreateVersion7();
        var broken = Plan(Rows(new Row(Guid.CreateVersion7(), Batch: batch, Notify: true)),
            [new WorkflowBatchRow(batch, null, null, null, 0, 1), new WorkflowBatchRow(batch, null, null, null, 0, 1)]);   // 23505 внутри
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, Ct);
        await Store.EnqueueAsync(Batch(mine), tx, Ct);

        var e = await Assert.ThrowsAsync<PostgresException>(() => Flow.SubmitWorkflowAsync(broken, tx, Ct));
        Assert.Equal(PostgresErrorCodes.UniqueViolation, e.SqlState);
        await Store.EnqueueAsync(Batch(Guid.CreateVersion7()), tx, Ct);   // транзакция рабочая после ROLLBACK TO SAVEPOINT
        await tx.CommitAsync(Ct);

        Assert.True(await InQueueAsync(mine));
        Assert.Equal(2L, await CountAsync("jobs"));
        Assert.Equal(0L, await CountAsync("batches"));
        Assert.Equal(0, Signal.Count);
    }

    [Fact]
    public async Task User_transaction_commit_publishes_graph_without_signal()
    {
        var parent = await EnqueueAsync();
        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE job_id = @id; UPDATE {schema}.jobs SET state = 2 WHERE id = @id", ("id", parent));
        var child = Guid.CreateVersion7();
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);

        Assert.True((await Flow.SubmitWorkflowAsync(Plan(Rows(new Row(child, parent, 1)), external: [new WorkflowParent(parent, false)]),
            tx, Ct)).SignalResolver);
        Assert.False(await InQueueAsync(child));   // не закоммичено
        await tx.CommitAsync(Ct);

        Assert.True(await InQueueAsync(child));
        Assert.Equal($"{parent}:2", await SignalRowsAsync());
        Assert.Equal(0, Signal.Count);              // в пользовательской транзакции store не сигналит
    }

    [Fact]
    public async Task Repeated_plan_in_user_transaction_throws_invalid_operation()   // §6.1: внутренний признак повтора не уходит наружу
    {
        var plan = Plan(Rows(new Row(Guid.CreateVersion7()), new Row(Guid.CreateVersion7())));
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, Ct);
        await Flow.SubmitWorkflowAsync(plan, tx, Ct);

        // exists_by_id видит строки первой записи этой же транзакции; без повтора — InvalidOperationException, не внутренний тип.
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => Flow.SubmitWorkflowAsync(plan, tx, Ct));
        Assert.IsType<WorkflowAlreadyWrittenException>(e.InnerException);
        await tx.CommitAsync(Ct);   // транзакция рабочая после ROLLBACK TO SAVEPOINT второй записи

        Assert.Equal(2L, await CountAsync("jobs"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Retry_after_lost_commit_returns_success_without_duplicates(bool withBatch)   // Review Focus 2, §6.1 «Повтор»
    {
        var batch = Guid.CreateVersion7();
        var plan = withBatch
            ? Plan(Rows(new Row(Guid.CreateVersion7(), Batch: batch, Notify: true)), [new WorkflowBatchRow(batch, null, null, null, 0, 1)])
            : Plan(Rows(new Row(Guid.CreateVersion7()), new Row(Guid.CreateVersion7())));

        await Flow.SubmitWorkflowAsync(plan, null, Ct);
        await Flow.SubmitWorkflowAsync(plan, null, Ct);   // повтор той же попытки после потерянного ответа на COMMIT

        Assert.Equal(withBatch ? 1L : 2L, await CountAsync("jobs"));
        Assert.Equal(withBatch ? 1L : 0L, await CountAsync("batches"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Retry_while_previous_attempt_is_in_flight_succeeds(bool withBatch)   // §12 «Падения»
    {
        var batch = Guid.CreateVersion7();
        var plan = withBatch
            ? Plan(Rows(new Row(Guid.CreateVersion7(), Batch: batch, Notify: true)), [new WorkflowBatchRow(batch, null, null, null, 0, 1)])
            : Plan(Rows(new Row(Guid.CreateVersion7()), new Row(Guid.CreateVersion7())));
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await Flow.SubmitWorkflowAsync(plan, tx, Ct);                  // первая попытка: записала, COMMIT ещё не дошёл

        var retry = Flow.SubmitWorkflowAsync(plan, null, Ct);          // повтор ждёт её строк (PK batches / queue)
        await WaitForLockWaitersAsync();
        Assert.False(retry.IsCompleted);
        await tx.CommitAsync(Ct);
        await retry;                                                     // 23505 / exists_by_id → проверка первой строки → успех

        Assert.Equal(withBatch ? 1L : 2L, await CountAsync("jobs"));
        Assert.Equal(withBatch ? 1L : 0L, await CountAsync("batches"));
    }

    [Fact]
    public async Task Workflow_and_recurring_columns_are_exclusive()
    {
        var plan = Rows(new Row(Guid.CreateVersion7())) with { RecurringIds = ["r"] };
        Assert.Throws<ArgumentException>(plan.Validate);
        Assert.Throws<ArgumentException>((Rows(new Row(Guid.CreateVersion7())) with { Notify = null }).Validate);
    }
}

public sealed class WorkflowWriteTests_Direct(PostgresFixture pg) : WorkflowWriteTests(pg, ConnectionMode.Direct);
public sealed class WorkflowWriteTests_Bouncer(PostgresFixture pg) : WorkflowWriteTests(pg, ConnectionMode.Bouncer);

/// <summary>§4: поля графа пишет только планировщик workflow — публичный EnqueueAsync не может записать Awaiting-строки.</summary>
public sealed class WorkflowEnqueueBatchApiTests
{
    [Theory]
    [InlineData(nameof(EnqueueBatch.ParentIds))]
    [InlineData(nameof(EnqueueBatch.Continuations))]
    [InlineData(nameof(EnqueueBatch.Notify))]
    [InlineData(nameof(EnqueueBatch.BatchIds))]
    public void Workflow_fields_of_EnqueueBatch_are_not_settable_outside_the_library(string property)
    {
        var setter = typeof(EnqueueBatch).GetProperty(property)!.SetMethod!;

        Assert.True(setter.IsAssembly, $"{property} init accessor must be internal");
    }
}
