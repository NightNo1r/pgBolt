using System.Diagnostics;
using Npgsql;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §12 «Гонки» и «Падения»: два-три соединения, ручные транзакции, держатели блокировок и хуки.</summary>
public abstract class WorkflowRaceTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    public enum Finalizer { Ack, Fail, Cancel }

    private static readonly TimeSpan NoWait = TimeSpan.FromSeconds(10);

    private Task<WorkflowResult> SubscribeAsync(Guid parent, NpgsqlTransaction? tx = null,
        ContinuationCondition condition = ContinuationCondition.OnAnyFinished) =>
        Client.Workflow(new WorkflowOptions { Transaction = tx }).After(new JobId(parent)).ContinueWith(A(1), condition).SubmitAsync(Ct).AsTask();

    private async Task<Func<Task>> FinalizerAsync(Finalizer kind, Guid parent)
    {
        switch (kind)
        {
            case Finalizer.Ack:
            {
                var lease = await LeaseAsync(parent);
                return async () => Assert.Single(await Flow.AckAsync(ServerA, [new LeaseRef(parent, lease)], Ct));
            }
            case Finalizer.Fail:
            {
                var lease = await LeaseAsync(parent);
                return async () => Assert.Equal(FailOutcome.Failed,
                    await Flow.FailAsync(parent, lease, ServerA, JobEventKind.Failed, "x", null, Ct));
            }
            default:
                return async () => Assert.Equal(CancelResult.Cancelled, await Flow.CancelAsync(parent, "x", Ct));
        }
    }

    [Theory]
    [InlineData(Finalizer.Ack, true)]
    [InlineData(Finalizer.Ack, false)]
    [InlineData(Finalizer.Fail, true)]
    [InlineData(Finalizer.Fail, false)]
    [InlineData(Finalizer.Cancel, true)]
    [InlineData(Finalizer.Cancel, false)]
    public async Task Subscription_versus_finalization(Finalizer kind, bool subscribeFirst)   // §12 Гонки, §6.2 «Почему гонки нет»
    {
        var parent = (await Client.Workflow().Enqueue(A(0)).SubmitAsync(Ct)).Jobs[0].Value;
        var finalize = await FinalizerAsync(kind, parent);
        Guid child;
        if (subscribeFirst)
        {
            await using var user = await BeginAsync();
            child = (await SubscribeAsync(parent, user.Transaction)).Jobs[0].Value;   // п. 1 держит строку queue родителя
            var final = finalize();
            await WaitForLockWaitersAsync();
            Assert.False(final.IsCompleted);
            await user.CommitAsync();
            await final;                                                           // DELETE … RETURNING видит notify = true
        }
        else
        {
            await using var holder = await HoldRowsAsync("SELECT 1 FROM {schema}.queue WHERE job_id = @id FOR UPDATE", ("id", parent));
            var final = finalize();
            await WaitForLockWaitersAsync(1);                                      // финализация первой в очереди строки
            var subscribe = SubscribeAsync(parent);
            await WaitForLockWaitersAsync(2);                                      // п. 1 подписки — второй
            await holder.DisposeAsync();
            await final;
            child = (await subscribe).Jobs[0].Value;                               // п. 1 не нашёл строку → п. 2 видит терминальное
        }

        await ResolveAllAsync();

        Assert.Equal("launched", await FateAsync(child));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Subscription_versus_sweeper(bool subscribeFirst)   // §12 Гонки
    {
        var parent = (await Client.Workflow().Enqueue(A(0), new StepOptions { MaxAttempts = 1 }).SubmitAsync(Ct)).Jobs[0].Value;
        await LeaseAsync(parent);
        await ExpireLeaseAsync(parent);
        Guid child;
        if (subscribeFirst)
        {
            await using var user = await BeginAsync();
            child = (await SubscribeAsync(parent, user.Transaction)).Jobs[0].Value;
            Assert.Equal(0, await Flow.SweepExpiredLeasesAsync(100, Ct));            // SKIP LOCKED: строку держит подписка
            await user.CommitAsync();
            Assert.Equal(1, await Flow.SweepExpiredLeasesAsync(100, Ct));
        }
        else
        {
            Assert.Equal(1, await Flow.SweepExpiredLeasesAsync(100, Ct));
            child = (await SubscribeAsync(parent)).Jobs[0].Value;
        }

        await ResolveAllAsync();

        Assert.Equal((short?)3, await JobStateAsync(parent));
        Assert.Equal("launched", await FateAsync(child));
    }

    [Fact]
    public async Task Subscription_to_failed_parent_waits_for_requeue_and_releases_jobs_row()   // §12 Гонки, §6.2 пп. 2–3
    {
        var parent = (await Client.Workflow().Enqueue(A(0)).SubmitAsync(Ct)).Jobs[0].Value;
        await FailFinallyAsync(parent);
        await using var holder = await HoldRowsAsync("SELECT 1 FROM {schema}.jobs WHERE id = @id FOR UPDATE", ("id", parent));
        var requeue = Client.RequeueAsync(new JobId(parent), Ct).AsTask();
        await WaitForLockWaitersAsync(1);                                            // requeue ждёт держателя строки jobs
        await using var user = await BeginAsync();
        var subscribe = SubscribeAsync(parent, user.Transaction, ContinuationCondition.OnSuccess);
        await WaitForLockWaitersAsync(2);                                            // п. 2 подписки ждёт за requeue
        await holder.DisposeAsync();

        Assert.Equal(RequeueResult.Requeued, await requeue);
        var child = (await subscribe).Jobs[0].Value;                                 // 0 строк → откат к точке сохранения → п. 1
        // notify = true п. 1 видна только в незакоммиченной транзакции подписки: закоммиченную строку queue requeue создал из
        // jobs.notify = false (UPDATE jobs п. 2 откатан к pgbolt_after) — читать через Db (Admin) до коммита нельзя.
        await using (var notify = new NpgsqlCommand(Db.Sql("SELECT notify FROM {schema}.queue WHERE job_id = @id"),
                         user.Connection, user.Transaction))
        {
            notify.Parameters.AddWithValue("id", parent);
            Assert.True((bool)(await notify.ExecuteScalarAsync(Ct))!);
        }
        await using (var probe = await Db.Admin.OpenConnectionAsync(Ct))             // строка jobs не заблокирована подпиской
        await using (var nowait = new NpgsqlCommand(Db.Sql("SELECT 1 FROM {schema}.jobs WHERE id = @id FOR UPDATE NOWAIT"), probe))
        {
            await using var probeTx = await probe.BeginTransactionAsync(Ct);
            nowait.Transaction = probeTx;
            nowait.Parameters.AddWithValue("id", parent);
            await nowait.ExecuteScalarAsync(Ct);
        }

        var cancel = Flow.CancelAsync(parent, "x", Ct);                              // ждёт строку queue, взятую п. 1
        await WaitForLockWaitersAsync(1);
        await user.CommitAsync();
        Assert.Equal(CancelResult.Cancelled, await cancel.WaitAsync(NoWait, Ct));    // без 40P01

        await ResolveAllAsync();
        Assert.Equal("cancelled", await FateAsync(child));
    }

    public enum QueueHolder { Ack, CancelMembers, Cancel }

    /// <summary>
    /// §12 Гонки п. 2, §6.2 «Почему гонки нет»: п. 2 подписки ждёт requeue и после его коммита блокирует новую версию строки jobs
    /// (0 строк). Ответ п. 2 задерживается прокси, поэтому подписка держит jobs, пока операция берёт строку queue перезапущенной
    /// задачи и встаёт за ней в ожидание jobs. Откат к pgbolt_after снимает блокировку, п. 1 ждёт строку queue операции — без
    /// отката был бы цикл и 40P01. Операция завершается до коммита подписки; п. 1 после неё строки не находит, п. 2 видит
    /// терминальное состояние.
    /// </summary>
    [Theory]
    [InlineData(QueueHolder.Ack)]
    [InlineData(QueueHolder.CancelMembers)]
    [InlineData(QueueHolder.Cancel)]
    public async Task Subscription_retry_after_requeue_does_not_deadlock_with_operation_holding_queue(QueueHolder op)
    {
        // CancelMembers ждёт jobs под lock_timeout = '1s' порции (§6.5). Если от входа операции в ожидание до отката подписки
        // к pgbolt_after прошло больше секунды (загруженный CI), порция откатывается по таймауту: попытка не доказывает отсутствие
        // цикла и повторяется на новом batch. Регрессия (подписка держит jobs и ждёт queue — цикл) таймаутит каждую попытку,
        // последняя требует (1, 0) — тест падает.
        const int attempts = 3;
        for (var attempt = 1; attempt < attempts; attempt++)
            if (await SubscriptionRetryAttemptAsync(op, lastAttempt: false))
                return;
        Assert.True(await SubscriptionRetryAttemptAsync(op, lastAttempt: true));
    }

    /// <summary>Одна попытка сценария; false — порция CancelMembers откатилась по lock_timeout (не последняя попытка).</summary>
    private async Task<bool> SubscriptionRetryAttemptAsync(QueueHolder op, bool lastAttempt)
    {
        var conclusive = true;
        IJobStep? member = null;
        var batch = (await Client.Workflow().StartBatch(b =>
        {
            member = b.Enqueue(A(0));
            b.Enqueue(A(1));
        }).SubmitAsync(Ct)).Batches[0];
        var parent = member!.Id.Value;
        await FailFinallyAsync(parent);
        // retryOnConflict: false — 40P01 операции не гасится повтором и проявился бы исключением
        var strict = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: false);

        var proxy = TcpProxy.For(Db.ConnectionString);
        try
        {
            await using var userSource = NpgsqlDataSource.Create(proxy.ConnectionStringVia(Db.ConnectionString));
            await using var conn = await userSource.OpenConnectionAsync(Ct);
            await using var tx = await conn.BeginTransactionAsync(Ct);
            Task<WorkflowResult>? subscribe = null;
            try
            {
                await using (var holder = await HoldRowsAsync("SELECT 1 FROM {schema}.jobs WHERE id = @id FOR UPDATE", ("id", parent)))
                {
                    var requeue = Client.RequeueAsync(new JobId(parent), Ct).AsTask();
                    await WaitForLockWaitersAsync(1);                                // requeue ждёт держателя строки jobs
                    subscribe = SubscribeAsync(parent, tx, ContinuationCondition.OnSuccess);
                    await WaitForLockWaitersAsync(2);                                // п. 2 подписки — за requeue
                    proxy.Pause();
                    await holder.DisposeAsync();
                    Assert.Equal(RequeueResult.Requeued, await requeue);
                }
                await WaitForSubscriptionStep2DoneAsync();                           // п. 2 держит новую версию jobs

                Func<Task> operation;
                switch (op)
                {
                    case QueueHolder.Ack:
                    {
                        var lease = await LeaseAsync(parent);
                        operation = async () => Assert.Single(await strict.AckAsync(ServerA, [new LeaseRef(parent, lease)], Ct));
                        break;
                    }
                    case QueueHolder.CancelMembers:
                        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch, Ct));
                        operation = async () =>
                        {
                            var result = await strict.CancelMembersAsync(1000, 5000, 100, TimeSpan.FromSeconds(30), Ct);
                            if (!lastAttempt && result is { Portions: 0, TimedOut: 1 })
                                conclusive = false;                                  // сигнал отложен на retryDelay
                            else
                                Assert.Equal((1, 0), (result.Portions, result.TimedOut));
                        };
                        break;
                    default:
                        operation = async () => Assert.Equal(CancelResult.Cancelled, await strict.CancelAsync(parent, "x", Ct));
                        break;
                }
                var running = operation();
                await WaitForLockWaitersAsync(1);                                    // операция держит queue и ждёт jobs
                Assert.False(subscribe.IsCompleted);
                proxy.Resume();

                await running.WaitAsync(NoWait, Ct);                                 // без 40P01, подписка не закоммичена
                var child = (await subscribe.WaitAsync(NoWait, Ct)).Jobs[0].Value;
                await tx.CommitAsync(Ct);
                if (!conclusive)
                    await Db.ExecAsync("UPDATE {schema}.completions SET retry_after = NULL");   // прошёл retryDelay
                await ResolveAllAsync();

                var (parentState, childFate) = op == QueueHolder.Ack ? ((short?)2, "launched") : ((short?)4, "cancelled");
                Assert.Equal(parentState, await JobStateAsync(parent));
                Assert.Equal(childFate, await FateAsync(child));
                return conclusive;
            }
            catch
            {
                proxy.Resume();
                proxy.DropAll();                                                     // сервер откатит транзакцию подписки
                if (subscribe is not null)                                           // dispose conn не маскирует исходную ошибку
                    await Task.WhenAny(subscribe, Task.Delay(NoWait, CancellationToken.None));
                throw;
            }
        }
        finally
        {
            await proxy.DisposeAsync();
        }
    }

    /// <summary>Ждёт, пока backend подписки выполнит п. 2 (UPDATE jobs) и встанет в idle in transaction.</summary>
    private async Task WaitForSubscriptionStep2DoneAsync()
    {
        var started = Stopwatch.GetTimestamp();
        while (await Db.ScalarAsync<long>("SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction' AND query LIKE @p",
                   ("p", $"%{Db.Schema}.jobs SET notify%")) < 1)
        {
            if (Stopwatch.GetElapsedTime(started) > TimeSpan.FromSeconds(10))
                throw new TimeoutException("The subscription did not finish step 2.");
            await Task.Delay(20, Ct);
        }
    }

    [Fact]
    public async Task Requeue_waits_for_user_transaction_subscribed_to_terminal_parent()   // §12 Гонки
    {
        var parent = (await Client.Workflow().Enqueue(A(0)).SubmitAsync(Ct)).Jobs[0].Value;
        await FailFinallyAsync(parent);
        await using var user = await BeginAsync();
        var child = (await SubscribeAsync(parent, user.Transaction, ContinuationCondition.OnSuccess)).Jobs[0].Value;   // п. 2 держит jobs

        var requeue = Client.RequeueAsync(new JobId(parent), Ct).AsTask();
        await WaitForLockWaitersAsync(1);
        Assert.False(requeue.IsCompleted);
        await user.CommitAsync();

        Assert.Equal(RequeueResult.Requeued, await requeue);
        Assert.True(await Db.ScalarAsync<bool>("SELECT notify FROM {schema}.queue WHERE job_id = @id", ("id", parent)));   // из jobs.notify
        await SucceedAsync(parent);
        await ResolveAllAsync();
        Assert.Equal("launched", await FateAsync(child));
    }

    [Fact]
    public async Task Batch_finished_before_subscription_commit_resolves_children_by_subscribed()   // §12 Гонки
    {
        IJobStep? m = null;
        var batch = (await Client.Workflow().StartBatch(b => m = b.Enqueue(A(1))).SubmitAsync(Ct)).Batches[0];
        await using var user = await BeginAsync();
        var child = (await Client.Workflow(new WorkflowOptions { Transaction = user.Transaction })
            .After(batch).ContinueWith(A(2)).SubmitAsync(Ct)).Jobs[0].Value;

        await SucceedAsync(m!.Id.Value);
        await ResolveAllAsync().WaitAsync(NoWait, Ct);                               // учёт не ждёт KEY SHARE подписки
        Assert.Equal((short?)2, await BatchStateAsync(batch.Value));                 // сигнал batch обработан до коммита детей
        await user.CommitAsync();
        await ResolveAllAsync();

        Assert.Equal("launched", await FateAsync(child));
    }

    [Fact]
    public async Task Open_batch_subscription_does_not_block_counting_requeue_or_cancel()   // §12 Гонки
    {
        IReadOnlyList<IJobStep> members = [];
        var batch = (await Client.Workflow().StartBatch(b => members = b.EnqueueMany(new[] { A(1), A(2), A(3) })).SubmitAsync(Ct)).Batches[0];
        await FailFinallyAsync(members[0].Id.Value);
        await using var user = await BeginAsync();
        await Client.Workflow(new WorkflowOptions { Transaction = user.Transaction }).After(batch).ContinueWith(A(9)).SubmitAsync(Ct);

        await SucceedAsync(members[1].Id.Value);
        await ResolveWithoutCancelMembersAsync().WaitAsync(NoWait, Ct);
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(members[0].Id, Ct).AsTask().WaitAsync(NoWait, Ct));
        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch, Ct).AsTask().WaitAsync(NoWait, Ct));

        await user.CommitAsync();
    }

    [Fact]
    public async Task Stale_cancelled_signal_does_not_cancel_new_OnSuccess_child()   // §12 Гонки «Устаревший сигнал»
    {
        var r = await Client.Workflow().Enqueue(A(0)).ContinueWith(A(1), ContinuationCondition.OnAnyFinished).SubmitAsync(Ct);
        var parent = r.Jobs[0];
        await CancelJobAsync(parent.Value);                                          // сигнал Cancelled не обработан
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(parent, Ct));
        var child = (await Client.Workflow().After(parent).ContinueWith(A(2)).SubmitAsync(Ct)).Jobs[0].Value;

        await ResolveAllAsync();
        Assert.Equal("awaiting", await FateAsync(child));

        await SucceedAsync(parent.Value);
        await ResolveAllAsync();
        Assert.Equal("launched", await FateAsync(child));
    }

    [Fact]
    public async Task Stale_failed_signal_does_not_launch_OnAnyFinished_child_while_parent_runs()   // §12 Гонки
    {
        var parent = (await Client.Workflow().Enqueue(A(0)).ContinueWith(A(1)).SubmitAsync(Ct)).Jobs[0];
        await FailFinallyAsync(parent.Value);
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(parent, Ct));
        var lease = await LeaseAsync(parent.Value);                                  // родитель снова выполняется
        var child = (await Client.Workflow().After(parent).ContinueWith(A(2), ContinuationCondition.OnAnyFinished).SubmitAsync(Ct)).Jobs[0].Value;

        await ResolveAllAsync();
        Assert.Equal("awaiting", await FateAsync(child));

        Assert.Single(await Flow.AckAsync(ServerA, [new LeaseRef(parent.Value, lease)], Ct));
        await ResolveAllAsync();
        Assert.Equal("launched", await FateAsync(child));
    }

    [Fact]
    public async Task Stale_batch_outcome_after_member_requeue_is_dropped()   // §12 Гонки
    {
        IJobStep? x = null;
        var batch = (await Client.Workflow().StartBatch(b => x = b.Enqueue(A(1))).SubmitAsync(Ct)).Batches[0];
        await FailFinallyAsync(x!.Id.Value);
        await Flow.CountCompletionsAsync(500, Ct);                                   // batch Failed, его сигнал не обработан
        Assert.Equal((short?)3, await BatchStateAsync(batch.Value));
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(x.Id, Ct));
        var child = (await Client.Workflow().After(batch).ContinueWith(A(2), ContinuationCondition.OnAnyFinished).SubmitAsync(Ct)).Jobs[0].Value;

        await ResolveAllAsync();
        Assert.Equal("awaiting", await FateAsync(child));

        await SucceedAsync(x.Id.Value);
        await ResolveAllAsync();
        Assert.Equal("launched", await FateAsync(child));
    }

    [Fact]
    public async Task Two_resolvers_count_one_batch_exactly_without_deadlock()   // §12 Гонки
    {
        IReadOnlyList<IJobStep> members = [];
        var batch = (await Client.Workflow().StartBatch(b => members = b.EnqueueMany(Enumerable.Range(0, 2000).Select(A).ToArray()))
            .SubmitAsync(Ct)).Batches[0];
        await SucceedManyAsync([.. members.Select(m => m.Id.Value)]);
        // retryOnConflict: false — 40P01 не гасится повтором и проявился бы исключением
        var resolvers = Enumerable.Range(0, 2)
            .Select(_ => new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: false)).ToArray();

        await Task.WhenAll(resolvers.Select(s => Task.Run(async () =>
        {
            while ((await s.CountCompletionsAsync(100, Ct)).Captured > 0)
            {
            }
        }, Ct)));
        await ResolveAllAsync();

        Assert.True(await Db.ScalarAsync<bool>("SELECT state = 2 AND succeeded = 2000 AND failed = 0 FROM {schema}.batches WHERE id = @id",
            ("id", batch.Value)));
    }

    [Theory]
    [InlineData(WorkflowOutcome.Cancelled, true, false)]
    [InlineData(WorkflowOutcome.Cancelled, false, false)]
    [InlineData(WorkflowOutcome.Failed, true, false)]
    [InlineData(WorkflowOutcome.Failed, false, false)]
    [InlineData(WorkflowOutcome.Cancelled, true, true)]
    [InlineData(WorkflowOutcome.Failed, false, true)]
    public async Task Requeue_versus_counting_keeps_batch_active(WorkflowOutcome first, bool countedBeforeRequeue, bool nested)   // §12 Гонки
    {
        IJobStep? x = null, y = null;
        IBatchStep? inner = null;
        var wf = Client.Workflow();
        var batch = nested
            ? wf.StartBatch(o =>
            {
                inner = o.StartBatch(b => x = b.Enqueue(A(1)));
                y = o.Enqueue(A(2));
            })
            : wf.StartBatch(b =>
            {
                x = b.Enqueue(A(1));
                y = b.Enqueue(A(2));
            });
        var onSuccess = batch.ContinueWith(A(3));
        var onAny = batch.ContinueWith(A(4), ContinuationCondition.OnAnyFinished);
        await wf.SubmitAsync(Ct);
        var yLease = await LeaseAsync(y!.Id.Value);                                  // Y выполняется
        await FinishAsync(x!.Id.Value, first);
        if (countedBeforeRequeue)
            await ResolveAllAsync();                                                 // сигнал X учтён до requeue

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(x.Id, Ct));
        if (nested)
            Assert.Equal((short?)0, await BatchStateAsync(inner!.Id.Value));         // вложенный вернулся в Active
        await SucceedAsync(x.Id.Value);
        await ResolveAllAsync();

        Assert.Equal((short?)0, await BatchStateAsync(batch.Id.Value));
        Assert.Equal("awaiting", await FateAsync(onSuccess.Id.Value));               // не отменено
        Assert.Equal("awaiting", await FateAsync(onAny.Id.Value));                   // не запущено раньше времени

        Assert.Single(await Flow.AckAsync(ServerA, [new LeaseRef(y.Id.Value, yLease)], Ct));
        await ResolveAllAsync();

        Assert.Equal((short?)2, await BatchStateAsync(batch.Id.Value));
        Assert.Equal("launched", await FateAsync(onSuccess.Id.Value));
        Assert.Equal("launched", await FateAsync(onAny.Id.Value));
    }

    [Fact]
    public async Task Subscription_holding_awaiting_child_does_not_stall_resolver()   // §12 Гонки, Review Focus 3
    {
        var first = await Client.Workflow().Enqueue(A(0)).ContinueWith(A(1)).SubmitAsync(Ct);
        var second = await Client.Workflow().Enqueue(A(2)).ContinueWith(A(3)).SubmitAsync(Ct);
        var held = first.Jobs[1];
        await using var user = await BeginAsync();
        await Client.Workflow(new WorkflowOptions { Transaction = user.Transaction }).After(held).ContinueWith(A(4)).SubmitAsync(Ct);
        await SucceedAsync(first.Jobs[0].Value);
        await SucceedAsync(second.Jobs[0].Value);

        await ResolveAllAsync().WaitAsync(NoWait, Ct);

        Assert.Equal("launched", await FateAsync(second.Jobs[1].Value));
        Assert.Equal("awaiting", await FateAsync(held.Value));
        Assert.True(await Db.ScalarAsync<bool>("SELECT retry_after > now() FROM {schema}.completions WHERE node_id = @id",
            ("id", first.Jobs[0].Value)));

        await user.CommitAsync();
        await Db.ExecAsync("UPDATE {schema}.completions SET retry_after = NULL");   // прошёл ResolverInterval
        await ResolveAllAsync();
        Assert.Equal("launched", await FateAsync(held.Value));
    }

    [Fact]
    public async Task Open_subscription_blocks_heartbeat_of_running_parent_until_commit()   // §12 Гонки (документированное поведение)
    {
        var parent = (await Client.Workflow().Enqueue(A(0)).SubmitAsync(Ct)).Jobs[0].Value;
        var lease = await LeaseAsync(parent);
        await using var user = await BeginAsync();
        await SubscribeAsync(parent, user.Transaction);

        var heartbeat = Flow.HeartbeatAsync(ServerA, [new LeaseRef(parent, lease)], Lease, Ct);
        await WaitForLockWaitersAsync();
        Assert.False(heartbeat.IsCompleted);
        await user.CommitAsync();

        Assert.Single((await heartbeat).Leases);
    }

    [Fact]
    public async Task Open_subscription_hides_pending_parent_from_dequeue_until_commit()   // §12 Гонки (документированное поведение)
    {
        var parent = (await Client.Workflow().Enqueue(A(0)).SubmitAsync(Ct)).Jobs[0].Value;
        await using var user = await BeginAsync();
        await SubscribeAsync(parent, user.Transaction);

        Assert.Empty(await Flow.DequeueAsync("client", 10, ServerA, Lease, Ct));      // SKIP LOCKED
        await user.CommitAsync();

        Assert.Equal(parent, Assert.Single(await Flow.DequeueAsync("client", 10, ServerA, Lease, Ct)).Id);
    }

    [Fact]
    public async Task Signal_written_by_ack_is_resolved_by_another_process()   // §12 Падения
    {
        var r = await Client.Workflow().Enqueue(A(0)).ContinueWith(A(1)).SubmitAsync(Ct);
        await SucceedAsync(r.Jobs[0].Value);                                         // «процесс» умер, не разбудив resolver
        var other = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: true);

        await other.CountCompletionsAsync(500, Ct);
        await other.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);

        Assert.Equal("launched", await FateAsync(r.Jobs[1].Value));
    }

    [Fact]
    public async Task Failure_inside_walk_rolls_back_and_retry_launches_once_and_counts_once()   // §12 Падения
    {
        IJobStep? m = null;
        var batch = Client.Workflow().StartBatch(b => m = b.Enqueue(A(1)));
        var next = batch.ContinueWith(A(2));
        await batch.SubmitAsync(Ct);
        await SucceedAsync(m!.Id.Value);
        await Flow.CountCompletionsAsync(500, Ct);                                   // счётчик учтён, сигнал batch создан
        await Flow.CountCompletionsAsync(500, Ct);                                   // сигнал batch передан Т2
        Flow.ResolverHook = (step, _) => step == ResolverStep.Walked ? throw new InvalidOperationException("crash") : Task.CompletedTask;

        await Assert.ThrowsAsync<InvalidOperationException>(() => Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct));
        Assert.Equal("awaiting", await FateAsync(next.Id.Value));                    // откат: запуск не закоммичен
        Flow.ResolverHook = null;
        await ResolveAllAsync();

        Assert.Equal("launched", await FateAsync(next.Id.Value));
        Assert.True(await Db.ScalarAsync<bool>("SELECT state = 2 AND succeeded = 1 FROM {schema}.batches WHERE id = @id", ("id", batch.Id.Value)));
        Assert.Equal(0L, await SignalsAsync());
    }
}

public sealed class WorkflowRaceTests_Direct(PostgresFixture pg) : WorkflowRaceTests(pg, ConnectionMode.Direct);
public sealed class WorkflowRaceTests_Bouncer(PostgresFixture pg) : WorkflowRaceTests(pg, ConnectionMode.Bouncer);
