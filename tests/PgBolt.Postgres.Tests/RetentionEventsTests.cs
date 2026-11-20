using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class RetentionEventsTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    private static readonly TimeSpan Days3 = TimeSpan.FromDays(3);

    [Fact]
    public async Task Fresh_schema_gives_empty_result()   // Review Focus 1
    {
        var r = await Runner(o => { o.JobsRetention = Days3; o.EventsRetention = Days3; }).ApplyAsync(Ct);

        Assert.Empty(r.DroppedJobsPartitions);
        Assert.Empty(r.DroppedEventsPartitions);
        Assert.Empty(r.Pinned);
        Assert.Empty(r.Deferred);
        Assert.Equal(0, r.DroppedCompactionRemnants);
        Assert.Equal(3, r.AttachedJobsPartitions);   // сегодня + 2
    }

    [Fact]
    public async Task Events_candidates_follow_day_boundary()
    {
        foreach (var d in new[] { 5, 4, 3, 1 })
            await CreatePartitionsAsync(Ago(d), jobs: false);

        var r = await Runner(o => o.EventsRetention = Days3).ApplyAsync(Ct);   // horizon = today − 4

        Assert.Equal([Ago(5), Ago(4)], r.DroppedEventsPartitions);
        Assert.False(await ExistsAsync(E(Ago(5))));
        Assert.False(await ExistsAsync(E(Ago(4))));
        Assert.True(await AttachedAsync(E(Ago(3))));
        Assert.True(await AttachedAsync(E(Ago(1))));
        Assert.True(await AttachedAsync(E(Today)));
        Assert.True(await AttachedAsync(E(Today.AddDays(2))));
    }

    [Fact]
    public async Task Events_retention_rounds_up_and_null_keeps_everything()
    {
        await CreatePartitionsAsync(Ago(3), jobs: false);
        await CreatePartitionsAsync(Ago(2), jobs: false);

        Assert.Empty((await Runner().ApplyAsync(Ct)).DroppedEventsPartitions);
        var r = await Runner(o => o.EventsRetention = TimeSpan.FromHours(36)).ApplyAsync(Ct);   // Rd = 2 → horizon = today − 3

        Assert.Equal([Ago(3)], r.DroppedEventsPartitions);
        Assert.True(await AttachedAsync(E(Ago(2))));
    }

    [Fact]
    public async Task Remnants_are_dropped_with_any_retention()
    {
        var recent = Ago(1);
        await CreatePartitionsAsync(recent);
        await DetachAsync("jobs", J(recent));
        await DetachAsync("job_events", E(recent));
        await Db.ExecAsync($"CREATE TABLE {{schema}}.{J(Ago(2))}_r4242 (LIKE {{schema}}.jobs)");

        var r = await Runner().ApplyAsync(Ct);   // оба срока null

        Assert.Equal([recent], r.DroppedJobsPartitions);
        Assert.Equal([recent], r.DroppedEventsPartitions);
        Assert.Equal(1, r.DroppedCompactionRemnants);
        Assert.False(await ExistsAsync(J(recent)));
        Assert.False(await ExistsAsync(E(recent)));
        Assert.False(await ExistsAsync($"{J(Ago(2))}_r4242"));
    }

    [Fact]
    public async Task Locked_remnant_is_deferred_and_others_are_dropped()
    {
        await CreatePartitionsAsync(Ago(1));
        await DetachAsync("jobs", J(Ago(1)));
        await DetachAsync("job_events", E(Ago(1)));
        await using (var holder = await HoldAsync(J(Ago(1)), "ACCESS SHARE"))
        {
            var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

            var deferred = Assert.Single(r.Deferred);
            Assert.Equal(new DeferredPartition("jobs", Ago(1), DeferReason.DropLockTimeout, 0, null), deferred);
            Assert.Equal([Ago(1)], r.DroppedEventsPartitions);
            await EnqueueAsync();   // горячий путь не затронут: родитель не блокируется
        }

        Assert.Equal([Ago(1)], (await Runner().ApplyAsync(Ct)).DroppedJobsPartitions);
    }

    [Fact]
    public async Task Concurrent_drops_of_one_detached_table_count_it_once()   // §5.5: перекрытие в транзакции 2
    {
        await CreatePartitionsAsync(Ago(1), events: false);
        await DetachAsync("jobs", J(Ago(1)));
        RetentionRunner Run() => Runner(o => o.RetentionLockTimeout = TimeSpan.FromSeconds(30));
        Task<RetentionResult> first, second;
        var holder = await HoldAsync(J(Ago(1)), "ACCESS SHARE");
        try
        {
            first = Run().ApplyAsync(Ct);
            second = Run().ApplyAsync(Ct);
            await WaitForLockWaitersAsync(J(Ago(1)), 2);   // оба запуска прошли проверку oid, оба DROP ждут блокировку
        }
        finally
        {
            await holder.DisposeAsync();
        }

        var results = await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(10), Ct);

        // Первый DROP удаляет таблицу, второй после ожидания получает 42P01 → Skipped.
        Assert.Equal(new[] { Ago(1) }, results.SelectMany(r => r.DroppedJobsPartitions));
        Assert.All(results, r => Assert.Empty(r.Deferred));
        Assert.False(await ExistsAsync(J(Ago(1))));
    }

    [Fact]
    public async Task Cancellation_while_drop_waits_for_lock_throws_instead_of_drop_failed()   // §12
    {
        await CreatePartitionsAsync(Ago(1), events: false);
        await DetachAsync("jobs", J(Ago(1)));
        await using (var holder = await HoldAsync(J(Ago(1)), "ACCESS SHARE"))
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            var run = Runner(o => o.RetentionLockTimeout = TimeSpan.FromSeconds(30)).ApplyAsync(cts.Token);
            await WaitForLockWaitersAsync(J(Ago(1)), 1);   // DROP транзакции 2 ждёт блокировку, до lock_timeout ещё 30 с
            await cts.CancelAsync();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(TimeSpan.FromSeconds(10), Ct));
            Assert.True(await ExistsAsync(J(Ago(1))));
        }

        Assert.Equal([Ago(1)], (await Runner().ApplyAsync(Ct)).DroppedJobsPartitions);
    }

    [Fact]
    public async Task Autovacuum_like_lock_on_todays_partition_does_not_block_removal()
    {
        await CreatePartitionsAsync(Ago(10), jobs: false);
        await using var holder = await HoldAsync(E(Today), "SHARE UPDATE EXCLUSIVE");

        var r = await Runner(o => o.EventsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal([Ago(10)], r.DroppedEventsPartitions);
        Assert.Empty(r.Deferred);
    }

    [Fact]
    public async Task Direct_holder_of_old_partition_is_deferred_before_lock()
    {
        await CreatePartitionsAsync(Ago(11), jobs: false);
        await CreatePartitionsAsync(Ago(10), jobs: false);
        var hooks = new HookRecorder();
        await using var holder = await HoldAsync(E(Ago(11)), "SHARE UPDATE EXCLUSIVE");

        var r = await Runner(o => o.EventsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.Equal(new DeferredPartition("job_events", Ago(11), DeferReason.LockTimeout, 7, null), Assert.Single(r.Deferred));
        Assert.Equal([Ago(10)], r.DroppedEventsPartitions);
        var locksChecked = Assert.Single(hooks.Calls, c => c.Step == RetentionStep.LocksChecked);
        Assert.Equal([Ago(10)], locksChecked.Days);
    }

    [Fact]
    public async Task Lock_timeout_on_parent_defers_group_and_stops_events()
    {
        foreach (var d in new[] { 12, 11, 10 })
            await CreatePartitionsAsync(Ago(d), jobs: false);
        await using var holder = await HoldAsync("job_events", "ACCESS SHARE");

        var r = await Runner(o => o.EventsRetention = Days3, new HookRecorder().Tuning(k: 2)).ApplyAsync(Ct);

        Assert.Equal(new[] { Ago(12), Ago(11) }, r.Deferred.Select(d => d.Day));
        Assert.All(r.Deferred, d => Assert.Equal(DeferReason.LockTimeout, d.Reason));
        Assert.Empty(r.DroppedEventsPartitions);
        Assert.True(await AttachedAsync(E(Ago(10))));   // не тронута и не попала в результат
    }

    [Theory]
    [InlineData("BatchA")]
    [InlineData("BatchB")]
    public async Task Idle_pause_under_lock_is_cut_by_hold_timeout(string pauseAfter)   // RetentionStep — internal
    {
        var step = Enum.Parse<RetentionStep>(pauseAfter);
        await CreatePartitionsAsync(Ago(10), jobs: false);
        await CreatePartitionsAsync(Ago(9), jobs: false);
        var hooks = new HookRecorder
        {
            Then = (c, ct) => c.Step == step ? Task.Delay(TimeSpan.FromSeconds(1.5), ct) : Task.CompletedTask,
        };

        var r = await Runner(o => o.EventsRetention = Days3, hooks.Tuning(k: 1, holdIdle: TimeSpan.FromMilliseconds(300))).ApplyAsync(Ct);

        Assert.Equal(new DeferredPartition("job_events", Ago(10), DeferReason.HoldTimeout, 6, null), Assert.Single(r.Deferred));
        Assert.True(await AttachedAsync(E(Ago(10))));
        Assert.True(await AttachedAsync(E(Ago(9))));   // остановка после HoldTimeout
        await EnqueueAsync();                          // горячий путь работает

        Assert.Equal([Ago(10), Ago(9)], (await Runner(o => o.EventsRetention = Days3).ApplyAsync(Ct)).DroppedEventsPartitions);
    }

    [Fact]
    public async Task Statement_under_lock_is_cut_by_statement_timeout()
    {
        await CreatePartitionsAsync(Ago(10), jobs: false);
        var tuning = new HookRecorder().Tuning(holdStatement: TimeSpan.FromMilliseconds(100), batchBPrefix: "SELECT pg_sleep(2)");

        var r = await Runner(o => o.EventsRetention = Days3, tuning).ApplyAsync(Ct);

        Assert.Equal(DeferReason.HoldTimeout, Assert.Single(r.Deferred).Reason);
        Assert.True(await AttachedAsync(E(Ago(10))));
    }

    [Fact]
    public async Task Cancellation_between_batches_rolls_back_and_throws()
    {
        await CreatePartitionsAsync(Ago(10), jobs: false);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var hooks = new HookRecorder { Then = (c, _) => c.Step == RetentionStep.BatchA ? cts.CancelAsync() : Task.CompletedTask };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Runner(o => o.EventsRetention = Days3, hooks.Tuning()).ApplyAsync(cts.Token));

        Assert.True(await AttachedAsync(E(Ago(10))));
        await EnqueueAsync().WaitAsync(TimeSpan.FromSeconds(2), Ct);   // блокировка родителя снята
        Assert.Equal([Ago(10)], (await Runner(o => o.EventsRetention = Days3).ApplyAsync(Ct)).DroppedEventsPartitions);
    }

    [Fact]
    public async Task Groups_of_k_partitions_share_one_transaction()
    {
        for (var d = 30; d > 5; d--)
            await CreatePartitionsAsync(Ago(d), jobs: false);
        var hooks = new HookRecorder();

        var r = await Runner(o => o.EventsRetention = Days3, hooks.Tuning(k: 10)).ApplyAsync(Ct);

        Assert.Equal(25, r.DroppedEventsPartitions.Count);
        Assert.Equal(3, hooks.Count(RetentionStep.BatchA, "job_events"));
        Assert.Equal(3, hooks.Count(RetentionStep.Committed, "job_events"));
    }

    [Fact]
    public async Task Attached_jobs_partitions_are_counted_after_the_run()
    {
        await CreatePartitionsAsync(Ago(10));
        await CreatePartitionsAsync(Ago(1));

        Assert.Equal(5, (await Runner().ApplyAsync(Ct)).AttachedJobsPartitions);
    }
}

public sealed class RetentionEventsTests_Direct(PostgresFixture pg) : RetentionEventsTests(pg, ConnectionMode.Direct);
public sealed class RetentionEventsTests_Bouncer(PostgresFixture pg) : RetentionEventsTests(pg, ConnectionMode.Bouncer);

/// <summary>
/// Event trigger — глобальное состояние БД: он срабатывает на DROP любого параллельного теста. Поэтому тест вынесен
/// в непараллельную коллекцию "Serial" (определена в HotUpdateTests.cs: DisableParallelization, запуск после всех
/// параллельных коллекций).
/// </summary>
public abstract class RetentionDropFailureTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    private static readonly TimeSpan Days3 = TimeSpan.FromDays(3);

    [Fact]
    public async Task Failed_drop_is_deferred_as_drop_failed_and_retried_as_remnant()
    {
        await CreatePartitionsAsync(Ago(10), jobs: false);
        await CreatePartitionsAsync(Ago(9), jobs: false);
        var trigger = $"evt_{Db.Schema}";
        await Db.ExecAsync($$"""
            CREATE FUNCTION {schema}.forbid_drop() RETURNS event_trigger LANGUAGE plpgsql AS $f$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_event_trigger_dropped_objects()
                           WHERE object_identity = '{schema}.{{E(Ago(10))}}') THEN
                    RAISE EXCEPTION 'drop forbidden by test';
                END IF;
            END $f$;
            CREATE EVENT TRIGGER {{trigger}} ON sql_drop EXECUTE FUNCTION {schema}.forbid_drop();
            """);
        try
        {
            var r = await Runner(o => o.EventsRetention = Days3).ApplyAsync(Ct);

            var failed = Assert.Single(r.Deferred);
            Assert.Equal("job_events", failed.Table);
            Assert.Equal(Ago(10), failed.Day);
            Assert.Equal(DeferReason.DropFailed, failed.Reason);
            Assert.Contains("drop forbidden by test", failed.Error);
            Assert.Equal([Ago(9)], r.DroppedEventsPartitions);
            Assert.False(await AttachedAsync(E(Ago(10))));
        }
        finally
        {
            await Db.ExecAsync($"DROP EVENT TRIGGER IF EXISTS {trigger}");
        }

        Assert.Equal([Ago(10)], (await Runner().ApplyAsync(Ct)).DroppedEventsPartitions);
    }
}

[Collection("Serial")]
public sealed class RetentionDropFailureTests_Direct(PostgresFixture pg) : RetentionDropFailureTests(pg, ConnectionMode.Direct);

[Collection("Serial")]
public sealed class RetentionDropFailureTests_Bouncer(PostgresFixture pg) : RetentionDropFailureTests(pg, ConnectionMode.Bouncer);

/// <summary>
/// Prepared-транзакция — глобальное состояние сервера (держит горизонт xmin всей БД): коллекция "Serial" (HotUpdateTests.cs).
/// </summary>
public abstract class RetentionPreparedTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    [Fact]
    public async Task Prepared_transaction_holding_old_partition_is_deferred_before_lock()
    {
        await CreatePartitionsAsync(Ago(11), jobs: false);
        await CreatePartitionsAsync(Ago(10), jobs: false);
        var gid = $"gid_{Db.Schema}";
        var prepared = false;
        try
        {
            // Блокировки prepared-транзакции в pg_locks — с pid = NULL; сама транзакция создаётся напрямую (Admin).
            await Db.ExecAsync($"BEGIN; LOCK TABLE {{schema}}.{E(Ago(11))} IN ACCESS SHARE MODE; PREPARE TRANSACTION '{gid}'");
            prepared = true;

            var r = await Runner(o => o.EventsRetention = TimeSpan.FromDays(3)).ApplyAsync(Ct);

            Assert.Equal(new DeferredPartition("job_events", Ago(11), DeferReason.LockTimeout, 7, null), Assert.Single(r.Deferred));
            Assert.Equal([Ago(10)], r.DroppedEventsPartitions);
            Assert.True(await AttachedAsync(E(Ago(11))));
        }
        finally
        {
            if (prepared)
                await Db.ExecAsync($"ROLLBACK PREPARED '{gid}'");
        }
    }
}

[Collection("Serial")]
public sealed class RetentionPreparedTests_Direct(PostgresFixture pg) : RetentionPreparedTests(pg, ConnectionMode.Direct);

[Collection("Serial")]
public sealed class RetentionPreparedTests_Bouncer(PostgresFixture pg) : RetentionPreparedTests(pg, ConnectionMode.Bouncer);

/// <summary>Только Direct: пул из одного соединения без сброса — проверяется, что SET LOCAL не остаётся на соединении.</summary>
public sealed class RetentionSessionStateTests(PostgresFixture pg) : RetentionTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Set_local_does_not_leak_to_the_pooled_connection()
    {
        await CreatePartitionsAsync(Ago(10), jobs: false);
        await CreatePartitionsAsync(Ago(9), jobs: false);
        var cs = new NpgsqlConnectionStringBuilder(Pg.DirectConnectionString) { MaxPoolSize = 1, NoResetOnClose = true }.ConnectionString;
        await using var single = NpgsqlDataSource.Create(cs);
        var hooks = new HookRecorder
        {
            // вторая группа откатывается без пакета Б: её партицию отсоединили до транзакции 1
            Then = (c, _) => c.Step == RetentionStep.LocksChecked && c.Days[0] == Ago(9)
                ? DetachAsync("job_events", E(Ago(9)))
                : Task.CompletedTask,
        };

        await Runner(o => o.EventsRetention = TimeSpan.FromDays(3), hooks.Tuning(k: 1), single).ApplyAsync(Ct);

        Assert.Equal(1, hooks.Count(RetentionStep.BatchB, "job_events"));   // пакет Б только у первой группы
        await using var cmd = single.CreateCommand(
            "SELECT current_setting('lock_timeout') || '|' || current_setting('statement_timeout') || '|' || current_setting('idle_in_transaction_session_timeout')");
        Assert.Equal("0|0|0", (string)(await cmd.ExecuteScalarAsync(Ct))!);
    }
}
