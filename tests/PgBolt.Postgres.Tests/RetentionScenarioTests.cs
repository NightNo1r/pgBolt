using System.Collections.Concurrent;
using System.Data;
using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class RetentionScenarioTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    private static readonly TimeSpan Days3 = TimeSpan.FromDays(3);

    private async Task PinnedAsync(DateOnly day, int live = 2, int terminal = 150, string queue = "q")
    {
        await CreatePartitionsAsync(day);
        await EnqueueLiveAsync(day, live, queue);
        await InsertTerminalAsync(day, terminal);
    }

    private Task<long> OrphansAsync() => Db.ScalarAsync<long>("""
        SELECT (SELECT count(*) FROM {schema}.queue q WHERE NOT EXISTS (SELECT 1 FROM {schema}.jobs j WHERE j.id = q.job_id))
             + (SELECT count(*) FROM {schema}.jobs j WHERE j.state = 0 AND NOT EXISTS (SELECT 1 FROM {schema}.queue q WHERE q.job_id = j.id))
        """);

    [Fact]
    public async Task Parallel_runs_process_every_partition_exactly_once()
    {
        for (var d = 20; d > 10; d--)
        {
            await CreatePartitionsAsync(Ago(d));
            await InsertTerminalAsync(Ago(d), 5);
        }
        for (var d = 10; d > 7; d--)
            await PinnedAsync(Ago(d));

        RetentionRunner Run() => Runner(o => { o.JobsRetention = Days3; o.RetentionLockTimeout = TimeSpan.FromSeconds(2); });

        // Перекрытие: предпроверка и прогрев одного запуска держат партицию в обход родителя, и предпроверка pg_locks
        // другого может отложить её (LockTimeout) — это штатно; третий, последовательный запуск доделывает отложенное.
        // Отсоединённую одним запуском таблицу другой может увидеть остатком и тоже дойти до DROP: его DROP после ожидания
        // блокировки получает 42P01 и даёт Skipped (Task 6), поэтому сутки не учитываются дважды.
        var parallel = await Task.WhenAll(Run().ApplyAsync(Ct), Run().ApplyAsync(Ct));
        var runs = parallel.Append(await Run().ApplyAsync(Ct)).ToArray();

        Assert.All(runs.SelectMany(r => r.Deferred), d => Assert.Equal(DeferReason.LockTimeout, d.Reason));
        var dropped = runs.SelectMany(r => r.DroppedJobsPartitions).ToList();
        Assert.Equal(10, dropped.Count);                 // каждая партиция удалена ровно один раз
        Assert.Equal(10, dropped.Distinct().Count());
        var compacted = runs.SelectMany(r => r.Pinned).Where(p => p.Compacted).Select(p => p.Day).ToList();
        Assert.Equal(3, compacted.Count);                // каждая закреплённая сжата ровно один раз
        Assert.Equal(3, compacted.Distinct().Count());
        for (var d = 10; d > 7; d--)
        {
            Assert.True(await AttachedAsync(J(Ago(d))));
            Assert.Equal(2L, await Db.ScalarAsync<long>($"SELECT count(*) FROM {{schema}}.{J(Ago(d))} WHERE state = 0"));
        }
        Assert.Equal(6L, await CountAsync("queue"));
        Assert.Equal(0L, await OrphansAsync());
    }

    [Theory]
    [InlineData("Pacific/Kiritimati")]
    [InlineData("Etc/GMT+12")]
    public async Task Session_time_zone_does_not_shift_days(string zone)
    {
        await CreatePartitionsAsync(Ago(3));
        await CreatePartitionsAsync(Ago(2));
        var cs = new NpgsqlConnectionStringBuilder(Db.ConnectionString) { Timezone = zone }.ConnectionString;
        await using var zoned = NpgsqlDataSource.Create(cs);
        await using (var check = zoned.CreateCommand("SELECT current_setting('TimeZone')"))
            Assert.Equal(zone, (string)(await check.ExecuteScalarAsync(Ct))!);

        var r = await Runner(o => { o.JobsRetention = TimeSpan.FromDays(2); o.EventsRetention = TimeSpan.FromDays(2); },
            dataSource: zoned).ApplyAsync(Ct);   // Rd = 2 → horizon = today − 3 (UTC)

        Assert.Equal([Ago(3)], r.DroppedJobsPartitions);
        Assert.Equal([Ago(3)], r.DroppedEventsPartitions);
        Assert.True(await AttachedAsync(J(Ago(2))));
        Assert.True(await AttachedAsync(E(Ago(2))));
    }

    [Fact]
    public async Task Hot_path_keeps_working_during_compactions()
    {
        foreach (var d in new[] { 12, 11, 10 })
            await PinnedAsync(Ago(d), live: 20, queue: "old");
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var errors = new ConcurrentQueue<Exception>();

        async Task LoopAsync(Func<CancellationToken, Task> body)
        {
            while (!stop.IsCancellationRequested)
            {
                try
                {
                    await body(stop.Token);
                }
                catch (Exception) when (stop.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception e)
                {
                    errors.Enqueue(e);
                }
            }
        }

        // Успешные итерации каждого воркера и итерации dequeue-воркеров, реально взявшие задачи.
        var enqueued = 0;
        var ackIterations = 0;
        var acked = 0;
        var releaseIterations = 0;
        var released = 0;
        var workers = new[]
        {
            LoopAsync(async _ =>
            {
                await EnqueueAsync("new");
                Interlocked.Increment(ref enqueued);
            }),
            LoopAsync(async ct =>
            {
                var leased = await Store.DequeueAsync("new", 5, ServerA, Lease, ct);
                if (leased.Count > 0)
                {
                    await Store.AckAsync(ServerA, [.. leased.Select(j => new LeaseRef(j.Id, j.LeaseId))], ct);
                    Interlocked.Increment(ref acked);
                }
                Interlocked.Increment(ref ackIterations);
            }),
            LoopAsync(async ct =>
            {
                var leased = await Store.DequeueAsync("old", 2, ServerB, Lease, ct);
                foreach (var j in leased)
                {
                    await Store.HeartbeatAsync(ServerB, [new LeaseRef(j.Id, j.LeaseId)], Lease, ct);
                    await Store.ReleaseAsync(j.Id, j.LeaseId, ServerB, ran: false, null, ct);
                }
                if (leased.Count > 0)
                    Interlocked.Increment(ref released);
                Interlocked.Increment(ref releaseIterations);
            }),
        };

        var compactions = 0;
        for (var i = 0; i < 6; i++)
        {
            compactions += (await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct)).Pinned.Count(p => p.Compacted);
            await InsertTerminalAsync(Ago(10 + i % 3), 150);
        }
        await stop.CancelAsync();
        await Task.WhenAll(workers);

        Assert.Empty(errors);
        Assert.True(enqueued > 0, "enqueue worker made no successful iterations");
        Assert.True(ackIterations > 0, "'new' dequeue worker made no successful iterations");
        Assert.True(acked > 0, "'new' dequeue worker never leased a job");
        Assert.True(releaseIterations > 0, "'old' dequeue worker made no successful iterations");
        Assert.True(released > 0, "'old' dequeue worker never leased a job");
        Assert.True(compactions >= 3, $"compactions: {compactions}");
        Assert.Equal(0L, await OrphansAsync());
        Assert.Equal(60L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.queue WHERE queue = 'old'"));
    }

    [Theory]
    [InlineData(DequeueTests.QueueKind.Plain)]
    [InlineData(DequeueTests.QueueKind.Limited)]
    public async Task ReadAcrossPartitionSwap(DequeueTests.QueueKind kind)
    {
        var day = Ago(10);
        await PinnedAsync(day, live: 1);
        var parent = await OidAsync("jobs");
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hooks = new HookRecorder
        {
            Then = async (step, token) =>
            {
                if (step is { Parent: "jobs", Step: RetentionStep.BatchB })
                {
                    ready.TrySetResult();
                    await release.Task.WaitAsync(token);
                }
            },
        };
        var compaction = Runner(o => o.JobsRetention = Days3, hooks.Tuning(holdIdle: TimeSpan.FromSeconds(15))).ApplyAsync(Ct);
        async Task<IReadOnlyList<LeasedJob>> TakeAsync() => kind == DequeueTests.QueueKind.Plain
            ? await Store.DequeueAsync("q", 1, ServerA, Lease, Ct)
            : (await Store.DequeueLimitedAsync("q", 1, ServerA, Lease, Ct)).Jobs;
        Task<IReadOnlyList<LeasedJob>> dequeue;
        try
        {
            await ready.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
            dequeue = TakeAsync();
            await WaitForLockWaitersAsync(parent, 1);
        }
        finally
        {
            release.TrySetResult();
            Assert.True(Assert.Single((await compaction).Pinned).Compacted);
        }
        Assert.Single(await dequeue);
    }

    [Fact]
    public async Task Repeatable_read_snapshot_taken_before_compaction_misses_moved_rows()   // документирует §5.3 «MVCC»
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day);
        var live = await EnqueueLiveAsync(day, 2);
        await InsertTerminalAsync(day, 150);
        await using var rr = await Db.Admin.OpenConnectionAsync(Ct);
        await using var tx = await rr.BeginTransactionAsync(IsolationLevel.RepeatableRead, Ct);
        await using (var snapshot = new NpgsqlCommand(Db.Sql("SELECT count(*) FROM {schema}.queue"), rr, tx))
            await snapshot.ExecuteScalarAsync(Ct);

        Assert.True(Assert.Single((await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct)).Pinned).Compacted);

        await using (var read = new NpgsqlCommand(Db.Sql("SELECT count(*) FROM {schema}.jobs WHERE id = ANY(@ids)"), rr, tx))
        {
            read.Parameters.AddWithValue("ids", live);
            Assert.Equal(0L, (long)(await read.ExecuteScalarAsync(Ct))!);
        }
        Assert.Equal(2L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE id = ANY(@ids)", ("ids", live)));
    }

    [Fact]
    public async Task Works_with_multiplexing_data_source()
    {
        await PinnedAsync(Ago(11));
        await CreatePartitionsAsync(Ago(10));
        await InsertTerminalAsync(Ago(10), 3);
        var cs = new NpgsqlConnectionStringBuilder(Db.ConnectionString) { Multiplexing = true }.ConnectionString;
        await using var mux = NpgsqlDataSource.Create(cs);

        var r = await Runner(o => { o.JobsRetention = Days3; o.EventsRetention = Days3; }, dataSource: mux).ApplyAsync(Ct);

        Assert.True(Assert.Single(r.Pinned).Compacted);
        Assert.Equal([Ago(10)], r.DroppedJobsPartitions);
        Assert.Equal([Ago(11), Ago(10)], r.DroppedEventsPartitions);
    }

    [Fact]
    public async Task Concurrent_partition_creation_neither_deadlocks_nor_fails()   // Review Focus 2
    {
        await PinnedAsync(Ago(12));
        for (var d = 11; d > 5; d--)
        {
            await CreatePartitionsAsync(Ago(d));
            await InsertTerminalAsync(Ago(d), 3);
        }
        var manager = new PartitionManager(Db.DataSource, Db.Schema, 80, TimeSpan.FromSeconds(2));
        var ensure = Task.Run(async () =>
        {
            for (var days = 3; days <= 14; days++)
                await manager.EnsureAsync(days, Ct);
        }, Ct);
        RetentionRunner Run() => Runner(o => { o.JobsRetention = Days3; o.EventsRetention = Days3; });
        var retention = Run().ApplyAsync(Ct);

        await Task.WhenAll(ensure, retention).WaitAsync(TimeSpan.FromSeconds(60), Ct);

        // Второй, последовательный запуск с теми же сроками доделывает отложенное первым (LockTimeout); по двум запускам
        // каждые сутки обработаны ровно один раз — значит, первый запуск не просто «не упал», а работал вместе с EnsureAsync.
        var runs = new[] { await retention, await Run().ApplyAsync(Ct) };
        Assert.All(runs.SelectMany(r => r.Deferred), d => Assert.Equal(DeferReason.LockTimeout, d.Reason));
        Assert.Equal([Ago(11), Ago(10), Ago(9), Ago(8), Ago(7), Ago(6)],
            runs.SelectMany(r => r.DroppedJobsPartitions).Order());
        Assert.Equal([Ago(12), Ago(11), Ago(10), Ago(9), Ago(8), Ago(7), Ago(6)],
            runs.SelectMany(r => r.DroppedEventsPartitions).Order());
        Assert.Equal([Ago(12)], runs.SelectMany(r => r.Pinned).Where(p => p.Compacted).Select(p => p.Day));
        Assert.True(await AttachedAsync(J(Ago(12))));
        Assert.Equal(2L, await Db.ScalarAsync<long>($"SELECT count(*) FROM {{schema}}.{J(Ago(12))} WHERE state = 0"));
        Assert.True(await AttachedAsync(J(Today.AddDays(14))));
        Assert.Equal(0L, await OrphansAsync());
    }
}

public sealed class RetentionScenarioTests_Direct(PostgresFixture pg) : RetentionScenarioTests(pg, ConnectionMode.Direct);
public sealed class RetentionScenarioTests_Bouncer(PostgresFixture pg) : RetentionScenarioTests(pg, ConnectionMode.Bouncer);

/// <summary>Только Direct: верхняя граница порога компактирования укладывается в HoldStatementTimeout (§5.3 «Порог»).</summary>
public sealed class RetentionCapacityTests(PostgresFixture pg) : RetentionTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Compaction_of_ten_thousand_live_jobs_fits_hold_timeout()   // Review Focus 5
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        await EnqueueLiveAsync(day, 10_000, args: $$"""{"blob":"{{new string('y', 1000)}}"}""");
        await InsertTerminalAsync(day, 10_000);

        var r = await Runner(o => { o.JobsRetention = TimeSpan.FromDays(3); o.RetentionCompactionMaxLiveJobs = 10_000; }).ApplyAsync(Ct);

        Assert.Equal(new PinnedPartition(day, 6, 10_000, true), Assert.Single(r.Pinned));
        Assert.Empty(r.Deferred);
    }
}
