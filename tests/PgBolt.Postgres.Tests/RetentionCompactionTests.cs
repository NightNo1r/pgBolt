using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class RetentionCompactionTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    /// <summary>~6.4 КБ случайного hex: несжимаемо, поэтому значение уходит во внешний TOAST (повтор 'x' сжался бы inline).</summary>
    private static readonly string BigArgs =
        $$"""{"blob":"{{string.Concat(Enumerable.Range(0, 200).Select(_ => Guid.NewGuid().ToString("N")))}}"}""";

    private sealed record Pinned(DateOnly Day, Guid[] Live);

    private sealed record StepDay(RetentionStep Step, DateOnly Day);

    private RetentionRunner Compactor(RetentionTuning? tuning = null, int maxLive = 1000, bool events = false) =>
        Runner(o =>
        {
            o.JobsRetention = TimeSpan.FromDays(3);
            o.RetentionCompactionMaxLiveJobs = maxLive;
            if (events)
                o.EventsRetention = TimeSpan.FromDays(3);
        }, tuning);

    /// <summary>
    /// Сутки D = today − ago (по умолчанию 10; OverdueDays = ago − 4 при сроке 3 сут): live живых и terminal терминальных строк.
    /// </summary>
    private async Task<Pinned> PinnedPartitionAsync(int live = 2, int terminal = 150, bool events = false,
        short terminalState = 2, int ago = 10)
    {
        var day = Ago(ago);
        await CreatePartitionsAsync(day, events: events);
        var ids = await EnqueueLiveAsync(day, live);
        if (terminal > 0)
            await InsertTerminalAsync(day, terminal, terminalState);
        return new Pinned(day, ids);
    }

    private async Task<Dictionary<Guid, string>> RowsAsync(string table, Guid[] ids)
    {
        await using var cmd = Db.Admin.CreateCommand(Db.Sql($"SELECT id, row_to_json(j)::text FROM {{schema}}.{table} j WHERE id = ANY(@ids)"));
        cmd.Parameters.AddWithValue("ids", ids);
        await using var r = await cmd.ExecuteReaderAsync(Ct);
        var rows = new Dictionary<Guid, string>();
        while (await r.ReadAsync(Ct))
            rows[r.GetGuid(0)] = r.GetString(1);
        return rows;
    }

    private Task<string?> QueueSnapshotAsync() =>
        Db.ScalarAsync<string?>("SELECT string_agg(row_to_json(q)::text, ',' ORDER BY job_id) FROM {schema}.queue q");

    [Fact]
    public async Task Compaction_keeps_live_rows_and_drops_terminal_ones()
    {
        var pinned = await PinnedPartitionAsync(live: 3);
        var day = pinned.Day;
        var big = await EnqueueLiveAsync(day, 1, args: BigArgs);                      // Review Focus 3: TOAST
        await Store.DequeueAsync("q", 2, ServerA, Lease, Ct);                        // часть живых — в lease
        var all = pinned.Live.Concat(big).ToArray();
        var before = await RowsAsync(J(day), all);
        var oldOid = await OidAsync(J(day));
        var queueBefore = await QueueSnapshotAsync();

        var r = await Compactor().ApplyAsync(Ct);

        Assert.Equal(new PinnedPartition(day, 6, 4, true), Assert.Single(r.Pinned));
        Assert.Empty(r.Deferred);
        Assert.Equal(0, r.DroppedCompactionRemnants);
        Assert.True(await AttachedAsync(J(day)));
        Assert.NotEqual(oldOid, await OidAsync(J(day)));
        Assert.Equal(4L, await CountAsync(J(day)));
        Assert.Equal(before, await RowsAsync(J(day), all));
        Assert.True(await Db.ScalarAsync<bool>("SELECT length(args::text) > 5000 FROM {schema}.jobs WHERE id = @id", ("id", big[0])));
        Assert.Contains("fillfactor=80",
            await Db.ScalarAsync<string[]>("SELECT reloptions FROM pg_class WHERE oid = to_regclass(@t)", ("t", $"{Db.Schema}.{J(day)}")));
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM pg_index WHERE indrelid = to_regclass(@t) AND indisprimary",
            ("t", $"{Db.Schema}.{J(day)}")));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM pg_constraint WHERE conrelid = to_regclass(@t) AND contype = 'c'",
            ("t", $"{Db.Schema}.{J(day)}")));
        Assert.False(await ExistsAsync($"{J(day)}_r{oldOid}"));
        Assert.Equal(queueBefore, await QueueSnapshotAsync());
    }

    [Fact]
    public async Task Compacted_partition_is_dropped_after_its_live_jobs_finish()
    {
        var day = (await PinnedPartitionAsync()).Day;
        Assert.True(Assert.Single((await Compactor().ApplyAsync(Ct)).Pinned).Compacted);

        var leased = await Store.DequeueAsync("q", 2, ServerA, Lease, Ct);
        Assert.Equal(2, (await Store.AckAsync(ServerA, [.. leased.Select(j => new LeaseRef(j.Id, j.LeaseId))], Ct)).Count);

        Assert.Equal([day], (await Compactor().ApplyAsync(Ct)).DroppedJobsPartitions);
    }

    [Fact]
    public async Task Leased_jobs_finish_normally_after_compaction()
    {
        var day = (await PinnedPartitionAsync(live: 6, events: true)).Day;
        var jobs = (await Store.DequeueAsync("q", 6, ServerA, Lease, Ct)).OrderBy(j => j.Id).ToArray();
        Assert.Equal(6, jobs.Length);
        await Store.CancelAsync(jobs[5].Id, "stop", Ct);   // cancel_requested у взятой задачи

        Assert.True(Assert.Single((await Compactor().ApplyAsync(Ct)).Pinned).Compacted);

        Assert.Equal(6, (await Store.HeartbeatAsync(ServerA, [.. jobs.Select(j => new LeaseRef(j.Id, j.LeaseId))], Lease, Ct)).Leases.Count);
        Assert.Single(await Store.AckAsync(ServerA, [new LeaseRef(jobs[0].Id, jobs[0].LeaseId)], Ct));
        Assert.Equal(FailOutcome.Retried, await Store.RetryAsync(jobs[1].Id, jobs[1].LeaseId, ServerA, TimeSpan.Zero, JobEventKind.Retry, "m", null, Ct));
        Assert.Equal(FailOutcome.Failed, await Store.FailAsync(jobs[2].Id, jobs[2].LeaseId, ServerA, JobEventKind.Failed, "m", null, Ct));
        Assert.Equal(ReleaseOutcome.Released, await Store.ReleaseAsync(jobs[3].Id, jobs[3].LeaseId, ServerA, ran: true, null, Ct));
        Assert.Equal(ReleaseOutcome.Released, await Store.ReleaseAsync(jobs[4].Id, jobs[4].LeaseId, ServerA, ran: false, null, Ct));
        Assert.Equal(ReleaseOutcome.Cancelled, await Store.ReleaseAsync(jobs[5].Id, jobs[5].LeaseId, ServerA, ran: true, null, Ct));

        Assert.Equal(new short?[] { 2, 0, 3, 0, 0, 4 }, await Task.WhenAll(jobs.Select(j => JobStateAsync(j.Id))));
        Assert.Equal(6L, await CountAsync(J(day)));   // итоги записаны в новую партицию тех же суток
    }

    [Fact]
    public async Task Live_threshold_decides_between_compaction_and_deferral()
    {
        await CreatePartitionsAsync(Ago(11), events: false);
        await EnqueueLiveAsync(Ago(11), 5);
        await InsertTerminalAsync(Ago(11), 150);
        await CreatePartitionsAsync(Ago(10), events: false);
        await EnqueueLiveAsync(Ago(10), 6);
        await InsertTerminalAsync(Ago(10), 150);
        var oid10 = await OidAsync(J(Ago(10)));

        var r = await Compactor(maxLive: 5).ApplyAsync(Ct);

        Assert.Equal(new PinnedPartition(Ago(11), 7, 5, true), Assert.Single(r.Pinned));
        Assert.Equal(new DeferredPartition("jobs", Ago(10), DeferReason.LiveJobs, 6, 6), Assert.Single(r.Deferred));
        Assert.Equal(oid10, await OidAsync(J(Ago(10))));
    }

    [Fact]
    public async Task Zero_threshold_disables_compaction()
    {
        var day = (await PinnedPartitionAsync(live: 1)).Day;

        var r = await Compactor(maxLive: 0).ApplyAsync(Ct);

        Assert.Empty(r.Pinned);
        Assert.Equal(new DeferredPartition("jobs", day, DeferReason.LiveJobs, 6, 1), Assert.Single(r.Deferred));
    }

    [Theory]
    [InlineData(50, 99, false)]
    [InlineData(50, 100, true)]
    [InlineData(300, 299, false)]
    [InlineData(300, 300, true)]
    [InlineData(3, 0, false)]
    public async Task Compaction_needs_at_least_max_of_live_and_100_terminal_rows(int live, int terminal, bool compacted)
    {
        var day = (await PinnedPartitionAsync(live, terminal)).Day;
        var hooks = new HookRecorder();

        var r = await Compactor(hooks.Tuning()).ApplyAsync(Ct);

        Assert.Equal(new PinnedPartition(day, 6, live, compacted), Assert.Single(r.Pinned));
        Assert.Equal(compacted ? 1 : 0, hooks.Count(RetentionStep.BatchA, "jobs"));   // без сжатия LOCK не берётся
    }

    [Fact]
    public async Task Compacted_partition_is_left_alone_without_new_terminal_rows()
    {
        var day = (await PinnedPartitionAsync()).Day;
        await Compactor().ApplyAsync(Ct);
        var oid = await OidAsync(J(day));

        var r = await Compactor().ApplyAsync(Ct);

        Assert.Equal(new PinnedPartition(day, 6, 2, false), Assert.Single(r.Pinned));
        Assert.Equal(oid, await OidAsync(J(day)));
    }

    [Fact]
    public async Task Each_compaction_has_its_own_locks_check_and_warm_up()
    {
        foreach (var d in new[] { 12, 11, 10 })
        {
            await CreatePartitionsAsync(Ago(d), events: false);
            await EnqueueLiveAsync(Ago(d), 2);
            await InsertTerminalAsync(Ago(d), 150);
        }
        var hooks = new HookRecorder();

        await Compactor(hooks.Tuning()).ApplyAsync(Ct);

        var steps = hooks.Calls
            .Where(c => c.Parent == "jobs" && c.Step is RetentionStep.LocksChecked or RetentionStep.WarmedUp or RetentionStep.BatchA)
            .Select(c => new StepDay(c.Step, c.Days[0]))
            .ToArray();
        Assert.Equal(
            new[] { 12, 11, 10 }.SelectMany(d => new[]
            {
                new StepDay(RetentionStep.LocksChecked, Ago(d)), new StepDay(RetentionStep.WarmedUp, Ago(d)),
                new StepDay(RetentionStep.BatchA, Ago(d)),
            }),
            steps);
    }

    [Fact]
    public async Task Warm_up_reads_heap_not_index_only()
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var ids = await EnqueueLiveAsync(day, 20, args: BigArgs);
        await InsertTerminalAsync(day, 500);
        await Db.ExecAsync($"VACUUM ANALYZE {{schema}}.{J(day)}");   // visibility map: Index Only Scan стал бы возможен
        var sql = Compactor().WarmUpSql(J(day)).Replace("$1", $"ARRAY[{string.Join(",", ids.Select(i => $"'{i}'"))}]::uuid[]");
        await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using (var off = new NpgsqlCommand("SET enable_seqscan = off", conn))
            await off.ExecuteNonQueryAsync(Ct);
        await using var explain = new NpgsqlCommand("EXPLAIN " + sql, conn);
        var lines = new List<string>();
        await using (var r = await explain.ExecuteReaderAsync(Ct))
            while (await r.ReadAsync(Ct))
                lines.Add(r.GetString(0));
        var plan = string.Join('\n', lines);

        Assert.DoesNotContain("Index Only Scan", plan);
        Assert.True(plan.Contains("Index Scan") || plan.Contains("Bitmap Heap Scan"), plan);
    }

    [Fact]
    public async Task Partition_detached_by_someone_else_is_not_compacted()
    {
        var day = (await PinnedPartitionAsync()).Day;
        var hooks = new HookRecorder
        {
            Then = (c, _) => c.Step == RetentionStep.WarmedUp ? DetachAsync("jobs", J(day)) : Task.CompletedTask,
        };

        var r = await Compactor(hooks.Tuning()).ApplyAsync(Ct);

        Assert.Equal(0, hooks.Count(RetentionStep.BatchB, "jobs"));
        Assert.Equal([day], r.DroppedJobsPartitions);   // отсоединённая с тем же oid — в транзакцию 2
        Assert.Empty(r.Pinned);
    }

    [Fact]
    public async Task Live_jobs_above_threshold_found_under_lock_roll_back()
    {
        var day = (await PinnedPartitionAsync(live: 5)).Day;
        var oid = await OidAsync(J(day));
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step == RetentionStep.WarmedUp)
                    await EnqueueLiveAsync(day, 2);
            },
        };

        var r = await Compactor(hooks.Tuning(), maxLive: 5).ApplyAsync(Ct);

        Assert.Equal(0, hooks.Count(RetentionStep.BatchB, "jobs"));
        Assert.Equal(new DeferredPartition("jobs", day, DeferReason.LiveJobs, 6, 6), Assert.Single(r.Deferred));
        Assert.Equal(oid, await OidAsync(J(day)));
    }

    [Fact]
    public async Task Job_requeued_before_lock_is_moved_with_live_rows()
    {
        var day = (await PinnedPartitionAsync(terminal: 0)).Day;
        var failed = await InsertTerminalAsync(day, 150, state: 3);
        var hooks = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                if (c.Step == RetentionStep.WarmedUp)
                    Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(failed[0], ct)).Result);
            },
        };

        var r = await Compactor(hooks.Tuning()).ApplyAsync(Ct);

        Assert.Equal(new PinnedPartition(day, 6, 2, true), Assert.Single(r.Pinned));   // §5.1: LiveJobs — по предпроверке
        Assert.Equal(3L, await CountAsync(J(day)));                                    // перенесены живые под блокировкой
        Assert.True(await InQueueAsync(failed[0]));
        Assert.Equal((short)0, await JobStateAsync(failed[0]));
    }

    [Fact]
    public async Task Interrupted_compaction_leaves_remnant_that_next_run_drops()
    {
        var day = (await PinnedPartitionAsync()).Day;
        var oid = await OidAsync(J(day));
        var hooks = new HookRecorder
        {
            Then = (c, _) => c.Step == RetentionStep.Committed ? throw new InvalidOperationException("pod died") : Task.CompletedTask,
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => Compactor(hooks.Tuning()).ApplyAsync(Ct));

        Assert.True(await AttachedAsync(J(day)));
        Assert.True(await ExistsAsync($"{J(day)}_r{oid}"));
        var r = await Runner().ApplyAsync(Ct);   // оба срока null
        Assert.Equal(1, r.DroppedCompactionRemnants);
        Assert.False(await ExistsAsync($"{J(day)}_r{oid}"));
        Assert.Equal(2L, await CountAsync(J(day)));
    }

    [Fact]
    public async Task Locked_remnant_of_this_run_is_pinned_and_deferred()
    {
        var day = (await PinnedPartitionAsync()).Day;
        var oid = await OidAsync(J(day));
        NpgsqlConnection? holder = null;
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step == RetentionStep.Committed)
                    holder = await HoldAsync($"{J(day)}_r{oid}", "ACCESS SHARE");
            },
        };
        try
        {
            var r = await Compactor(hooks.Tuning()).ApplyAsync(Ct);

            Assert.Equal(new PinnedPartition(day, 6, 2, true), Assert.Single(r.Pinned));
            Assert.Equal(new DeferredPartition("jobs", day, DeferReason.DropLockTimeout, 6, null), Assert.Single(r.Deferred));
            Assert.Equal(0, r.DroppedCompactionRemnants);
        }
        finally
        {
            if (holder is not null)
                await holder.DisposeAsync();
        }

        Assert.Equal(1, (await Runner().ApplyAsync(Ct)).DroppedCompactionRemnants);
    }

    [Fact]
    public async Task Stale_listing_never_drops_a_recompacted_partition()
    {
        var day = (await PinnedPartitionAsync()).Day;
        var hooks = new HookRecorder
        {
            // другой под сжимает партицию между предпроверкой и транзакцией 1 этого запуска
            Then = async (c, ct) =>
            {
                if (c.Step == RetentionStep.Prechecked)
                    Assert.True(Assert.Single((await Compactor().ApplyAsync(ct)).Pinned).Compacted);
            },
        };

        var r = await Compactor(hooks.Tuning()).ApplyAsync(Ct);

        Assert.Empty(r.Pinned);
        Assert.Empty(r.DroppedJobsPartitions);
        Assert.True(await AttachedAsync(J(day)));
        Assert.Equal(2L, await CountAsync(J(day)));
    }

    [Fact]
    public async Task Remnant_replaced_by_attached_table_with_same_name_is_not_dropped()
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        await DetachAsync("jobs", J(day));   // остаток отсоединения
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step != RetentionStep.Listed)
                    return;
                await Db.ExecAsync($"ALTER TABLE {{schema}}.{J(day)} RENAME TO old_detached");
                await CreatePartitionsAsync(day, events: false);
            },
        };

        var r = await Runner(tuning: hooks.Tuning()).ApplyAsync(Ct);

        Assert.Empty(r.DroppedJobsPartitions);
        Assert.True(await AttachedAsync(J(day)));
    }

    [Fact]
    public async Task Holder_appearing_after_warm_up_rolls_back_compaction()
    {
        var day = (await PinnedPartitionAsync()).Day;
        var oid = await OidAsync(J(day));
        NpgsqlConnection? holder = null;
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step == RetentionStep.WarmedUp)
                    holder = await HoldAsync(J(day), "SHARE UPDATE EXCLUSIVE");
            },
        };
        try
        {
            var r = await Compactor(hooks.Tuning()).ApplyAsync(Ct);

            Assert.Equal(new DeferredPartition("jobs", day, DeferReason.LockTimeout, 6, null), Assert.Single(r.Deferred));
            Assert.Equal(oid, await OidAsync(J(day)));
            Assert.Equal(152L, await CountAsync(J(day)));
        }
        finally
        {
            if (holder is not null)
                await holder.DisposeAsync();
        }
    }

    [Fact]
    public async Task Unexpected_ddl_error_rolls_back_and_throws()
    {
        var day = (await PinnedPartitionAsync()).Day;
        var oid = await OidAsync(J(day));
        var hooks = new HookRecorder
        {
            Then = (c, _) => c.Step == RetentionStep.WarmedUp
                ? Db.ExecAsync($"CREATE TABLE {{schema}}.{J(day)}_r{oid} (id int)")
                : Task.CompletedTask,
        };

        var e = await Assert.ThrowsAsync<PostgresException>(() => Compactor(hooks.Tuning()).ApplyAsync(Ct));

        Assert.Equal(PostgresErrorCodes.DuplicateTable, e.SqlState);
        Assert.Equal(oid, await OidAsync(J(day)));
        Assert.True(await AttachedAsync(J(day)));
        Assert.Equal(152L, await CountAsync(J(day)));
    }

    [Theory]
    [InlineData("BatchA")]
    [InlineData("BatchB")]
    public async Task Hold_timeout_during_compaction_leaves_partition_intact(string pauseAfter)   // RetentionStep — internal
    {
        var step = Enum.Parse<RetentionStep>(pauseAfter);
        // §5.3 п. 2: отсоединения, затем компактирования от старых к новым — first обрабатывается раньше second.
        var first = (await PinnedPartitionAsync(events: true)).Day;            // Ago(10), OverdueDays = 6
        var second = (await PinnedPartitionAsync(events: true, ago: 9)).Day;   // Ago(9), OverdueDays = 5
        var firstOid = await OidAsync(J(first));
        var secondOid = await OidAsync(J(second));
        var hooks = new HookRecorder
        {
            // только этап jobs: пауза в job_events дала бы HoldTimeout и там
            Then = (c, ct) => c.Parent == "jobs" && c.Step == step ? Task.Delay(TimeSpan.FromSeconds(1.5), ct) : Task.CompletedTask,
        };

        var r = await Compactor(hooks.Tuning(holdIdle: TimeSpan.FromMilliseconds(300)), events: true).ApplyAsync(Ct);

        Assert.Equal(new DeferredPartition("jobs", first, DeferReason.HoldTimeout, 6, null), Assert.Single(r.Deferred));
        Assert.Empty(r.Pinned);                                   // second не обрабатывался: остановка этапа jobs (п. 2)
        Assert.Empty(r.DroppedJobsPartitions);
        Assert.Equal([first, second], r.DroppedEventsPartitions); // этап job_events выполняется после остановки jobs
        Assert.Equal(firstOid, await OidAsync(J(first)));
        Assert.Equal(secondOid, await OidAsync(J(second)));
        Assert.Equal(1, hooks.Count(step, "jobs"));               // транзакция 1 только у first

        Assert.Equal(2, (await Compactor().ApplyAsync(Ct)).Pinned.Count(p => p.Compacted));
    }

    [Fact]
    public async Task Compaction_follows_parent_column_changes()
    {
        var pinned = await PinnedPartitionAsync(terminal: 0);
        var day = pinned.Day;
        var live = pinned.Live;
        await Db.ExecAsync("ALTER TABLE {schema}.jobs ADD COLUMN extra text");
        await Db.ExecAsync("UPDATE {schema}.jobs SET extra = 'kept' WHERE id = ANY(@ids)", ("ids", live));
        await InsertTerminalAsync(day, 150);
        Assert.True(Assert.Single((await Compactor().ApplyAsync(Ct)).Pinned).Compacted);
        Assert.Equal(2L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE extra = 'kept'"));

        await Db.ExecAsync("ALTER TABLE {schema}.jobs DROP COLUMN extra");   // у сжатой партиции — удалённый слот
        var day2 = Ago(9);
        await CreatePartitionsAsync(day2, events: false);                      // создана после DROP COLUMN: без слота
        var live2 = await EnqueueLiveAsync(day2, 2);
        await InsertTerminalAsync(day2, 150);
        await InsertTerminalAsync(day, 150);
        var before = (await RowsAsync(J(day), live)).Concat(await RowsAsync(J(day2), live2)).ToDictionary();

        var r = await Compactor().ApplyAsync(Ct);

        Assert.Equal(2, r.Pinned.Count(p => p.Compacted));
        Assert.Equal(before, (await RowsAsync(J(day), live)).Concat(await RowsAsync(J(day2), live2)).ToDictionary());
        Assert.Equal(0L, await Db.ScalarAsync<long>(
            "SELECT count(*) FROM pg_attribute WHERE attrelid = to_regclass(@t) AND attnum > 0 AND attgenerated <> ''",
            ("t", $"{Db.Schema}.jobs")));
        await using var cmd = Db.Admin.CreateCommand(Db.Sql("""
            SELECT count(DISTINCT cols) FROM (
                SELECT c.oid, (SELECT string_agg(attname, ',' ORDER BY attnum) FROM pg_attribute
                               WHERE attrelid = c.oid AND attnum > 0 AND NOT attisdropped) AS cols
                FROM pg_class c
                WHERE c.oid = to_regclass('{schema}.jobs')
                   OR c.oid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent = to_regclass('{schema}.jobs'))
            ) x
            """));
        Assert.Equal(1L, (long)(await cmd.ExecuteScalarAsync(Ct))!);   // контракт §5.3 «SELECT *»
    }
}

public sealed class RetentionCompactionTests_Direct(PostgresFixture pg) : RetentionCompactionTests(pg, ConnectionMode.Direct);
public sealed class RetentionCompactionTests_Bouncer(PostgresFixture pg) : RetentionCompactionTests(pg, ConnectionMode.Bouncer);

public abstract class RetentionPublicationTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    [Theory]
    [InlineData("FOR TABLE {schema}.jobs")]
    [InlineData("FOR ALL TABLES")]
    public async Task Publication_skips_compaction_but_not_detach(string target)
    {
        var pinned = Ago(11);
        var empty = Ago(10);
        await CreatePartitionsAsync(pinned);
        await EnqueueLiveAsync(pinned, 3);
        await InsertTerminalAsync(pinned, 150);
        await CreatePartitionsAsync(empty);
        await InsertTerminalAsync(empty, 5);
        var oid = await OidAsync(J(pinned));
        var publication = $"pub_{Db.Schema}";
        await Db.ExecAsync($"CREATE PUBLICATION {publication} {target}");
        try
        {
            var r = await Runner(o => { o.JobsRetention = TimeSpan.FromDays(3); o.EventsRetention = TimeSpan.FromDays(3); }).ApplyAsync(Ct);

            Assert.Equal(new DeferredPartition("jobs", pinned, DeferReason.Replicated, 7, 3), Assert.Single(r.Deferred));
            Assert.Empty(r.Pinned);
            Assert.Equal([empty], r.DroppedJobsPartitions);
            Assert.Equal([pinned, empty], r.DroppedEventsPartitions);
            Assert.Equal(oid, await OidAsync(J(pinned)));
        }
        finally
        {
            await Db.ExecAsync($"DROP PUBLICATION IF EXISTS {publication}");
        }

        Assert.True(Assert.Single((await Runner(o => o.JobsRetention = TimeSpan.FromDays(3)).ApplyAsync(Ct)).Pinned).Compacted);
    }
}

[Collection("Serial")]   // публикация FOR ALL TABLES — состояние всей БД; коллекция из HotUpdateTests.cs
public sealed class RetentionPublicationTests_Direct(PostgresFixture pg) : RetentionPublicationTests(pg, ConnectionMode.Direct);

[Collection("Serial")]
public sealed class RetentionPublicationTests_Bouncer(PostgresFixture pg) : RetentionPublicationTests(pg, ConnectionMode.Bouncer);
