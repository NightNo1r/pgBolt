using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class RetentionJobsTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    private static readonly TimeSpan Days3 = TimeSpan.FromDays(3);

    private async Task TerminalPartitionAsync(DateOnly day, int rows = 3, bool events = false)
    {
        await CreatePartitionsAsync(day, events: events);
        await InsertTerminalAsync(day, rows);
    }

    [Fact]
    public async Task Jobs_candidates_follow_day_boundary()
    {
        foreach (var d in new[] { 5, 4, 3 })
            await TerminalPartitionAsync(Ago(d));

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal([Ago(5), Ago(4)], r.DroppedJobsPartitions);
        Assert.True(await AttachedAsync(J(Ago(3))));
        Assert.True(await AttachedAsync(J(Today)));
        Assert.Empty(r.Pinned);
        Assert.Empty(r.Deferred);
        Assert.Equal(4, r.AttachedJobsPartitions);
    }

    [Fact]
    public async Task Null_jobs_retention_keeps_partitions()
    {
        await TerminalPartitionAsync(Ago(30));

        Assert.Empty((await Runner(o => o.EventsRetention = Days3).ApplyAsync(Ct)).DroppedJobsPartitions);
        Assert.True(await AttachedAsync(J(Ago(30))));
    }

    [Fact]
    public async Task Partition_with_only_live_jobs_is_pinned_until_they_finish()
    {
        await CreatePartitionsAsync(Ago(10), events: false);
        await EnqueueLiveAsync(Ago(10), 2);

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal(new PinnedPartition(Ago(10), 6, 2, false), Assert.Single(r.Pinned));
        var leased = await Store.DequeueAsync("q", 2, ServerA, Lease, Ct);
        Assert.Equal(2, (await Store.AckAsync(ServerA, [.. leased.Select(j => new LeaseRef(j.Id, j.LeaseId))], Ct)).Count);
        Assert.Equal([Ago(10)], (await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct)).DroppedJobsPartitions);
    }

    [Fact]
    public async Task Requeue_between_precheck_and_lock_keeps_partition_attached()
    {
        await CreatePartitionsAsync(Ago(10), events: false);
        var failed = await InsertTerminalAsync(Ago(10), 2, state: 3);
        await TerminalPartitionAsync(Ago(9));
        var hooks = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                if (c.Step == RetentionStep.LocksChecked)
                    Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(failed[0], ct)).Result);
            },
        };

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.Equal([Ago(9)], r.DroppedJobsPartitions);
        Assert.DoesNotContain(r.Pinned, p => p.Day == Ago(10));
        Assert.DoesNotContain(r.Deferred, d => d.Day == Ago(10));
        Assert.True(await AttachedAsync(J(Ago(10))));
        Assert.True(await InQueueAsync(failed[0]));
        Assert.Equal((short)0, await JobStateAsync(failed[0]));
    }

    [Fact]
    public async Task Groups_of_k_partitions_share_one_transaction()
    {
        for (var d = 30; d > 5; d--)
            await TerminalPartitionAsync(Ago(d), rows: 1);
        var hooks = new HookRecorder();

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning(k: 10)).ApplyAsync(Ct);

        Assert.Equal(25, r.DroppedJobsPartitions.Count);
        Assert.Equal(3, hooks.Count(RetentionStep.BatchA, "jobs"));
    }

    [Fact]
    public async Task Interrupted_run_leaves_detached_table_that_next_run_drops()
    {
        await TerminalPartitionAsync(Ago(10));
        var hooks = new HookRecorder
        {
            Then = (c, _) => c.Step == RetentionStep.Committed ? throw new InvalidOperationException("pod died") : Task.CompletedTask,
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct));

        Assert.True(await ExistsAsync(J(Ago(10))));
        Assert.False(await AttachedAsync(J(Ago(10))));
        Assert.Equal([Ago(10)], (await Runner().ApplyAsync(Ct)).DroppedJobsPartitions);   // оба срока null
        Assert.False(await ExistsAsync(J(Ago(10))));
    }

    [Fact]
    public async Task Lock_timeout_on_jobs_defers_first_group_and_stops_jobs_only()
    {
        foreach (var d in new[] { 12, 11, 10 })
            await TerminalPartitionAsync(Ago(d), events: true);
        await using (var holder = await HoldAsync("jobs", "ACCESS SHARE"))
        {
            var r = await Runner(o => { o.JobsRetention = Days3; o.EventsRetention = Days3; }, new HookRecorder().Tuning(k: 2)).ApplyAsync(Ct);

            Assert.Equal(new[] { Ago(12), Ago(11) }, r.Deferred.Select(d => d.Day));
            Assert.All(r.Deferred, d =>
            {
                Assert.Equal("jobs", d.Table);
                Assert.Equal(DeferReason.LockTimeout, d.Reason);
            });
            Assert.Empty(r.DroppedJobsPartitions);
            Assert.Equal([Ago(12), Ago(11), Ago(10)], r.DroppedEventsPartitions);
            Assert.True(await AttachedAsync(J(Ago(10))));
            await EnqueueAsync().WaitAsync(TimeSpan.FromSeconds(5), Ct);
            await Store.DequeueAsync("q", 1, ServerA, Lease, Ct).WaitAsync(TimeSpan.FromSeconds(5), Ct);
        }

        Assert.Equal([Ago(12), Ago(11), Ago(10)], (await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct)).DroppedJobsPartitions);
    }

    [Fact]
    public async Task Direct_holder_of_old_partition_is_deferred_before_lock()
    {
        await TerminalPartitionAsync(Ago(11));
        await TerminalPartitionAsync(Ago(10));
        await using var holder = await HoldAsync(J(Ago(11)), "SHARE UPDATE EXCLUSIVE");

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal(new DeferredPartition("jobs", Ago(11), DeferReason.LockTimeout, 7, null), Assert.Single(r.Deferred));
        Assert.Equal([Ago(10)], r.DroppedJobsPartitions);
    }

    [Fact]
    public async Task Reader_through_parent_is_not_excluded_by_precheck_and_times_out_the_lock()
    {
        await TerminalPartitionAsync(Ago(10));
        await using var reader = await Db.Admin.OpenConnectionAsync(Ct);
        await using (var cmd = new NpgsqlCommand(Db.Sql("BEGIN; SELECT count(*) FROM {schema}.jobs"), reader))
            await cmd.ExecuteNonQueryAsync(Ct);

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal(new DeferredPartition("jobs", Ago(10), DeferReason.LockTimeout, 6, null), Assert.Single(r.Deferred));
    }

    [Fact]
    public async Task Holder_appearing_after_locks_check_times_out_in_detach()
    {
        await TerminalPartitionAsync(Ago(10));
        NpgsqlConnection? holder = null;
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step == RetentionStep.LocksChecked)
                    holder = await HoldAsync(J(Ago(10)), "SHARE UPDATE EXCLUSIVE");
            },
        };
        try
        {
            var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

            Assert.Equal(new DeferredPartition("jobs", Ago(10), DeferReason.LockTimeout, 6, null), Assert.Single(r.Deferred));
            Assert.True(await AttachedAsync(J(Ago(10))));
        }
        finally
        {
            if (holder is not null)
                await holder.DisposeAsync();
        }
    }

    [Fact]
    public async Task Autovacuum_like_lock_on_todays_partition_does_not_block_removal()
    {
        await TerminalPartitionAsync(Ago(10));
        await using var holder = await HoldAsync(J(Today), "SHARE UPDATE EXCLUSIVE");

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal([Ago(10)], r.DroppedJobsPartitions);
        Assert.Empty(r.Deferred);
    }

    [Fact]
    public async Task Partition_dropped_after_listing_is_skipped()
    {
        await CreatePartitionsAsync(Ago(10), events: false);
        await EnqueueLiveAsync(Ago(10), 1);
        await InsertTerminalAsync(Ago(10), 150);
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step != RetentionStep.Listed)
                    return;
                await DetachAsync("jobs", J(Ago(10)));
                await Db.ExecAsync($"DROP TABLE {{schema}}.{J(Ago(10))}");
            },
        };

        // Сверка oid в предпроверке: to_regclass → NULL, к партиции по имени предпроверка не обращается.
        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.Empty(r.Pinned);
        Assert.Empty(r.Deferred);
        Assert.Empty(r.DroppedJobsPartitions);
    }

    [Theory]
    [InlineData(10)]    // старый код: count по имени новой таблицы → ложный Pinned(Compacted = false)
    [InlineData(150)]   // старый код: Compact → прогрев новой таблицы и транзакция 1 под блокировкой родителя
    public async Task Partition_replaced_after_listing_is_not_prechecked_by_name(int terminal)
    {
        var day = Ago(10);
        await TerminalPartitionAsync(day);
        long newOid = 0;
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step != RetentionStep.Listed)
                    return;
                await DetachAsync("jobs", J(day));
                await Db.ExecAsync($"ALTER TABLE {{schema}}.{J(day)} RENAME TO old_x");
                await CreatePartitionsAsync(day, events: false);
                await EnqueueLiveAsync(day, 2);
                await InsertTerminalAsync(day, terminal);
                newOid = await OidAsync(J(day));
            },
        };

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.DoesNotContain(r.Pinned, p => p.Day == day);
        Assert.DoesNotContain(r.Deferred, d => d.Day == day);
        Assert.DoesNotContain(day, r.DroppedJobsPartitions);
        Assert.Equal(0, hooks.Count(RetentionStep.WarmedUp, "jobs"));
        Assert.Equal(0, hooks.Count(RetentionStep.BatchA, "jobs"));
        Assert.Equal(newOid, await OidAsync(J(day)));
        Assert.True(await AttachedAsync(J(day)));
        Assert.Equal(2L + terminal, await CountAsync(J(day)));
        Assert.True(await ExistsAsync("old_x"));
    }

    /// <summary>
    /// Другой под сжимает партицию, пока count предпроверки ждёт блокировку старой таблицы: предварительная сверка oid
    /// видит старую таблицу (транзакция держателя не закоммичена), а count после получения блокировки заново разрешает имя
    /// и читает новую. Держатель коммитит, как только предпроверка встала в очередь блокировки старого oid (pg_locks).
    /// </summary>
    [Theory]
    [InlineData(10)]    // без повторной сверки: count прочитал новую таблицу → ложный Pinned(Compacted = false)
    [InlineData(150)]   // без повторной сверки: Compact → прогрев новой таблицы и транзакция 1 под блокировкой родителя
    public async Task Partition_compacted_while_precheck_waits_on_count_is_skipped(int terminal)
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        await EnqueueLiveAsync(day, 2);
        await InsertTerminalAsync(day, 150);
        var oldOid = await OidAsync(J(day));
        long newOid = 0;
        var committed = Task.CompletedTask;
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step != RetentionStep.Listed)
                    return;
                var holder = await BeginHolderAsync($$"""
                    ALTER TABLE {schema}.jobs DETACH PARTITION {schema}.{{J(day)}};
                    ALTER TABLE {schema}.{{J(day)}} RENAME TO old_x;
                    CREATE TABLE {schema}.{{J(day)}} (LIKE {schema}.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS) WITH (fillfactor = 80);
                    INSERT INTO {schema}.{{J(day)}} SELECT * FROM {schema}.old_x WHERE id IN (SELECT job_id FROM {schema}.queue);
                    INSERT INTO {schema}.{{J(day)}}
                        SELECT * FROM {schema}.old_x WHERE id NOT IN (SELECT job_id FROM {schema}.queue) LIMIT {{terminal}};
                    ALTER TABLE {schema}.jobs ATTACH PARTITION {schema}.{{J(day)}}
                        FOR VALUES FROM ('{{UuidV7.LowerBound(day)}}') TO ('{{UuidV7.LowerBound(day.AddDays(1))}}');
                    """);
                await using (var oid = new NpgsqlCommand(Db.Sql($"SELECT '{{schema}}.{J(day)}'::regclass::oid::bigint"), holder))
                    newOid = (long)(await oid.ExecuteScalarAsync(Ct))!;
                committed = CommitWhenWaitedAsync(holder, oldOid);
            },
        };

        var r = await Runner(o => { o.JobsRetention = Days3; o.RetentionLockTimeout = TimeSpan.FromSeconds(30); }, hooks.Tuning())
            .ApplyAsync(Ct);
        await committed.WaitAsync(TimeSpan.FromSeconds(10), Ct);   // предпроверка ждала блокировку старого oid

        Assert.NotEqual(oldOid, newOid);
        Assert.DoesNotContain(r.Pinned, p => p.Day == day);
        Assert.DoesNotContain(r.Deferred, d => d.Day == day);
        Assert.DoesNotContain(day, r.DroppedJobsPartitions);
        Assert.Equal(0, hooks.Count(RetentionStep.WarmedUp, "jobs"));
        Assert.Equal(0, hooks.Count(RetentionStep.BatchA, "jobs"));
        Assert.Equal(newOid, await OidAsync(J(day)));
        Assert.True(await AttachedAsync(J(day)));
        Assert.Equal(2L + terminal, await CountAsync(J(day)));
        Assert.Equal(oldOid, await OidAsync("old_x"));
    }

    /// <summary>
    /// Другой под удаляет партицию, пока count предпроверки ждёт её блокировку: сверка oid до count видит таблицу
    /// (DROP не закоммичен), count после получения блокировки не находит имя → 42P01 → пропуск.
    /// </summary>
    [Fact]
    public async Task Partition_dropped_while_precheck_waits_on_count_is_skipped()
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        await EnqueueLiveAsync(day, 1);
        await InsertTerminalAsync(day, 150);
        var oldOid = await OidAsync(J(day));
        var committed = Task.CompletedTask;
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step != RetentionStep.Listed)
                    return;
                var holder = await BeginHolderAsync($$"""
                    ALTER TABLE {schema}.jobs DETACH PARTITION {schema}.{{J(day)}};
                    DROP TABLE {schema}.{{J(day)}};
                    """);
                committed = CommitWhenWaitedAsync(holder, oldOid);
            },
        };

        var r = await Runner(o => { o.JobsRetention = Days3; o.RetentionLockTimeout = TimeSpan.FromSeconds(30); }, hooks.Tuning())
            .ApplyAsync(Ct);
        await committed.WaitAsync(TimeSpan.FromSeconds(10), Ct);   // предпроверка ждала блокировку удаляемой таблицы

        Assert.Empty(r.Pinned);
        Assert.Empty(r.Deferred);
        Assert.Empty(r.DroppedJobsPartitions);
        Assert.Equal(0, hooks.Count(RetentionStep.WarmedUp, "jobs"));
        Assert.False(await ExistsAsync(J(day)));
    }

    /// <summary>Открытая транзакция другого «пода» на отдельном соединении (Admin) после выполнения sql.</summary>
    private async Task<NpgsqlConnection> BeginHolderAsync(string sql)
    {
        var conn = await Db.Admin.OpenConnectionAsync(Ct);
        try
        {
            await using var cmd = new NpgsqlCommand(Db.Sql("BEGIN;\n" + sql), conn);
            await cmd.ExecuteNonQueryAsync(Ct);
            return conn;
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Коммитит транзакцию держателя, как только кто-то встал в очередь блокировки oid (pg_locks, granted = false), и
    /// закрывает соединение; не дождался — закрывает соединение (откат) и бросает TimeoutException.
    /// </summary>
    private Task CommitWhenWaitedAsync(NpgsqlConnection holder, long oid) => Task.Run(async () =>
    {
        await using (holder)
        {
            await WaitForLockWaitersAsync(oid, 1);
            await using var commit = new NpgsqlCommand("COMMIT", holder);
            await commit.ExecuteNonQueryAsync(Ct);
        }
    });

    [Fact]
    public async Task Missing_queue_in_precheck_is_an_error_not_a_gone_partition()
    {
        await CreatePartitionsAsync(Ago(10), events: false);
        await EnqueueLiveAsync(Ago(10), 1);
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step == RetentionStep.Listed)
                    await Db.ExecAsync("ALTER TABLE {schema}.queue RENAME TO queue_x");
            },
        };

        var e = await Assert.ThrowsAsync<PostgresException>(() => Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct));

        Assert.Equal(PostgresErrorCodes.UndefinedTable, e.SqlState);
    }

    [Fact]
    public async Task Foreign_tables_with_partition_like_names_are_not_touched()
    {
        var foreignDay = Ago(20);
        var attachedRemnantDay = Ago(21);
        await Db.ExecAsync("CREATE TABLE {schema}.jobs_2026101 (LIKE {schema}.jobs)");
        await Db.ExecAsync($"CREATE TABLE {{schema}}.{J(Ago(30))}_old (LIKE {{schema}}.jobs)");
        await Db.ExecAsync("CREATE TABLE {schema}.jobs_20261399 (LIKE {schema}.jobs)");
        await Db.ExecAsync($$"""
            CREATE TABLE {schema}.foreign_parent (LIKE {schema}.jobs) PARTITION BY RANGE (id);
            CREATE TABLE {schema}.{{J(foreignDay)}} (LIKE {schema}.jobs);
            ALTER TABLE {schema}.foreign_parent ATTACH PARTITION {schema}.{{J(foreignDay)}}
                FOR VALUES FROM ('{{UuidV7.LowerBound(foreignDay)}}') TO ('{{UuidV7.LowerBound(foreignDay.AddDays(1))}}');
            CREATE TABLE {schema}.{{J(attachedRemnantDay)}}_r1 (LIKE {schema}.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
            ALTER TABLE {schema}.jobs ATTACH PARTITION {schema}.{{J(attachedRemnantDay)}}_r1
                FOR VALUES FROM ('{{UuidV7.LowerBound(attachedRemnantDay)}}') TO ('{{UuidV7.LowerBound(attachedRemnantDay.AddDays(1))}}');
            """);

        var r = await Runner(o => { o.JobsRetention = Days3; o.EventsRetention = Days3; }).ApplyAsync(Ct);

        Assert.Empty(r.DroppedJobsPartitions);
        Assert.Empty(r.Deferred);
        foreach (var t in new[] { "jobs_2026101", $"{J(Ago(30))}_old", "jobs_20261399", J(foreignDay), $"{J(attachedRemnantDay)}_r1" })
            Assert.True(await ExistsAsync(t), t);
    }
}

public sealed class RetentionJobsTests_Direct(PostgresFixture pg) : RetentionJobsTests(pg, ConnectionMode.Direct);
public sealed class RetentionJobsTests_Bouncer(PostgresFixture pg) : RetentionJobsTests(pg, ConnectionMode.Bouncer);
