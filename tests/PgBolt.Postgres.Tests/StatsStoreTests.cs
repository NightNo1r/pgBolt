using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 6 §4.1, §4.2: сброс счётчиков исходов, снимок queue_stats, retention stats_minutes.</summary>
public abstract class StatsStoreTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private Task<string?> MinutesAsync() =>
        Db.ScalarAsync<string?>("""
            SELECT string_agg(server_id || ':' || succeeded || ':' || failed || ':' || retried, ',' ORDER BY server_id)
            FROM {schema}.stats_minutes
            """);

    private Task<string?> QueueStatsAsync() =>
        Db.ScalarAsync<string?>("""
            SELECT string_agg(queue || ':' || scheduled || ':' || enqueued || ':' || processing || ':' || awaiting, ',' ORDER BY queue)
            FROM {schema}.queue_stats
            """);

    [Fact]
    public async Task Flush_inserts_and_adds_to_existing_rows()
    {
        var b = new DateTime(2026, 10, 5, 12, 0, 0, DateTimeKind.Utc);

        await Store.FlushOutcomeStatsAsync([new(b, ServerA, "q", 2, 1, 0), new(b, ServerB, "q", 1, 0, 0)], Ct);
        await Store.FlushOutcomeStatsAsync([new(b, ServerA, "q", 3, 0, 4)], Ct);

        Assert.Equal($"{ServerA}:5:1:4,{ServerB}:1:0:0", await MinutesAsync());
        await Db.ExecAsync("DROP TABLE {schema}.stats_minutes");
        await Store.FlushOutcomeStatsAsync([], Ct);   // пустой список — без обращения к БД
    }

    [Fact]
    public async Task Snapshot_counts_every_derived_state_per_queue()
    {
        await EnqueueAsync("a", delay: TimeSpan.FromHours(1));
        await EnqueueAsync("a");
        await EnqueueAsync("a");
        await LeaseOneAsync("a");
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition)
            VALUES (@id, 'a', 0, 'infinity', false, 3, @p, 1)
            """, ("id", Guid.CreateVersion7()), ("p", Guid.CreateVersion7()));
        await EnqueueAsync("b");

        await Store.SnapshotQueueStatsAsync(Ct);

        Assert.Equal("a:1:1:1:1,b:0:1:0:0", await QueueStatsAsync());
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.queue_stats WHERE measured_at IS NULL"));
    }

    [Fact]
    public async Task Snapshot_removes_vanished_queues_and_empty_queue_gives_empty_table()
    {
        await EnqueueAsync("a");
        await Store.SnapshotQueueStatsAsync(Ct);
        Assert.Equal("a:0:1:0:0", await QueueStatsAsync());

        await Db.ExecAsync("DELETE FROM {schema}.queue");
        await Store.SnapshotQueueStatsAsync(Ct);

        Assert.Null(await QueueStatsAsync());
    }

    [Fact]
    public async Task Trim_deletes_old_buckets_in_portions()
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.stats_minutes (bucket, server_id, queue)
            SELECT now() - interval '8 days' - g * interval '1 minute', @s, 'q' FROM generate_series(1, 25) g
            UNION ALL
            SELECT now() - g * interval '1 minute', @s, 'q' FROM generate_series(1, 3) g
            """, ("s", ServerA));
        var retention = TimeSpan.FromDays(7);

        Assert.Equal(10, await Store.TrimStatsAsync(retention, 10, Ct));
        Assert.Equal(10, await Store.TrimStatsAsync(retention, 10, Ct));
        Assert.Equal(5, await Store.TrimStatsAsync(retention, 10, Ct));
        Assert.Equal(0, await Store.TrimStatsAsync(retention, 10, Ct));
        Assert.Equal(3L, await CountAsync("stats_minutes"));
    }
}

public sealed class StatsStoreTests_Direct(PostgresFixture pg) : StatsStoreTests(pg, ConnectionMode.Direct);
public sealed class StatsStoreTests_Bouncer(PostgresFixture pg) : StatsStoreTests(pg, ConnectionMode.Bouncer);
