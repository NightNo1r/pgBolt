using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Снимок queue_stats с возрастом старейшей готовой задачи и чтение снимка вместе с возрастом.</summary>
public abstract class QueueStatsSnapshotTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private Task MakeOlderAsync(Guid id, int seconds) =>
        Db.ExecAsync("UPDATE {schema}.queue SET run_at = now() - make_interval(secs => @s) WHERE job_id = @id",
            ("s", (double)seconds), ("id", id));

    private Task<double?> OldestReadyAsync(string queue) =>
        Db.ScalarAsync<double?>("SELECT oldest_ready_seconds FROM {schema}.queue_stats WHERE queue = @q", ("q", queue));

    private Task SetStatsTaskAsync(TimeSpan finishedAgo, string? error) =>
        Db.ExecAsync("""
            INSERT INTO {schema}.maintenance_tasks (name, next_run_at, last_finished_at, last_error)
            VALUES ('stats', now(), now() - @ago, @e)
            """, ("ago", finishedAgo), ("e", error));

    [Fact]
    public async Task Snapshot_writes_oldest_ready_seconds()
    {
        var ready = await EnqueueAsync("a");
        await MakeOlderAsync(ready, 10);
        await EnqueueAsync("a", delay: TimeSpan.FromHours(1));
        await EnqueueAsync("b", delay: TimeSpan.FromHours(1));
        await EnqueueAsync("c");

        await Store.SnapshotQueueStatsAsync(Ct);

        Assert.True(await OldestReadyAsync("a") >= 10);
        Assert.Null(await OldestReadyAsync("b"));

        await LeaseOneAsync("a");
        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE queue = 'c'");
        await Store.SnapshotQueueStatsAsync(Ct);

        Assert.Null(await OldestReadyAsync("a"));
        Assert.Equal(2L, await CountAsync("queue_stats"));
    }

    [Fact]
    public async Task Read_without_stats_row_has_no_age()
    {
        var snapshot = await Store.ReadQueueStatsAsync(Ct);

        Assert.Null(snapshot.AgeSeconds);
        Assert.Empty(snapshot.Rows);
    }

    [Fact]
    public async Task Read_after_successful_pass_over_empty_queue()
    {
        await SetStatsTaskAsync(TimeSpan.FromSeconds(3), null);

        var snapshot = await Store.ReadQueueStatsAsync(Ct);

        Assert.Empty(snapshot.Rows);
        Assert.True(snapshot.AgeSeconds is >= 3 and < 10);
    }

    [Fact]
    public async Task Read_returns_rows_with_oldest_ready_seconds()
    {
        var ready = await EnqueueAsync("a");
        await MakeOlderAsync(ready, 10);
        await EnqueueAsync("b", delay: TimeSpan.FromHours(1));
        await Store.SnapshotQueueStatsAsync(Ct);

        var snapshot = await Store.ReadQueueStatsAsync(Ct);

        var rows = snapshot.Rows.OrderBy(r => r.Queue, StringComparer.Ordinal).ToList();
        Assert.Equal(["a", "b"], rows.Select(r => r.Queue));
        Assert.Equal((0L, 1L), (rows[0].Scheduled, rows[0].Enqueued));
        Assert.True(rows[0].OldestReadySeconds >= 10);
        Assert.Equal((1L, 0L), (rows[1].Scheduled, rows[1].Enqueued));
        Assert.Null(rows[1].OldestReadySeconds);
    }

    [Fact]
    public async Task Read_after_trim_error_uses_measured_at()
    {
        await EnqueueAsync("a");
        await Store.SnapshotQueueStatsAsync(Ct);
        await SetStatsTaskAsync(TimeSpan.FromHours(1), "x");

        var snapshot = await Store.ReadQueueStatsAsync(Ct);

        Assert.Single(snapshot.Rows);
        Assert.True(snapshot.AgeSeconds is >= 0 and < 2);
    }

    [Fact]
    public async Task Read_after_error_over_empty_table_has_no_age()
    {
        await SetStatsTaskAsync(TimeSpan.FromSeconds(3), "x");

        var snapshot = await Store.ReadQueueStatsAsync(Ct);

        Assert.Empty(snapshot.Rows);
        Assert.Null(snapshot.AgeSeconds);
    }
}

public sealed class QueueStatsSnapshotTests_Direct(PostgresFixture pg) : QueueStatsSnapshotTests(pg, ConnectionMode.Direct);
public sealed class QueueStatsSnapshotTests_Bouncer(PostgresFixture pg) : QueueStatsSnapshotTests(pg, ConnectionMode.Bouncer);
