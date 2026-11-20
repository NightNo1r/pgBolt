using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>Часть 6 §4.1, §4.2: счётчики исходов процесса в stats_minutes и задача stats кластера.</summary>
public abstract class StatsTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    /// <summary>Суммы stats_minutes сервера по очереди TestJobs.Queue: «succeeded/failed/retried».</summary>
    private Task<string> OutcomesAsync(Guid serverId) =>
        Db.ScalarAsync<string>("""
            SELECT format('%s/%s/%s', coalesce(sum(succeeded), 0), coalesce(sum(failed), 0), coalesce(sum(retried), 0))
            FROM {schema}.stats_minutes WHERE server_id = @s AND queue = @q
            """, ("s", serverId), ("q", TestJobs.Queue));

    [Fact]
    public async Task Confirmed_outcomes_reach_stats_minutes()
    {
        var host = await StartHostAsync(s => s.Processing = p => { p.StatsFlushInterval = TimeSpan.FromMilliseconds(200); });

        var succeeded = new List<JobId>();
        for (var i = 0; i < 3; i++)
            succeeded.Add(await EnqueueAsync(new SuccessArgs(i)));
        succeeded.Add(await EnqueueAsync(new FlakyArgs(1), ImmediateRetry));
        var failed = await EnqueueAsync(new FailArgs("boom"), new EnqueueOptions { MaxAttempts = 1 });

        foreach (var id in succeeded)
            await WaitForStateAsync(id, DbState.Succeeded, Timeout);
        await WaitForStateAsync(failed, DbState.Failed, Timeout);

        await WaitUntilAsync(async () => await OutcomesAsync(host.ServerId) == "4/1/1", Timeout,
            "stats_minutes holds succeeded = 4, failed = 1, retried = 1");
    }

    [Fact]
    public async Task Final_flush_on_stop_records_last_outcomes()
    {
        var host = await StartHostAsync(s => s.Processing = p => { p.StatsFlushInterval = TimeSpan.FromHours(1); });
        JobId[] ids = [await EnqueueAsync(new SuccessArgs(1)), await EnqueueAsync(new SuccessArgs(2))];
        foreach (var id in ids)
            await WaitForStateAsync(id, DbState.Succeeded, Timeout);

        await host.StopAsync();

        Assert.Equal("2/0/0", await OutcomesAsync(host.ServerId));
    }

    [Fact]
    public async Task Stats_task_snapshots_live_states()
    {
        await StartHostAsync(s => s.Processing = p => { p.StatsSnapshotInterval = TimeSpan.FromSeconds(1); });
        await EnqueueAsync(new SuccessArgs(1), new EnqueueOptions { Delay = TimeSpan.FromHours(1) });

        await WaitUntilAsync(() => Db.ScalarAsync<bool>(
                "SELECT EXISTS (SELECT 1 FROM {schema}.queue_stats WHERE queue = @q AND scheduled = 1)", ("q", TestJobs.Queue)),
            TimeSpan.FromSeconds(10), "queue_stats has scheduled = 1 for the queue");
        await WaitUntilAsync(() => Db.ScalarAsync<bool>(
                "SELECT EXISTS (SELECT 1 FROM {schema}.maintenance_tasks WHERE name = 'stats' AND last_finished_at IS NOT NULL AND last_error IS NULL)"),
            TimeSpan.FromSeconds(10), "maintenance_tasks row stats is completed without last_error");
    }
}

public sealed class StatsTests_Direct(PostgresFixture pg) : StatsTests(pg, ConnectionMode.Direct);
public sealed class StatsTests_Bouncer(PostgresFixture pg) : StatsTests(pg, ConnectionMode.Bouncer);
