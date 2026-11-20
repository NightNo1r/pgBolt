using Microsoft.Extensions.DependencyInjection;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

public abstract class RecurringTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    /// <summary>Code-first расписание «каждую секунду», тикер раз в 200 мс.</summary>
    private static void EverySecond(TestHostSettings s)
    {
        s.Core = o =>
        {
            o.RecurringOwner = "svc";
            o.AddRecurring("tick", "* * * * * *", new SuccessArgs(1),
                new RecurringJobOptions { Misfire = MisfirePolicy.CatchUp, Overlap = OverlapPolicy.Allow });
        };
        s.Processing = p => p.SchedulerInterval = TimeSpan.FromMilliseconds(200);
    }

    private Task<long> SucceededAsync(string id) =>
        Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE recurring_id = @id AND state = @s", ("id", id), ("s", DbState.Succeeded));

    [Fact]
    public async Task Code_first_schedule_fires_and_executor_sees_schedule_fields()
    {
        await StartHostAsync(EverySecond);

        await WaitUntilAsync(async () => await SucceededAsync("tick") >= 2, Timeout, "two scheduled runs succeed");

        var jobId = await Db.ScalarAsync<Guid>("SELECT id FROM {schema}.jobs WHERE recurring_id = 'tick' AND state = 2 ORDER BY id LIMIT 1");
        var scheduledAt = await Db.ScalarAsync<DateTime>("SELECT scheduled_at FROM {schema}.jobs WHERE id = @id", ("id", jobId));
        var run = Assert.Single(Probe.Runs(new JobId(jobId)));
        Assert.Equal("tick", run.RecurringJobId);
        Assert.Equal(new DateTimeOffset(scheduledAt, TimeSpan.Zero), run.ScheduledAt);
    }

    [Fact]
    public async Task Two_hosts_fire_each_second_once()
    {
        await StartHostAsync(s => { s.Name = "a"; EverySecond(s); });
        await StartHostAsync(s => { s.Name = "b"; EverySecond(s); });

        await WaitUntilAsync(async () => await SucceededAsync("tick") >= 4, Timeout, "four scheduled runs succeed");

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT count(*) = count(DISTINCT scheduled_at)
                   AND max(scheduled_at) - min(scheduled_at) = (count(*) - 1) * interval '1 second'
            FROM {schema}.jobs WHERE recurring_id = 'tick'
            """));
    }

    [Fact]
    public async Task Scheduler_fires_without_maintenance()
    {
        await StartHostAsync(s => { EverySecond(s); s.RunMaintenance = false; });

        await WaitUntilAsync(async () => await SucceededAsync("tick") >= 1, Timeout, "scheduled run succeeds");
    }

    [Fact]
    public async Task Api_schedule_is_fired_by_server()
    {
        var host = await StartHostAsync(s => s.Processing = p => p.SchedulerInterval = TimeSpan.FromMilliseconds(200));
        var manager = host.Services.GetRequiredService<IRecurringJobManager>();

        await manager.AddOrUpdateAsync("api", "* * * * * *", new SuccessArgs(2), new RecurringJobOptions { Overlap = OverlapPolicy.Allow }, Ct);

        await WaitUntilAsync(async () => await SucceededAsync("api") >= 1, Timeout, "API schedule runs");
        Assert.True((await manager.GetAsync("api", Ct))!.LastJobId is not null);
    }

    [Fact]
    public async Task Periodic_sync_runs_without_maintenance_and_scheduler()
    {
        const string Rows = "SELECT count(*) FROM {schema}.recurring_jobs WHERE id = 'daily' AND owner = 'svc'";
        await StartHostAsync(s =>
        {
            s.RunMaintenance = false;
            s.RunScheduler = false;
            s.Core = o => { o.RecurringOwner = "svc"; o.AddRecurring("daily", "0 3 * * *", new SuccessArgs(1)); };
            s.Processing = p => p.RecurringSyncInterval = TimeSpan.FromMilliseconds(200);
        });
        Assert.Equal(1L, await Db.ScalarAsync<long>(Rows));            // синхронизация инициализатора на старте

        await Db.ExecAsync("DELETE FROM {schema}.recurring_jobs WHERE id = 'daily'");

        // §6.1: цикл recurring-sync работает независимо от RunMaintenance и RunScheduler и восстанавливает объявление.
        await WaitUntilAsync(async () => await Db.ScalarAsync<long>(Rows) == 1, Timeout, "recurring-sync restores the declared row");
    }
}

public sealed class RecurringTests_Direct(PostgresFixture pg) : RecurringTests(pg, ConnectionMode.Direct);
public sealed class RecurringTests_Bouncer(PostgresFixture pg) : RecurringTests(pg, ConnectionMode.Bouncer);
