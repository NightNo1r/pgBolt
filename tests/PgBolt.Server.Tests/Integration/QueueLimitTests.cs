using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using PgBolt.Server.Runtime;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>Кластерный лимит очереди на работающем сервере: пропускная способность у лимита, пауза, смена лимита на лету.</summary>
public abstract class QueueLimitTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    private const string Queue = TestJobs.Queue;

    /// <summary>
    /// Максимум взятых строк очереди, пока не отменён stop. Каждый подсчёт — отдельный statement под READ COMMITTED: его
    /// снимок видит только закоммиченные выдачи.
    /// </summary>
    private async Task<long> MaxLeasedAsync(CancellationToken stop)
    {
        var max = 0L;
        await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using var count = new NpgsqlCommand(Db.Sql(
            "SELECT count(*) FROM {schema}.queue WHERE queue = @q AND lease_owner IS NOT NULL"), conn);
        count.Parameters.AddWithValue("q", Queue);
        while (!stop.IsCancellationRequested)
        {
            max = Math.Max(max, (long)(await count.ExecuteScalarAsync(Ct))!);
            await Task.Delay(5, Ct);
        }
        return max;
    }

    /// <summary>
    /// Ждёт лимит очереди в снимке процесса и затем HeartbeatInterval. Dequeue, начатый до публикации снимка, мог выбрать
    /// вариант без лимита и взять задачи, поставленные уже после неё; задачи dequeue диспетчер запускает, только если тот
    /// вернулся в пределах своего таймаута HeartbeatInterval. После ожидания такой dequeue задач теста не запустит.
    /// </summary>
    private static async Task WaitLimitAppliedAsync(QueueSettingsCache settings, string because)
    {
        await WaitUntilAsync(() => settings.Current.Limits.ContainsKey(Queue), TimeSpan.FromSeconds(5), because);
        await Task.Delay(ServerTestHost.HeartbeatInterval + TimeSpan.FromMilliseconds(100), Ct);
    }

    [Fact]
    public async Task Throughput_uses_full_limit()
    {
        await Client.EnqueueManyAsync(Enumerable.Range(0, 200).Select(_ => new SlowArgs(200)).ToList(), null, null, Ct);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var monitor = Task.Run(() => MaxLeasedAsync(stop.Token), Ct);

        // Без FastQueue: MaxPollInterval 10 с, плановые опросы не подменяют пробуждение после коммита исхода.
        await StartHostAsync(s => s.AddServer("s1", o =>
        {
            o.Workers = 16;
            o.AckFlushInterval = TimeSpan.FromMilliseconds(20);
            o.Queue(Queue, q => q.ClusterMaxConcurrency = 4);
        }));
        var clock = Stopwatch.StartNew();
        await Task.Delay(TimeSpan.FromSeconds(10), Ct);
        var completed = Probe.TotalCompleted;
        await stop.CancelAsync();
        var maxLeased = await monitor;

        Assert.True(completed >= 160, $"{completed} jobs of 200 ms completed in {clock.Elapsed.TotalSeconds:0.#} s at limit 4");
        Assert.InRange(maxLeased, 1, 4);
    }

    [Fact]
    public async Task Pause_stops_and_resume_continues()
    {
        var host = await StartHostAsync(s => s.AddServer("s1", o => o.Queue(Queue, ServerTestHost.FastQueue)));
        var manager = host.Services.GetRequiredService<IQueueSettingsManager>();
        var settings = host.Services.GetRequiredService<QueueSettingsCache>();

        await manager.SetConcurrencyOverrideAsync(Queue, 0, ct: Ct);
        await WaitLimitAppliedAsync(settings, "the pause reaches the settings snapshot of the process");

        var ids = new List<JobId>();
        var stream = Stopwatch.StartNew();
        while (stream.Elapsed < TimeSpan.FromSeconds(2))
        {
            ids.Add(await EnqueueAsync(new SuccessArgs(ids.Count), client: host.Client));   // enqueue этого процесса будит диспетчер
            await Task.Delay(50, Ct);
        }
        Assert.Equal(0, Probe.TotalStarted);

        await manager.ResetConcurrencyOverrideAsync(Queue, Ct);
        await WaitUntilAsync(() => Probe.TotalCompleted == ids.Count, TimeSpan.FromSeconds(15), "jobs run after resume");
    }

    [Fact]
    public async Task Limit_appears_after_refresh()
    {
        var host = await StartHostAsync(s => s.AddServer("s1", o => o.Queue(Queue, ServerTestHost.FastQueue)));
        var settings = host.Services.GetRequiredService<QueueSettingsCache>();
        Assert.DoesNotContain(Queue, settings.Current.Limits.Keys);

        await Db.ExecAsync("""
            INSERT INTO {schema}.queue_settings (queue, default_max_concurrency) VALUES (@q, 1)
            ON CONFLICT (queue) DO UPDATE SET default_max_concurrency = 1
            """, ("q", Queue));
        settings.RefreshNow();
        await WaitLimitAppliedAsync(settings, "the limit reaches the settings snapshot of the process");

        var ids = await Client.EnqueueManyAsync(Enumerable.Range(0, 8).Select(_ => new SlowArgs(100)).ToList(), null, null, Ct);
        await WaitUntilAsync(() => Probe.TotalCompleted == ids.Count, TimeSpan.FromSeconds(15), "all jobs complete");

        var runs = ids.SelectMany(r => Probe.Runs(r.Id!.Value)).OrderBy(r => r.StartedAt).ToList();
        Assert.Equal(ids.Count, runs.Count);
        for (var i = 1; i < runs.Count; i++)
            Assert.True(runs[i].StartedAt >= runs[i - 1].FinishedAt, $"run #{i} started before run #{i - 1} finished");
    }
}

[Collection(TimingSensitiveCollection.Name)]
public sealed class QueueLimitTests_Direct(PostgresFixture pg) : QueueLimitTests(pg, ConnectionMode.Direct);

[Collection(TimingSensitiveCollection.Name)]
public sealed class QueueLimitTests_Bouncer(PostgresFixture pg) : QueueLimitTests(pg, ConnectionMode.Bouncer);
