using Npgsql;
using PgBolt.Server.Runtime;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

public abstract class ShutdownTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    [Fact]
    public async Task Job_stopped_by_shutdown_token_returns_with_previous_attempt_and_is_taken_by_another_server()
    {
        var a = await StartHostAsync(h => h.Name = "A");
        var id = await EnqueueAsync(new BlockArgs(1));
        await WaitUntilAsync(() => Probe.RunCount(id) == 1, TimeSpan.FromSeconds(10), "A starts the job");
        var aId = a.ServerId;
        Assert.True(await ServerRowExistsAsync(aId));

        var elapsed = await a.StopAsync();

        Assert.True(elapsed < ServerTestHost.ShutdownTimeout, $"the job honours the token; stop took {elapsed}");
        var row = await QueueRowAsync(id);
        Assert.NotNull(row);
        Assert.Equal(0, row.Attempt);
        Assert.Null(row.LeaseOwner);
        Assert.Null(row.LeaseId);
        Assert.Null(row.LeaseUntil);
        Assert.True(row.Ready);
        Assert.Equal(DbState.Pending, await JobStateAsync(id));
        Assert.Empty(await EventsAsync(id));
        Assert.False(await ServerRowExistsAsync(aId));
        var first = Assert.Single(Probe.Runs(id));
        Assert.True(first.WasCancelled);
        Assert.Equal(RunEnd.Threw, first.End);

        var b = await StartHostAsync(h => h.Name = "B");
        Probe.Release(id);
        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(10));

        Assert.Equal(1, await AttemptAsync(id));
        Assert.Equal(b.ServerId, await JobServerIdAsync(id));
        var runs = Probe.Runs(id);
        Assert.Equal(2, runs.Count);
        Assert.Equal("B", runs[1].Host);
        Assert.Equal(1, runs[1].Attempt);
    }

    [Fact]
    public async Task Job_ignoring_token_is_abandoned_its_late_outcome_is_not_sent_and_sweeper_spends_the_attempt()
    {
        var a = await StartHostAsync(h => h.Name = "A");
        var id = await EnqueueAsync(new StubbornArgs(1));
        await WaitUntilAsync(() => Probe.RunCount(id) == 1, TimeSpan.FromSeconds(10), "A starts the job");
        var aId = a.ServerId;
        var leased = await QueueRowAsync(id);
        Assert.NotNull(leased);

        var elapsed = await a.StopAsync();

        Assert.InRange(elapsed, ServerTestHost.ShutdownTimeout - TimeSpan.FromMilliseconds(100),
            ServerTestHost.ShutdownTimeout + TimeSpan.FromSeconds(3));
        var afterStop = await QueueRowAsync(id);
        Assert.NotNull(afterStop);
        Assert.Equal(aId, afterStop.LeaseOwner);
        Assert.Equal(leased.LeaseId, afterStop.LeaseId);
        Assert.Equal(1, afterStop.Attempt);
        Assert.False(await ServerRowExistsAsync(aId));

        // Брошенное выполнение завершается успешно, пока lease A ещё действует: ack отправлять уже некому.
        Probe.Release(id);
        await WaitUntilAsync(() => Probe.Runs(id)[0].End == RunEnd.Completed, TimeSpan.FromSeconds(5),
            "abandoned execution finishes");
        await Task.Delay(500, Ct); // явно: за 500 мс (≫ AckFlushInterval) исход брошенного выполнения не пришёл

        var late = await QueueRowAsync(id);
        Assert.NotNull(late);
        Assert.Equal(afterStop.LeaseId, late.LeaseId);
        Assert.Equal(DbState.Pending, await JobStateAsync(id));
        Assert.Empty(await EventsAsync(id));

        var b = await StartHostAsync(h => h.Name = "B");
        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(15));

        Assert.Equal(2, await AttemptAsync(id));
        Assert.Equal(b.ServerId, await JobServerIdAsync(id));
        Assert.Equal(new[] { JobEventKind.LeaseExpired }, await EventKindsAsync(id));
        var runs = Probe.Runs(id);
        Assert.Equal(2, runs.Count);
        Assert.Equal("A", runs[0].Host);
        Assert.True(runs[0].WasCancelled);
        Assert.Equal("B", runs[1].Host);
        Assert.Equal(2, runs[1].Attempt);
    }

    [Fact]
    public async Task Release_of_job_with_cancel_requested_finalizes_it_as_cancelled_with_interrupted_attempt()
    {
        var a = await StartHostAsync(h =>
        {
            h.Name = "A";
            h.Configure = ServerTestHost.SlowHeartbeat;
        });
        var id = await EnqueueAsync(new BlockArgs(1));
        await WaitUntilAsync(() => Probe.RunCount(id) == 1, TimeSpan.FromSeconds(10), "A starts the job");
        var aId = a.ServerId;

        // Флаг — напрямую в БД: heartbeat (раз в 10 с) его не доставит, первым срабатывает shutdown-токен.
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id.Value));
        await a.StopAsync();

        Assert.Null(await QueueRowAsync(id));
        Assert.Equal(DbState.Cancelled, await JobStateAsync(id));
        Assert.Equal(1, await AttemptAsync(id));
        var cancelled = Assert.Single(await EventsAsync(id));
        Assert.Equal(JobEventKind.Cancelled, cancelled.Kind);
        Assert.Equal(1, cancelled.Attempt);
        Assert.Equal(aId, cancelled.ServerId);
        var run = Assert.Single(Probe.Runs(id));
        Assert.True(run.WasCancelled);
        Assert.Equal(RunEnd.Threw, run.End);
    }

    [Fact]
    public async Task Jobs_returned_by_inflight_dequeue_after_stop_signal_are_released_without_spending_attempts()
    {
        var plain1 = await EnqueueAsync(new BlockArgs(1));
        var plain2 = await EnqueueAsync(new BlockArgs(2));
        var cancelled = await EnqueueAsync(new BlockArgs(3));
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", cancelled.Value));

        var proxy = StartProxy();
        await using var locker = await Db.Admin.OpenConnectionAsync(Ct);
        await using var lockTx = await locker.BeginTransactionAsync(Ct);
        await using (var lk = new NpgsqlCommand(Db.Sql("LOCK TABLE {schema}.queue IN EXCLUSIVE MODE"), locker, lockTx))
            await lk.ExecuteNonQueryAsync(Ct);

        // H = 10 с: таймаут dequeue и heartbeat не срабатывают в окне сценария.
        var a = await StartHostAsync(h =>
        {
            h.Name = "A";
            h.Proxy = proxy;
            h.RunMaintenance = false;
            h.RunScheduler = false;
            h.Configure = ServerTestHost.SlowHeartbeat;
        });
        var aId = a.ServerId;
        await WaitForParkedDequeueAsync(TimeSpan.FromSeconds(5));

        proxy.Pause();                 // ответ dequeue будет задержан
        await lockTx.CommitAsync(Ct);  // dequeue выполняется и коммитится до сигнала остановки
        await WaitUntilAsync(async () =>
                await CountAsync("SELECT count(*) FROM {schema}.queue WHERE lease_owner = @s", ("s", aId)) == 3,
            TimeSpan.FromSeconds(5), "parked dequeue commits all three jobs");

        var stop = a.StopAsync();
        await Task.Delay(200, Ct);     // явно: сигнал остановки доставлен раньше ответа dequeue
        Assert.False(stop.IsCompleted, "stop must wait for the in-flight dequeue");
        proxy.Resume();
        await stop.WaitAsync(TimeSpan.FromSeconds(20), Ct);

        foreach (var id in new[] { plain1, plain2 })
        {
            var row = await QueueRowAsync(id);
            Assert.NotNull(row);
            Assert.Equal(0, row.Attempt);
            Assert.Null(row.LeaseOwner);
            Assert.True(row.Ready);
            Assert.Equal(DbState.Pending, await JobStateAsync(id));
            Assert.Empty(await EventsAsync(id));
        }
        Assert.Null(await QueueRowAsync(cancelled));
        Assert.Equal(DbState.Cancelled, await JobStateAsync(cancelled));
        Assert.Equal(0, await AttemptAsync(cancelled));
        var ev = Assert.Single(await EventsAsync(cancelled));
        Assert.Equal(JobEventKind.Cancelled, ev.Kind);
        Assert.Equal(0, ev.Attempt);
        Assert.Equal(0, Probe.TotalStarted);
        Assert.False(await ServerRowExistsAsync(aId));
    }

    [Fact]
    public async Task Several_virtual_servers_in_one_process_stop_together_without_spending_attempts()
    {
        var host = await StartHostAsync(h =>
        {
            h.AddServer("orders", s => s.Queue("qa", ServerTestHost.FastQueue));
            h.AddServer("emails", s => s.Queue("qb", ServerTestHost.FastQueue));
        });
        var ours = Assert.Single(host.HostedServices, s => s.GetType().Assembly == typeof(PgBoltServerOptions).Assembly);
        Assert.IsType<PgBoltProcessingService>(ours); // promoter и sweeper — один ProcessingMaintenance на процесс
        var serverIds = host.ServerIds;
        Assert.Equal(2, serverIds.Count);

        var ids = new List<JobId>();
        foreach (var queue in new[] { "qa", "qb" })
            for (var i = 0; i < 3; i++)
                ids.Add(await EnqueueAsync(new BlockArgs(i), new EnqueueOptions { Queue = queue }));
        await WaitUntilAsync(() => ids.All(id => Probe.RunCount(id) == 1), TimeSpan.FromSeconds(10), "all six jobs start");
        Assert.Equal(2L, await CountAsync("SELECT count(DISTINCT lease_owner) FROM {schema}.queue WHERE lease_owner IS NOT NULL"));

        var elapsed = await host.StopAsync();

        Assert.True(elapsed < ServerTestHost.ShutdownTimeout, $"servers stop concurrently and do not wait for timeouts; took {elapsed}");
        foreach (var id in ids)
        {
            var row = await QueueRowAsync(id);
            Assert.NotNull(row);
            Assert.Equal(0, row.Attempt);
            Assert.Null(row.LeaseOwner);
            Assert.Equal(DbState.Pending, await JobStateAsync(id));
            Assert.Empty(await EventsAsync(id));
        }
        foreach (var serverId in serverIds)
            Assert.False(await ServerRowExistsAsync(serverId));
    }
}

[Collection(TimingSensitiveCollection.Name)]
public sealed class ShutdownTests_Direct(PostgresFixture pg) : ShutdownTests(pg, ConnectionMode.Direct);

[Collection(TimingSensitiveCollection.Name)]
public sealed class ShutdownTests_Bouncer(PostgresFixture pg) : ShutdownTests(pg, ConnectionMode.Bouncer);
