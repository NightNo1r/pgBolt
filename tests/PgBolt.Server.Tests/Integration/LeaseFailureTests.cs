using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using PgBolt.Postgres.Hosting;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

public abstract class LeaseFailureTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    private const string LeaseRenewalNotConfirmed = "cancelled: lease renewal not confirmed";

    private static void LongLease(PgBoltServerOptions s) => s.LeaseDuration = TimeSpan.FromSeconds(6); // H = 1 с

    [Fact]
    public async Task Sweeper_of_another_server_takes_job_from_server_with_lost_heartbeat_and_late_ack_is_rejected()
    {
        var proxy = StartProxy();
        var a = await StartHostAsync(h =>
        {
            h.Name = "A";
            h.Proxy = proxy;
        });
        var id = await EnqueueAsync(new StubbornArgs(1));
        await WaitUntilAsync(() => Probe.RunCount(id) == 1, TimeSpan.FromSeconds(10), "A starts the job");
        var b = await StartHostAsync(h => h.Name = "B");

        proxy.Pause();
        await WaitUntilAsync(() => Probe.Run(id, 1)!.WasCancelled, TimeSpan.FromSeconds(5),
            "A cancels its execution when lease renewal is not confirmed");
        await WaitUntilAsync(() => Probe.Run(id, 2) is not null, TimeSpan.FromSeconds(15),
            "B takes the job after its sweeper resets the expired lease");

        var onA = Probe.Run(id, 1)!;
        var onB = Probe.Run(id, 2)!;
        Assert.Equal("A", onA.Host);
        Assert.Equal("B", onB.Host);
        Assert.True(onA.CancelledAt < onB.StartedAt, "A must be cancelled before the job is handed to B");

        Probe.Release(id); // обе копии завершаются успешно; у A — с устаревшим lease_id
        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(10));
        proxy.Resume();
        await a.StopAsync(); // A отправляет всё накопленное — это не должно ничего изменить

        Assert.Equal(DbState.Succeeded, await JobStateAsync(id));
        Assert.Equal(2, await AttemptAsync(id));
        Assert.Equal(b.ServerId, await JobServerIdAsync(id));
        Assert.Equal(new[] { JobEventKind.LeaseExpired }, await EventKindsAsync(id));
        Assert.Equal(RunEnd.Completed, onA.End);
        Assert.Equal(RunEnd.Completed, onB.End);
    }

    [Fact]
    public async Task Same_server_retaking_a_swept_job_tracks_executions_by_lease_id()
    {
        var proxy = StartProxy();
        var a = await StartHostAsync(h =>
        {
            h.Name = "A";
            h.Proxy = proxy;
        });
        await StartHostAsync(h =>
        {
            h.Name = "Sweeper";
            h.AddServer("idle", s => s.Queue("idle", ServerTestHost.FastQueue));
        });
        var id = await EnqueueAsync(new StubbornArgs(1));
        await WaitUntilAsync(() => Probe.RunCount(id) == 1, TimeSpan.FromSeconds(10), "A starts the job");
        var firstLease = (await QueueRowAsync(id))!.LeaseId;

        proxy.Pause();
        await WaitUntilAsync(async () => (await QueueRowAsync(id))?.LeaseOwner is null, TimeSpan.FromSeconds(15),
            "sweeper of another process resets the expired lease of A");
        proxy.Resume();
        await WaitUntilAsync(() => Probe.Run(id, 2) is not null, TimeSpan.FromSeconds(15),
            "A takes the same job again while the first execution is still running");

        var second = await QueueRowAsync(id);
        Assert.NotNull(second);
        Assert.Equal(a.ServerId, second.LeaseOwner);
        Assert.NotEqual(firstLease, second.LeaseId);
        Assert.Equal("A", Probe.Run(id, 2)!.Host);

        Probe.Release(id, attempt: 1); // первая копия завершается успешно — её исход относится к потерянному lease
        await WaitUntilAsync(() => Probe.Run(id, 1)!.End == RunEnd.Completed, TimeSpan.FromSeconds(5), "first copy finishes");
        await Task.Delay(TimeSpan.FromSeconds(1), Ct); // явно: за 1 с (≫ AckFlushInterval, ≈ H) исход первой копии не применился

        var still = await QueueRowAsync(id);
        Assert.NotNull(still);
        Assert.Equal(second.LeaseId, still.LeaseId);
        Assert.Equal(DbState.Pending, await JobStateAsync(id));
        Assert.Equal(RunEnd.Running, Probe.Run(id, 2)!.End);

        Probe.Release(id, attempt: 2);
        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(10));
        Assert.Equal(2, await AttemptAsync(id));
        Assert.Equal(new[] { JobEventKind.LeaseExpired }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Orphan_row_with_unknown_lease_is_not_renewed_and_is_handled_by_sweeper()
    {
        var a = await StartHostAsync(h => h.Name = "A");
        var aId = a.ServerId;
        var id = await EnqueueAsync(new SuccessArgs(1), new EnqueueOptions { Delay = TimeSpan.FromHours(1) });

        // Строка с lease_owner = A и lease_id, которого runtime A не знает (как после dequeue без полученного ответа).
        var orphanLeaseUntil = await Db.ScalarAsync<DateTime>("""
            UPDATE {schema}.queue
            SET lease_owner = @s, lease_id = gen_random_uuid(), lease_until = now() + interval '3 seconds',
                leased_at = now(), attempt = attempt + 1, ready = true
            WHERE job_id = @id
            RETURNING lease_until
            """, ("s", aId), ("id", id.Value));
        var orphanedAt = await DbNowAsync();

        await WaitUntilAsync(async () => await HeartbeatAtAsync(aId) > orphanedAt, TimeSpan.FromSeconds(5),
            "A sends a heartbeat after the orphan row appears");
        var row = await QueueRowAsync(id);
        Assert.NotNull(row);
        Assert.Equal(aId, row.LeaseOwner);
        Assert.Equal(orphanLeaseUntil, row.LeaseUntil); // heartbeat не продлил неизвестный lease

        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(15));
        Assert.Equal(2, await AttemptAsync(id));
        Assert.Equal(new[] { JobEventKind.LeaseExpired }, await EventKindsAsync(id));
        Assert.Equal(2, Assert.Single(Probe.Runs(id)).Attempt);
    }

    [Fact]
    public async Task Outcome_that_cannot_be_written_within_its_budget_expires_and_sweeper_spends_the_attempt()
    {
        var a = await StartHostAsync(h => h.Name = "A");
        var aId = a.ServerId;
        await WaitUntilAsync(() => Db.ScalarAsync<bool>(
            "SELECT count(*) = 3 AND bool_and(last_finished_at IS NOT NULL) FROM {schema}.maintenance_tasks WHERE name <> 'stats'"),
            TimeSpan.FromSeconds(10), "first housekeeping iteration finished");
        await DropTodayEventsPartitionAsync();
        var id = await EnqueueAsync(new FlakyArgs(1), ImmediateRetry);

        await WaitUntilAsync(() => Probe.Run(id, 1)?.End == RunEnd.Threw, TimeSpan.FromSeconds(10), "first attempt fails");
        await WaitUntilAsync(async () => await Db.ScalarAsync<bool>(
                "SELECT lease_owner = @s AND lease_until < now() FROM {schema}.queue WHERE job_id = @id",
                ("s", aId), ("id", id.Value)),
            TimeSpan.FromSeconds(15),
            "retry outcome is abandoned after its LeaseDuration budget and the lease is no longer renewed");

        // Sweeper тоже не может записать LeaseExpired — задача ждёт в queue, а не пропадает.
        Assert.Empty(await EventsAsync(id));
        Assert.Equal(DbState.Pending, await JobStateAsync(id));
        Assert.Equal(1, Probe.RunCount(id));

        await a.Services.GetRequiredService<IPgBoltMaintenance>().EnsurePartitionsAsync(2, Ct);
        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(15));

        Assert.Equal(2, await AttemptAsync(id));
        Assert.Equal(new[] { JobEventKind.LeaseExpired }, await EventKindsAsync(id));
        var runs = Probe.Runs(id);
        Assert.Equal(2, runs.Count);
        Assert.Equal(RunEnd.Threw, runs[0].End);
        Assert.Equal(RunEnd.Completed, runs[1].End);
    }

    [Fact]
    public async Task Short_outage_below_L_minus_H_cancels_nothing_and_delivers_outcomes_after_recovery()
    {
        var proxy = StartProxy();
        await StartHostAsync(h =>
        {
            h.Name = "A";
            h.Proxy = proxy;
            h.Configure = LongLease;
        });
        var ids = new List<JobId>();
        for (var i = 0; i < 4; i++)
            ids.Add(await EnqueueAsync(new BlockArgs(i)));
        await WaitUntilAsync(() => ids.All(id => Probe.RunCount(id) == 1), TimeSpan.FromSeconds(10), "all four jobs start");
        var running = ids[3];
        var leaseBefore = (await QueueRowAsync(running))!.LeaseUntil!.Value;

        proxy.Pause();
        for (var i = 0; i < 3; i++)
            Probe.Release(ids[i]); // завершаются во время сбоя: ack не может уйти
        await WaitUntilAsync(() => ids.Take(3).All(id => Probe.Run(id, 1)!.End == RunEnd.Completed), TimeSpan.FromSeconds(5),
            "three jobs finish during the outage");
        await Task.Delay(TimeSpan.FromSeconds(2), Ct); // длительность сбоя: 2 с < L − H = 5 с
        proxy.Resume();

        foreach (var id in ids.Take(3))
            await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(10));
        await WaitUntilAsync(async () => (await QueueRowAsync(running))!.LeaseUntil > leaseBefore + TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(5), "lease of the still running execution is renewed after recovery");
        Probe.Release(running);
        await WaitForStateAsync(running, DbState.Succeeded, TimeSpan.FromSeconds(10));

        foreach (var id in ids)
        {
            var run = Assert.Single(Probe.Runs(id));
            Assert.False(run.WasCancelled);
            Assert.Equal(1, await AttemptAsync(id));
            Assert.Empty(await EventsAsync(id));
        }
    }

    [Fact]
    public async Task Outage_longer_than_L_minus_H_cancels_executions_before_their_lease_expires_and_spends_the_attempt()
    {
        var proxy = StartProxy();
        await StartHostAsync(h =>
        {
            h.Name = "A";
            h.Proxy = proxy;
            h.Configure = LongLease;
        });
        var ids = new List<JobId>();
        for (var i = 0; i < 3; i++)
            ids.Add(await EnqueueAsync(new BlockArgs(i)));
        await WaitUntilAsync(() => ids.All(id => Probe.RunCount(id) == 1), TimeSpan.FromSeconds(10), "all three jobs start");

        proxy.Pause();
        var pausedLocal = Stopwatch.GetTimestamp();
        var pausedDb = await DbNowAsync(); // admin-подключение — мимо прокси
        var leaseUntil = new Dictionary<JobId, DateTime>();
        foreach (var id in ids)
            leaseUntil[id] = (await QueueRowAsync(id))!.LeaseUntil!.Value;

        await WaitUntilAsync(() => ids.All(id => Probe.Run(id, 1)!.WasCancelled), TimeSpan.FromSeconds(7),
            "executions are cancelled while the database is unreachable");
        foreach (var id in ids)
        {
            var cancelledDb = pausedDb + Stopwatch.GetElapsedTime(pausedLocal, Probe.Run(id, 1)!.CancelledAt);
            Assert.True(cancelledDb <= leaseUntil[id] - TimeSpan.FromMilliseconds(500),
                $"execution of {id} cancelled at {cancelledDb:O}, its lease is valid until {leaseUntil[id]:O}");
        }

        var rest = TimeSpan.FromSeconds(7) - Stopwatch.GetElapsedTime(pausedLocal);
        if (rest > TimeSpan.Zero)
            await Task.Delay(rest, Ct); // длительность сбоя: 7 с > L − H = 5 с
        proxy.Resume();
        foreach (var id in ids)
            Probe.Release(id);

        foreach (var id in ids)
        {
            await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(20));
            Assert.Equal(2, await AttemptAsync(id));
            // Попытку засчитывает либо retry-исход после восстановления, либо sweeper, если lease истёк раньше.
            var ev = Assert.Single(await EventsAsync(id));
            Assert.Contains(ev.Kind, new[] { JobEventKind.Retry, JobEventKind.LeaseExpired });
            if (ev.Kind == JobEventKind.Retry)
                Assert.Contains(LeaseRenewalNotConfirmed, ev.Message);
            Assert.Equal(2, Probe.RunCount(id));
        }
    }

    [Fact]
    public async Task Dequeue_completing_after_heartbeat_loss_releases_its_jobs_without_running_them()
    {
        var a = await StartHostAsync(h =>
        {
            h.Name = "A";
            h.RunMaintenance = false;
            h.RunScheduler = false;
            h.Configure = s =>
            {
                s.Workers = 4;
                s.LeaseDuration = TimeSpan.FromSeconds(9);
                s.HeartbeatInterval = TimeSpan.FromSeconds(3);
            };
        });
        var aId = a.ServerId;
        var canary = await EnqueueAsync(new BlockArgs(0), ImmediateRetry);
        await WaitUntilAsync(() => Probe.RunCount(canary) == 1, TimeSpan.FromSeconds(10), "canary starts");
        var canaryStarted = await DbNowAsync();
        await WaitUntilAsync(async () => await HeartbeatAtAsync(aId) > canaryStarted + TimeSpan.FromMilliseconds(200),
            TimeSpan.FromSeconds(10), "a successful heartbeat covers the canary lease");

        // 1. Heartbeat блокируется на строке servers (её обновляет только он); dequeue продолжает работать.
        await using var hbLock = await Db.Admin.OpenConnectionAsync(Ct);
        await using var hbTx = await hbLock.BeginTransactionAsync(Ct);
        DateTime lastHeartbeat;
        await using (var cmd = new NpgsqlCommand(Db.Sql("SELECT heartbeat_at FROM {schema}.servers WHERE id = @id FOR UPDATE"), hbLock, hbTx))
        {
            cmd.Parameters.AddWithValue("id", aId);
            lastHeartbeat = (DateTime)(await cmd.ExecuteScalarAsync(Ct))!;
        }

        // 2. «Heartbeat потерян» наступит в lastHeartbeat + L − H = +6 с; за 1.5 с до этого паркуем dequeue (таймаут H = 3 с).
        await DelayUntilDbTimeAsync(lastHeartbeat + TimeSpan.FromSeconds(4.5));
        await using var qLock = await Db.Admin.OpenConnectionAsync(Ct);
        await using var qTx = await qLock.BeginTransactionAsync(Ct);
        await using (var cmd = new NpgsqlCommand(Db.Sql("LOCK TABLE {schema}.queue IN EXCLUSIVE MODE"), qLock, qTx))
            await cmd.ExecuteNonQueryAsync(Ct);
        var plain = await EnqueueAsync(new SuccessArgs(1), new EnqueueOptions { Transaction = qTx });
        var cancelled = await EnqueueAsync(new SuccessArgs(2), new EnqueueOptions { Transaction = qTx });
        await using (var cmd = new NpgsqlCommand(Db.Sql("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id"), qLock, qTx))
        {
            cmd.Parameters.AddWithValue("id", cancelled.Value);
            await cmd.ExecuteNonQueryAsync(Ct);
        }
        await WaitForParkedDequeueAsync(TimeSpan.FromSeconds(1));

        // 3. Вход в «heartbeat потерян» виден по отмене canary: её рубеж validUntil − H совпадает с ним (§6.2).
        await WaitUntilAsync(() => Probe.Run(canary, 1)!.WasCancelled, TimeSpan.FromSeconds(4),
            "canary is cancelled at lastHeartbeat + L − H");
        Assert.True(await ParkedDequeueCountAsync() > 0, "dequeue must still be in flight when heartbeat is lost");
        await qTx.CommitAsync(Ct); // dequeue завершается уже в состоянии «heartbeat потерян»

        await WaitForStateAsync(cancelled, DbState.Cancelled, TimeSpan.FromSeconds(5));
        await WaitUntilAsync(async () => (await QueueRowAsync(plain))?.LeaseOwner is null, TimeSpan.FromSeconds(5),
            "plain job is released");
        Assert.Equal(0, (await QueueRowAsync(plain))!.Attempt);
        Assert.Equal(0, await AttemptAsync(cancelled));
        Assert.Equal(new[] { JobEventKind.Cancelled }, await EventKindsAsync(cancelled));
        Assert.Equal(0, Probe.RunCount(plain));
        Assert.Equal(0, Probe.RunCount(cancelled));

        // 4. Heartbeat восстанавливается: plain выполняется, canary повторяется с тратой попытки.
        await hbTx.CommitAsync(Ct);
        Probe.Release(canary);
        await WaitForStateAsync(plain, DbState.Succeeded, TimeSpan.FromSeconds(15));
        await WaitForStateAsync(canary, DbState.Succeeded, TimeSpan.FromSeconds(15));

        Assert.Equal(1, await AttemptAsync(plain));
        Assert.Equal(2, await AttemptAsync(canary));
        var retry = Assert.Single(await EventsAsync(canary));
        Assert.Equal(JobEventKind.Retry, retry.Kind);
        Assert.Contains(LeaseRenewalNotConfirmed, retry.Message);
    }

    [Fact]
    public async Task Job_exhausting_the_pool_is_cancelled_by_HeartbeatLost_and_fails_after_max_attempts()
    {
        var csb = new NpgsqlConnectionStringBuilder(Db.ConnectionString) { MaxPoolSize = 2 };
        var dataSource = Track(NpgsqlDataSource.Create(csb.ConnectionString));
        await StartHostAsync(h =>
        {
            h.Name = "A";
            h.DataSource = dataSource;
            h.RunMaintenance = false;
            h.RunScheduler = false;
            h.Configure = s => s.Workers = 1;
        });

        var id = await EnqueueAsync(new PoolHogArgs(2)); // max_attempts = 2, retry_delays = [0]

        await WaitForStateAsync(id, DbState.Failed, TimeSpan.FromSeconds(20));
        Assert.Equal(2, await AttemptAsync(id));
        var events = await EventsAsync(id);
        Assert.Equal(new[] { JobEventKind.Retry, JobEventKind.Failed }, events.Select(e => e.Kind));
        Assert.All(events, e => Assert.Contains(LeaseRenewalNotConfirmed, e.Message));
        var runs = Probe.Runs(id);
        Assert.Equal(2, runs.Count);
        Assert.All(runs, r =>
        {
            Assert.True(r.WasCancelled);
            Assert.Equal(RunEnd.Threw, r.End);
        });
    }

    /// <summary>Протокол удаления партиции core §4.4: короткий lock_timeout на родителе, DETACH + DROP, повтор.</summary>
    private async Task DropTodayEventsPartitionAsync()
    {
        var suffix = await Db.ScalarAsync<string>("SELECT to_char((now() AT TIME ZONE 'UTC')::date, 'YYYYMMDD')");
        for (var attempt = 1; ; attempt++)
        {
            await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
            await using var tx = await conn.BeginTransactionAsync(Ct);
            try
            {
                foreach (var sql in new[]
                         {
                             "SET LOCAL lock_timeout = '100ms'",
                             "LOCK TABLE {schema}.job_events IN ACCESS EXCLUSIVE MODE",
                             $"ALTER TABLE {{schema}}.job_events DETACH PARTITION {{schema}}.job_events_{suffix}",
                             $"DROP TABLE {{schema}}.job_events_{suffix}",
                         })
                {
                    await using var cmd = new NpgsqlCommand(Db.Sql(sql), conn, tx);
                    await cmd.ExecuteNonQueryAsync(Ct);
                }
                await tx.CommitAsync(Ct);
                return;
            }
            catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.LockNotAvailable && attempt < 50)
            {
                await tx.RollbackAsync(Ct);
            }
            await Task.Delay(50, Ct);
        }
    }
}

[Collection(TimingSensitiveCollection.Name)]
public sealed class LeaseFailureTests_Direct(PostgresFixture pg) : LeaseFailureTests(pg, ConnectionMode.Direct);

[Collection(TimingSensitiveCollection.Name)]
public sealed class LeaseFailureTests_Bouncer(PostgresFixture pg) : LeaseFailureTests(pg, ConnectionMode.Bouncer);
