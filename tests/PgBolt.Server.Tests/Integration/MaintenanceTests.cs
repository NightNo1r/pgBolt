using System.Globalization;
using Microsoft.Extensions.Logging;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

public abstract class MaintenanceTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    private static void FastMaintenance(TestHostSettings s)
    {
        s.Core = o =>
        {
            o.JobsRetention = TimeSpan.FromDays(3);
            o.EventsRetention = TimeSpan.FromDays(3);
            // Dequeue обоих хостов держит блокировки родителя и партиций; единичный LockTimeout при сроке по умолчанию
            // отложил бы удаление до следующего RetentionInterval (час) — тест упал бы по таймауту ожидания.
            o.RetentionLockTimeout = TimeSpan.FromSeconds(5);
        };
        s.Processing = p => p.MaintenancePollInterval = TimeSpan.FromMilliseconds(200);
    }

    private async Task<string> OldPartitionAsync()
    {
        var today = await Db.ScalarAsync<DateOnly>("SELECT (now() AT TIME ZONE 'UTC')::date");
        var day = today.AddDays(-10);
        var suffix = day.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        var lo = PgBolt.Postgres.UuidV7.LowerBound(day);
        var hi = PgBolt.Postgres.UuidV7.LowerBound(day.AddDays(1));
        await Db.ExecAsync($$"""
            CREATE TABLE {schema}.jobs_{{suffix}} (LIKE {schema}.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS) WITH (fillfactor = 80);
            ALTER TABLE {schema}.jobs ATTACH PARTITION {schema}.jobs_{{suffix}} FOR VALUES FROM ('{{lo}}') TO ('{{hi}}');
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, attempt, max_attempts, created_at, finished_at)
            VALUES ('{{Guid.CreateVersion7(new DateTimeOffset(day.ToDateTime(new TimeOnly(12, 0)), TimeSpan.Zero))}}', 't', 'q', 0, '{}', 2, 1, 3, now(), now());
            """);
        return $"jobs_{suffix}";
    }

    [Fact]
    public async Task Retention_runs_on_exactly_one_of_two_hosts()
    {
        var partition = await OldPartitionAsync();
        var a = await StartHostAsync(s => { s.Name = "a"; FastMaintenance(s); });
        var b = await StartHostAsync(s => { s.Name = "b"; FastMaintenance(s); });

        await WaitUntilAsync(async () => !await Db.ScalarAsync<bool>("SELECT to_regclass(@t) IS NOT NULL", ("t", $"{Db.Schema}.{partition}")),
            Timeout, "old partition is dropped");
        await WaitUntilAsync(() => Db.ScalarAsync<bool>(
            "SELECT count(*) = 3 AND bool_and(last_finished_at IS NOT NULL AND last_error IS NULL) FROM {schema}.maintenance_tasks WHERE name <> 'stats'"),
            Timeout, "all three tasks are claimed and completed");

        // Повторный захват до next_run_at переписал бы last_run_at: каждая задача захвачена ровно одним подом.
        const string claimsSql = "SELECT string_agg(name || '=' || last_run_at::text, ',' ORDER BY name) FROM {schema}.maintenance_tasks WHERE name <> 'stats'";
        var claims = await Db.ScalarAsync<string>(claimsSql);
        await Task.Delay(TimeSpan.FromSeconds(1), Ct); // явно: ≥ 4 итерации housekeeping каждого хоста
        Assert.Equal(claims, await Db.ScalarAsync<string>(claimsSql));

        var reporters = new[] { a, b }.Count(h => h.Logs.Entries.Any(e => e.Message.Contains("PgBolt retention: dropped 1 jobs partitions")));
        Assert.Equal(1, reporters);
    }

    [Fact]
    public async Task Removed_servers_row_of_live_server_is_restored_and_gone_after_stop()
    {
        var host = await StartHostAsync(s => s.RunMaintenance = false);
        var id = host.ServerId;
        await Db.ExecAsync("DELETE FROM {schema}.servers WHERE id = @id", ("id", id));

        await WaitUntilAsync(() => Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM {schema}.servers WHERE id = @id)", ("id", id)),
            Timeout, "next heartbeat registers the server again");
        // Warning пишется после RegisterServerAsync: строка в БД может появиться раньше записи в логе.
        await WaitUntilAsync(() => Task.FromResult(host.Logs.Entries.Any(e => e.Level == LogLevel.Warning && e.Message.Contains("registered again"))),
            Timeout, "re-registration is logged as a warning");

        await host.StopAsync();

        Assert.False(await Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM {schema}.servers WHERE id = @id)", ("id", id)));
    }

    [Fact]
    public async Task Dead_servers_rows_are_removed_by_the_servers_task()
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.servers (id, name, host, queues, capacity, started_at, heartbeat_at)
            VALUES (gen_random_uuid(), 'dead', 'h', ARRAY['q'], 1, now() - interval '2 hours', now() - interval '2 hours')
            """);
        var host = await StartHostAsync(FastMaintenance);

        await WaitUntilAsync(async () => await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.servers WHERE name = 'dead'") == 0,
            Timeout, "dead server row is removed");
        Assert.True(await Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM {schema}.servers WHERE id = @id)", ("id", host.ServerId)));

        // Строку живого хоста мог бы вернуть heartbeat (повторная регистрация) до проверки выше — значит, она не удалялась,
        // только если повторной регистрации не было. StopAsync дожидается цикла heartbeat, поэтому её Warning уже записан.
        await host.StopAsync();
        Assert.DoesNotContain(host.Logs.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("registered again"));
    }
}

public sealed class MaintenanceTests_Direct(PostgresFixture pg) : MaintenanceTests(pg, ConnectionMode.Direct);
public sealed class MaintenanceTests_Bouncer(PostgresFixture pg) : MaintenanceTests(pg, ConnectionMode.Bouncer);
