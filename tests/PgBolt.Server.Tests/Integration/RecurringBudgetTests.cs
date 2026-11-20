using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>Итерация цикла scheduler на реальной БД (часть 4 §5.1, §5.2): лимит срабатываний пакета и повтор полного пакета.</summary>
public abstract class RecurringBudgetTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    [Fact]
    public async Task Rows_over_fire_budget_fire_in_next_transaction_of_same_iteration()   // §10: лимит 1000 срабатываний пакета
    {
        var store = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: true);
        await store.PublishJobTypesAsync([new JobTypeRecord(TestJobs.Success.Name, "{}", TestJobs.Queue, 3, 0, null)], "test", Ct);
        // 15 строк CatchUp + Allow по 150 пропущенных срабатываний; r00 проснулась раньше всех, r14 — позже.
        await Db.ExecAsync("""
            INSERT INTO {schema}.recurring_jobs (id, type, args, cron, time_zone, misfire, catch_up_window, overlap,
                                                 next_fire_at, wake_at, created_at, updated_at)
            SELECT 'r' || lpad(g::text, 2, '0'), 'test.success', '{"N":1}', '* * * * *', 'UTC', 2, interval '1 day', 1,
                   date_trunc('minute', now()) - interval '150 minutes', now() - (100 - g) * interval '1 second', now(), now()
            FROM generate_series(0, 14) g
            """);
        var options = new PgBoltProcessingOptions();
        var clock = new MonoClock(TimeProvider.System);
        var maintenance = new ProcessingMaintenance(new PgBoltServerStore(store), new InProcessWakeupSignal(), options,
            new FakeMaintenance(), new PgBoltOptions(), clock, NullLogger.Instance, TestTelemetry.Metrics(), new ClusterQueueStats(clock, options),
            new ClusterTableStats(clock, options));

        var fired = await maintenance.TickOnceAsync(Ct);                        // одна итерация цикла

        Assert.Equal(await CountAsync("SELECT count(*) FROM {schema}.jobs"), fired);
        // Первая транзакция упёрлась в лимит: ровно 1000 задач r00–r09, строки r10–r14 не тронуты ею.
        const string FirstTx = "(SELECT xmin::text FROM {schema}.jobs WHERE recurring_id = 'r00' ORDER BY scheduled_at LIMIT 1)";
        Assert.True(await Db.ScalarAsync<bool>($"""
            SELECT count(*) = 1000 AND array_agg(DISTINCT recurring_id ORDER BY recurring_id) = ARRAY['r00','r01','r02','r03','r04','r05','r06','r07','r08','r09']
            FROM {"{schema}"}.jobs WHERE xmin::text = {FirstTx}
            """));
        // Не поместившиеся строки сработали следующей транзакцией той же итерации — первыми по wake_at, по 100 срабатываний.
        Assert.True(await Db.ScalarAsync<bool>($"""
            SELECT count(DISTINCT xmin::text) = 1 AND min(xmin::text) <> {FirstTx} AND count(*) = 500
            FROM (SELECT xmin, row_number() OVER (PARTITION BY recurring_id ORDER BY scheduled_at) AS n
                  FROM {"{schema}"}.jobs WHERE recurring_id IN ('r10', 'r11', 'r12', 'r13', 'r14')) j
            WHERE n <= 100
            """));
        // Итерация продолжалась, пока пакет полный: все строки догнали пропущенное без пропусков и дублей.
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT bool_and(r.wake_at = r.next_fire_at AND r.next_fire_at = r.last_fire_at + interval '1 minute'
                            AND j.n = j.d AND j.span = (j.n - 1) * interval '1 minute' AND j.n >= 150)
            FROM {schema}.recurring_jobs r
            JOIN (SELECT recurring_id, count(*) AS n, count(DISTINCT scheduled_at) AS d, max(scheduled_at) - min(scheduled_at) AS span
                  FROM {schema}.jobs GROUP BY recurring_id) j ON j.recurring_id = r.id
            """));
        Assert.Equal(15L, await CountAsync("SELECT count(DISTINCT recurring_id) FROM {schema}.jobs"));
    }
}

public sealed class RecurringBudgetTests_Direct(PostgresFixture pg) : RecurringBudgetTests(pg, ConnectionMode.Direct);
public sealed class RecurringBudgetTests_Bouncer(PostgresFixture pg) : RecurringBudgetTests(pg, ConnectionMode.Bouncer);
