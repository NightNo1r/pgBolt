using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class RecurringSyncTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private static readonly ILogger Log = NullLogger.Instance;

    private static RecurringDefinition Def(string id, string cron = "0 3 * * *", string args = """{"N":1}""",
        RecurringJobOptions? options = null) =>
        RecurringDefinition.Create(id, "rec.job", args, cron, options);

    private Task<bool> RowAsync(string id, string condition) =>
        Db.ScalarAsync<bool>($"SELECT {condition} FROM {{schema}}.recurring_jobs WHERE id = @id", ("id", id));

    private Task<string[]> IdsAsync(string owner) =>
        Db.ScalarAsync<string[]>("SELECT coalesce(array_agg(id ORDER BY id), '{}') FROM {schema}.recurring_jobs WHERE owner = @o", ("o", owner));

    [Fact]
    public async Task Inserts_declared_rows_with_owner_and_cursor()
    {
        var result = await Store.SyncRecurringAsync("svc", [Def("a"), Def("b")], Log, Ct);

        Assert.Equal(new[] { "a", "b" }, result.Inserted);
        Assert.Equal(2, result.Writes);
        Assert.Equal(new[] { "a", "b" }, await IdsAsync("svc"));
        Assert.True(await RowAsync("a", "next_fire_at > now() AND wake_at = next_fire_at AND NOT paused"));
    }

    [Fact]
    public async Task Second_sync_without_changes_writes_nothing()
    {
        var options = new RecurringJobOptions
        {
            RetryDelays = [TimeSpan.FromSeconds(1.5)], CatchUpWindow = TimeSpan.FromDays(1) + TimeSpan.FromTicks(5),
        };
        await Store.SyncRecurringAsync("svc", [Def("a", args: """{"a":1,"b":2}""", options: options)], Log, Ct);
        var updatedAt = await Db.ScalarAsync<DateTime>("SELECT updated_at FROM {schema}.recurring_jobs WHERE id = 'a'");

        var result = await Store.SyncRecurringAsync("svc", [Def("a", args: """{ "b": 2, "a": 1 }""", options: options)], Log, Ct);

        Assert.Equal(0, result.Writes);
        Assert.Equal(updatedAt, await Db.ScalarAsync<DateTime>("SELECT updated_at FROM {schema}.recurring_jobs WHERE id = 'a'"));
    }

    [Fact]
    public async Task Changed_args_update_row_and_keep_cursor()
    {
        await Store.SyncRecurringAsync("svc", [Def("a")], Log, Ct);
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET next_fire_at = '2099-01-01', wake_at = '2099-01-01', paused = true");

        var result = await Store.SyncRecurringAsync("svc", [Def("a", args: """{"N":2}""")], Log, Ct);

        Assert.Equal("args", Assert.Single(Assert.Single(result.Updated).Fields));
        Assert.True(await RowAsync("a", """next_fire_at = '2099-01-01' AND paused AND args = '{"N":2}'"""));
    }

    [Fact]
    public async Task Unreadable_stored_values_are_overwritten_with_declared()   // §6.2: SQL-правка в обход API не ломает синхронизацию
    {
        var options = new RecurringJobOptions { RetryDelays = [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)] };
        await Store.SyncRecurringAsync("svc", [Def("a", options: options), Def("b", options: options)], Log, Ct);
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET catch_up_window = interval '1 month' WHERE id = 'a'");
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET retry_delays = ARRAY[1, NULL]::int[] WHERE id = 'b'");
        await Db.ExecAsync("INSERT INTO {schema}.recurring_jobs (id, type, args, cron, time_zone, misfire, catch_up_window, overlap, " +
                           "created_at, updated_at) VALUES ('api', 'rec.job', '{}', '0 3 * * *', 'UTC', 0, interval '1 year', 0, now(), now())");

        var result = await Store.SyncRecurringAsync("svc", [Def("a", options: options), Def("b", options: options), Def("api")], Log, Ct);

        Assert.Equal(new[] { ("a", "catch_up_window"), ("b", "retry_delays") },
            result.Updated.Select(u => (u.Id, Assert.Single(u.Fields))).OrderBy(u => u.Id, StringComparer.Ordinal));
        Assert.Equal("api", Assert.Single(result.Collisions));                 // чужая строка с месяцами в interval не роняет проход
        Assert.True(await RowAsync("a", "catch_up_window = interval '1 day'"));
        Assert.True(await RowAsync("b", "retry_delays = ARRAY[1, 2]"));
        Assert.Equal(0, (await Store.SyncRecurringAsync("svc", [Def("a", options: options), Def("b", options: options)], Log, Ct)).Writes);
    }

    [Fact]
    public async Task Changed_cron_recalculates_cursor_after_last_fire()
    {
        await Store.SyncRecurringAsync("svc", [Def("a", cron: "* * * * *")], Log, Ct);
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET last_fire_at = date_trunc('minute', now()) + interval '2 minutes'");

        var result = await Store.SyncRecurringAsync("svc", [Def("a", cron: "*/2 * * * *")], Log, Ct);

        Assert.Contains("cron", Assert.Single(result.Updated).Fields);
        Assert.True(await RowAsync("a", "next_fire_at > last_fire_at AND wake_at = next_fire_at"));
    }

    [Fact]
    public async Task Undeclared_rows_of_owner_are_deleted_and_others_untouched()
    {
        await Store.SyncRecurringAsync("svc", [Def("a"), Def("b")], Log, Ct);
        await Store.SyncRecurringAsync("other", [Def("x")], Log, Ct);

        Assert.Equal(new[] { "b" }, (await Store.SyncRecurringAsync("svc", [Def("a")], Log, Ct)).Deleted);
        Assert.Equal(new[] { "a" }, (await Store.SyncRecurringAsync("svc", [], Log, Ct)).Deleted);   // ClearRecurring()

        Assert.Empty(await IdsAsync("svc"));
        Assert.Equal(new[] { "x" }, await IdsAsync("other"));
    }

    [Fact]
    public async Task Api_and_foreign_rows_are_collisions()
    {
        await Store.SyncRecurringAsync("other", [Def("x")], Log, Ct);
        await Store.UpsertRecurringAsync(Def("api"), Ct);

        var result = await Store.SyncRecurringAsync("svc", [Def("x", cron: "0 5 * * *"), Def("api", cron: "0 5 * * *")], Log, Ct);

        Assert.Equal(new[] { "api", "x" }, result.Collisions.Order());
        Assert.Equal(0, result.Writes);
        Assert.True(await RowAsync("x", "owner = 'other' AND cron = '0 3 * * *'"));
        Assert.True(await RowAsync("api", "owner IS NULL AND cron = '0 3 * * *'"));
    }

    [Fact]
    public async Task Parallel_syncs_of_one_owner_are_serialized()
    {
        RecurringDefinition[] defs = [Def("a"), Def("b"), Def("c")];

        await Task.WhenAll(Enumerable.Range(0, 5).Select(_ => Store.SyncRecurringAsync("svc", defs, Log, Ct)));

        Assert.Equal(new[] { "a", "b", "c" }, await IdsAsync("svc"));
    }

    [Fact]
    public async Task Cross_owner_collisions_in_reverse_order_do_not_deadlock()   // Review Focus 4; §10: без 40P01
    {
        // Без повтора при 40P01 (retryOnConflict: false — одна попытка): deadlock вышел бы исключением, а не погасился
        // повтором, после которого проигравший увидел бы закоммиченные строки как коллизии.
        var store = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: false);
        var log = new ListLogger<RecurringSyncTests>();
        RecurringDefinition[] forward = [Def("p"), Def("q"), Def("r"), Def("s")];
        RecurringDefinition[] backward = [.. forward.Reverse()];
        for (var i = 0; i < 20; i++)
        {
            await Db.ExecAsync("DELETE FROM {schema}.recurring_jobs");

            var results = await Task.WhenAll(
                store.SyncRecurringAsync("A", forward, log, Ct),
                store.SyncRecurringAsync("B", backward, log, Ct));

            Assert.Equal(4, results[0].Inserted.Count + results[1].Inserted.Count);
            Assert.Equal(results[0].Inserted.Order(), results[1].Collisions.Order());
            Assert.Equal(4L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.recurring_jobs WHERE owner IN ('A', 'B')"));
        }
        Assert.DoesNotContain(log.Entries, e => e.Message.Contains("40P01"));
    }

    [Fact]
    public async Task Cron_change_while_tick_holds_row_fires_once()         // §10: смена cron при удерживаемом тиком захвате; Review Focus 3
    {
        await RecurringTestSupport.PublishAsync(Store, Ct);
        var allow = new RecurringJobOptions { Overlap = OverlapPolicy.Allow };
        await Store.SyncRecurringAsync("svc", [Def("a", cron: "* * * * *", options: allow)], Log, Ct);
        await Db.ExecAsync("""
            UPDATE {schema}.recurring_jobs
            SET next_fire_at = date_trunc('minute', now()) - interval '1 minute', wake_at = now() - interval '1 second'
            WHERE id = 'a'
            """);
        var held = await RecurringTestSupport.HoldTickAsync(Store, Ct);     // тик поставил срабатывание X, COMMIT не выполнен

        // Новый cron с тем же множеством моментов: X совпадает с его срабатыванием; синхронизация ждёт FOR UPDATE строки.
        var sync = Store.SyncRecurringAsync("svc", [Def("a", cron: "0 * * * * *", options: allow)], Log, Ct);
        await RecurringTestSupport.WaitForRowLockWaiterAsync(Db, Ct);
        Assert.False(sync.IsCompleted);

        Assert.Equal(1, (await held.ReleaseAsync()).Fired);
        Assert.Contains("cron", Assert.Single((await sync).Updated).Fields);

        Assert.True(await RowAsync("a", "cron = '0 * * * * *' AND last_fire_at IS NOT NULL " +
                                        "AND next_fire_at > last_fire_at AND wake_at = next_fire_at"));
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET wake_at = now() WHERE id = 'a'");
        await Store.TickRecurringAsync(100, 1000, TimeSpan.FromMinutes(1), Ct);   // X второй раз не ставится
        Assert.Equal(1L, await Db.ScalarAsync<long>("""
            SELECT count(*) FROM {schema}.jobs WHERE recurring_id = 'a'
              AND scheduled_at = (SELECT min(scheduled_at) FROM {schema}.jobs WHERE recurring_id = 'a')
            """));
    }
}

public sealed class RecurringSyncTests_Direct(PostgresFixture pg) : RecurringSyncTests(pg, ConnectionMode.Direct);
public sealed class RecurringSyncTests_Bouncer(PostgresFixture pg) : RecurringSyncTests(pg, ConnectionMode.Bouncer);
