using Npgsql;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class QueueSettingsSyncTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private const string Me = "me/2";
    private const string Other = "other/1";

    private Task SyncAsync(IReadOnlyList<QueueDefault> defaults) => Store.SyncQueueDefaultsAsync(defaults, Me, Ct);

    private Task SeedDefaultAsync(string queue, int? limit, int? weight, string publishedBy, TimeSpan age = default) =>
        Db.ExecAsync("""
            INSERT INTO {schema}.queue_settings (queue, default_max_concurrency, default_weight, default_published_by, default_updated_at)
            VALUES (@q, @l, @w, @p, now() - @age)
            """, ("q", queue), ("l", limit), ("w", weight), ("p", publishedBy), ("age", age));

    private async Task<List<(string Queue, int? Limit, int? Weight)>> DefaultsAsync()
    {
        await using var cmd = Db.Admin.CreateCommand(Db.Sql(
            "SELECT queue, default_max_concurrency, default_weight FROM {schema}.queue_settings ORDER BY queue"));
        await using var r = await cmd.ExecuteReaderAsync(Ct);
        var list = new List<(string, int?, int?)>();
        while (await r.ReadAsync(Ct))
            list.Add((r.GetString(0), r.GetFieldValue<int?>(1), r.GetFieldValue<int?>(2)));
        return list;
    }

    [Fact]
    public async Task Sync_creates_row_when_any_default_set()
    {
        await SyncAsync([new("a", 5, null), new("b", null, 3), new("c", null, null)]);

        Assert.Equal([("a", 5, (int?)null), ("b", null, 3)], await DefaultsAsync());
    }

    [Fact]
    public async Task Sync_nulls_existing_row_and_keeps_overrides()
    {
        await SeedDefaultAsync("a", 5, 2, Other);
        await Db.ExecAsync("""
            UPDATE {schema}.queue_settings
            SET override_concurrency_set = true, override_max_concurrency = 0, override_weight_set = true, override_weight = 7
            WHERE queue = 'a'
            """);

        await SyncAsync([new("a", null, null)]);

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT default_max_concurrency IS NULL AND default_weight IS NULL AND default_published_by = @p
                   AND override_concurrency_set AND override_max_concurrency = 0 AND override_weight_set AND override_weight = 7
            FROM {schema}.queue_settings WHERE queue = 'a'
            """, ("p", Me)));
    }

    [Fact]
    public async Task Sync_reports_fresh_conflict_from_other_publisher()
    {
        await SeedDefaultAsync("a", 5, 2, Other);

        var conflict = Assert.Single(await Store.SyncQueueDefaultsAsync([new("a", 6, 2)], Me, Ct));

        Assert.Equal(new QueueDefaultConflict("a", 5, 2, Other), conflict);
    }

    [Fact]
    public async Task Sync_reports_nothing_for_stale_previous_publication()
    {
        await SeedDefaultAsync("a", 5, 2, Other, age: TimeSpan.FromMinutes(6));

        Assert.Empty(await Store.SyncQueueDefaultsAsync([new("a", 6, 2)], Me, Ct));
    }

    [Fact]
    public async Task Sync_reports_nothing_for_same_publisher()
    {
        await SeedDefaultAsync("a", 5, 2, Me);

        Assert.Empty(await Store.SyncQueueDefaultsAsync([new("a", 6, 2)], Me, Ct));
    }

    [Fact]
    public async Task Sync_reports_nothing_for_equal_values()
    {
        await SeedDefaultAsync("a", 5, 2, Other);

        Assert.Empty(await Store.SyncQueueDefaultsAsync([new("a", 5, 2)], Me, Ct));
    }

    [Fact]
    public async Task Sync_into_override_created_row_reports_nothing()
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue_settings (queue, override_concurrency_set, override_max_concurrency)
            VALUES ('a', true, 3)
            """);

        Assert.Empty(await Store.SyncQueueDefaultsAsync([new("a", 6, 2)], Me, Ct));
    }

    [Fact]
    public async Task Sync_with_empty_list_does_not_touch_database()
    {
        // Порт 1 закрыт: любое обращение к БД упало бы.
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Timeout=1");
        var store = new PgBoltStore(unreachable, Db.Schema, usePreparedStatements: false);

        Assert.Empty(await store.SyncQueueDefaultsAsync([], Me, Ct));
    }

    [Fact]
    public async Task Active_settings_follow_effective_values()
    {
        await SeedDefaultAsync("weight-only", null, 4, Me);
        await SeedDefaultAsync("unlimited-over-code", 5, 4, Me);
        await Db.ExecAsync("UPDATE {schema}.queue_settings SET override_concurrency_set = true WHERE queue = 'unlimited-over-code'");
        await SeedDefaultAsync("unweighted-over-code", 5, 4, Me);
        await Db.ExecAsync("UPDATE {schema}.queue_settings SET override_weight_set = true WHERE queue = 'unweighted-over-code'");
        await SeedDefaultAsync("paused", null, 4, Me);
        await Db.ExecAsync("UPDATE {schema}.queue_settings SET override_concurrency_set = true, override_max_concurrency = 0 WHERE queue = 'paused'");
        await SeedDefaultAsync("both-null", 5, 4, Me);
        await Db.ExecAsync("UPDATE {schema}.queue_settings SET override_concurrency_set = true, override_weight_set = true WHERE queue = 'both-null'");

        var active = await Store.GetActiveQueueSettingsAsync(Ct);

        Assert.Equal(
        [
            new ActiveQueueSetting("paused", 0, 4),
            new ActiveQueueSetting("unlimited-over-code", null, 4),
            new ActiveQueueSetting("unweighted-over-code", 5, null),
            new ActiveQueueSetting("weight-only", null, 4),
        ], active);
    }

    [Fact]
    public async Task Active_settings_return_effective_limit()
    {
        await SeedDefaultAsync("from-code", 5, null, Me);
        await SeedDefaultAsync("overridden", 5, null, Me);
        await Db.ExecAsync("UPDATE {schema}.queue_settings SET override_concurrency_set = true, override_max_concurrency = 2 WHERE queue = 'overridden'");
        await SeedDefaultAsync("paused", 5, null, Me);
        await Db.ExecAsync("UPDATE {schema}.queue_settings SET override_concurrency_set = true, override_max_concurrency = 0 WHERE queue = 'paused'");
        await SeedDefaultAsync("weight-only", null, 3, Me);

        var active = await Store.GetActiveQueueSettingsAsync(Ct);

        Assert.Equal(
            [("from-code", (int?)5), ("overridden", 2), ("paused", 0), ("weight-only", null)],
            active.Select(a => (a.Queue, a.Limit)));
    }
}

public sealed class QueueSettingsSyncTests_Direct(PostgresFixture pg) : QueueSettingsSyncTests(pg, ConnectionMode.Direct);
public sealed class QueueSettingsSyncTests_Bouncer(PostgresFixture pg) : QueueSettingsSyncTests(pg, ConnectionMode.Bouncer);
