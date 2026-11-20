using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using PgBolt.Dashboard.Data;
using PgBolt.Dashboard.Tests.Support;
using PgBolt.Dashboard.Views;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Data;

public abstract class OverviewStoreTests(PostgresFixture pg, ConnectionMode mode) : DashboardTestBase(pg, mode)
{
    internal static readonly Guid Live = Guid.Parse("00000000-0000-0000-0000-00000000000a");
    internal static readonly Guid Dead = Guid.Parse("00000000-0000-0000-0000-00000000000d");

    /// <summary>
    /// queue_stats двух очередей, stats_minutes в окне часа (минута minute и minute − 58: обе остаются в окне, даже если к чтению
    /// сменится минута) и вне его, живой и мёртвый серверы, свежая строка stats.
    /// </summary>
    internal static Task SeedOverviewAsync(TestDb db, DateTime minute) => db.ExecAsync("""
        INSERT INTO {schema}.queue_stats VALUES ('a', 1, 2, 3, 4, now()), ('b', 10, 20, 30, 40, now());
        INSERT INTO {schema}.stats_minutes VALUES
            (@minute, @live, 'a', 1, 2, 3),
            (@minute, @dead, 'a', 10, 20, 30),
            (@minute - interval '58 minutes', @live, 'a', 100, 0, 0),
            (@minute - interval '61 minutes', @live, 'a', 1000, 0, 0);
        INSERT INTO {schema}.servers VALUES
            (@live, 'alpha', 'h1', '{a,b}', 4, now() - interval '1 hour', now()),
            (@dead, 'beta', 'h2', '{c}', 2, now() - interval '1 hour', now() - interval '2 minutes');
        INSERT INTO {schema}.maintenance_tasks (name, next_run_at, last_run_at) VALUES ('stats', now() + interval '8 seconds', now());
        """, ("live", Live), ("dead", Dead), ("minute", minute));

    /// <summary>retention идёт (finished раньше run, срок в будущем), partitions завершена.</summary>
    internal static Task SeedMaintenanceAsync(TestDb db) => db.ExecAsync("""
        INSERT INTO {schema}.maintenance_tasks VALUES
            ('retention', now() + interval '1 hour', now(), now() - interval '1 hour', NULL),
            ('partitions', now() + interval '1 hour', now() - interval '1 minute', now(), 'disk full');
        """);

    internal static Task<DateTime> DbMinuteAsync(TestDb db) => db.ScalarAsync<DateTime>("SELECT date_trunc('minute', now())");

    private Task<DateTime> DbMinuteAsync() => DbMinuteAsync(Db);

    [Fact]
    public async Task Overview_of_empty_schema_reports_not_running_and_no_data()
    {
        var before = await DbMinuteAsync();
        var overview = await Store.GetOverviewAsync(Ct);
        var after = await DbMinuteAsync();

        Assert.Equal(new QueueTotals(0, 0, 0, 0), overview.Totals);
        Assert.Equal(0, overview.LiveServers);
        Assert.Equal(StatsFreshnessKind.NotRunning, overview.Freshness.Kind);
        Assert.Equal(60, overview.LastHour.Count);
        Assert.All(overview.LastHour, p => Assert.Equal((0L, 0L, 0L), (p.Succeeded, p.Failed, p.Retried)));
        Assert.InRange(overview.LastHour[^1].BucketUtc, before, after);
        Assert.Equal(overview.LastHour[^1].BucketUtc.AddMinutes(-59), overview.LastHour[0].BucketUtc);
    }

    [Fact]
    public async Task Overview_sums_queue_stats_and_last_hour_buckets()
    {
        var minute = await DbMinuteAsync();
        await SeedOverviewAsync(Db, minute);

        var overview = await Store.GetOverviewAsync(Ct);

        Assert.Equal(new QueueTotals(11, 22, 33, 44), overview.Totals);
        Assert.Equal(1, overview.LiveServers);
        Assert.Equal(new StatsFreshness(StatsFreshnessKind.Fresh, null), overview.Freshness);
        Assert.Equal(60, overview.LastHour.Count);
        Assert.Equal(new ThroughputPoint(minute.AddMinutes(-58), 100, 0, 0), overview.LastHour.Single(p => p.BucketUtc == minute.AddMinutes(-58)));
        Assert.Equal(new ThroughputPoint(minute, 11, 22, 33), overview.LastHour.Single(p => p.BucketUtc == minute));
        Assert.Equal(111L, overview.LastHour.Sum(p => p.Succeeded));
    }

    [Fact]
    public async Task Fresh_stats_with_empty_queue_stats_has_no_banner()
    {
        await Db.ExecAsync("INSERT INTO {schema}.maintenance_tasks (name, next_run_at, last_run_at) VALUES ('stats', now() + interval '8 seconds', now())");

        var overview = await Store.GetOverviewAsync(Ct);

        Assert.Equal(new QueueTotals(0, 0, 0, 0), overview.Totals);
        Assert.Equal(new StatsFreshness(StatsFreshnessKind.Fresh, null), overview.Freshness);
    }

    [Fact]
    public async Task Throughput_day_has_96_quarter_hour_points()
    {
        // Слот засева — от одного now(); к чтению 15-минутка может смениться: точка ищется по времени, а не по индексу.
        var slot = await Db.ScalarAsync<DateTime>("SELECT date_bin('15 minutes', now(), timestamptz '2000-01-01') - interval '15 minutes'");
        await Db.ExecAsync("""
            INSERT INTO {schema}.stats_minutes VALUES
                (@slot, @live, 'a', 1, 2, 3),
                (@slot + interval '1 minute', @live, 'b', 10, 20, 30);
            """, ("live", Live), ("slot", slot));

        var day = await Store.GetThroughputDayAsync(Ct);

        Assert.Equal(96, day.Count);
        Assert.Equal(new ThroughputPoint(slot, 11, 22, 33), day.Single(p => p.BucketUtc == slot));
        Assert.Equal(day[95].BucketUtc.AddMinutes(-15 * 95), day[0].BucketUtc);
        Assert.Equal(11L, day.Sum(p => p.Succeeded));
    }

    [Fact]
    public async Task Queues_union_snapshot_and_served_queues()
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue_stats VALUES ('a', 1, 2, 3, 4, now());
            INSERT INTO {schema}.servers VALUES
                (@live, 'alpha', 'h1', '{a,b}', 4, now(), now()),
                (@dead, 'beta', 'h2', '{c}', 2, now(), now() - interval '2 minutes');
            """, ("live", Live), ("dead", Dead));

        var queues = await Store.ListQueuesAsync(Ct);

        Assert.Equal(2, queues.Count);
        Assert.Equal(("a", 1L, 2L, 3L, 4L, 1), (queues[0].Name, queues[0].Scheduled, queues[0].Enqueued, queues[0].Processing,
            queues[0].Awaiting, queues[0].Servers));
        Assert.NotNull(queues[0].MeasuredAt);
        Assert.Equal(new QueueRow("b", 0, 0, 0, 0, 1, null), queues[1]);
    }

    [Fact]
    public async Task Override_only_queue_is_listed_and_resettable()
    {
        var manager = Services.GetRequiredService<IQueueSettingsManager>();
        await Db.ExecAsync("INSERT INTO {schema}.queue_settings (queue, default_max_concurrency, default_weight) VALUES ('code', 3, 2)");
        await manager.SetConcurrencyOverrideAsync("limited", 0, "tests", Ct);
        await manager.SetWeightOverrideAsync("weighted", 5, "tests", Ct);

        var queues = await Store.ListQueuesAsync(Ct);

        Assert.Equal([new QueueRow("limited", 0, 0, 0, 0, 0, null), new QueueRow("weighted", 0, 0, 0, 0, 0, null)], queues);

        await manager.ResetConcurrencyOverrideAsync("limited", Ct);
        await manager.ResetWeightOverrideAsync("weighted", Ct);

        Assert.Empty(await Store.ListQueuesAsync(Ct));
    }

    [Fact]
    public async Task Leased_rows_are_counted_per_requested_queue()
    {
        await SeedJobsAsync(3, "dash.job", "a", 0, "enqueued");
        await SeedJobsAsync(1, "dash.job", "b", 0, "enqueued");
        await SeedJobsAsync(1, "dash.job", "c", 0, "enqueued");
        await LeaseAsync("a");
        await LeaseAsync("a");
        await LeaseAsync("c");

        var leased = await Store.CountLeasedAsync(["a", "b"], Ct);

        Assert.Equal([KeyValuePair.Create("a", 2)], leased);
    }

    [Fact]
    public async Task Servers_with_hour_stats_and_maintenance_tasks()
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.servers VALUES
                (@live, 'alpha', 'h1', '{a,b}', 4, now() - interval '1 hour', now()),
                (@dead, 'beta', 'h2', '{c}', 2, now() - interval '1 hour', now() - interval '2 minutes');
            INSERT INTO {schema}.stats_minutes VALUES
                (date_trunc('minute', now()), @live, 'a', 1, 2, 3),
                (date_trunc('minute', now()) - interval '30 minutes', @live, 'b', 10, 20, 30),
                (date_trunc('minute', now()) - interval '2 hours', @live, 'a', 1000, 1000, 1000);
            """, ("live", Live), ("dead", Dead));
        await SeedMaintenanceAsync(Db);

        var data = await Store.GetServersAsync(Ct);

        Assert.Equal(["alpha", "beta"], data.Servers.Select(s => s.Name));
        var alpha = data.Servers[0];
        Assert.Equal((Live, "h1", 4, false, 11L, 22L, 33L), (alpha.Id, alpha.Host, alpha.Capacity, alpha.Stale, alpha.Succeeded, alpha.Failed, alpha.Retried));
        Assert.Equal(["a", "b"], alpha.Queues);
        Assert.Equal(DateTimeKind.Utc, alpha.HeartbeatAt.Kind);
        var beta = data.Servers[1];
        Assert.Equal((true, 0L, 0L, 0L), (beta.Stale, beta.Succeeded, beta.Failed, beta.Retried));

        Assert.Equal(["partitions", "retention"], data.Tasks.Select(t => t.Name));
        Assert.False(data.Tasks[0].Running);
        Assert.Equal("disk full", data.Tasks[0].LastError);
        Assert.True(data.Tasks[1].Running);
        Assert.Null(data.Tasks[1].LastError);
        Assert.NotNull(data.Tasks[1].LastFinishedAt);
    }

    [Fact]
    public async Task Job_types_are_listed_by_name_and_found_by_name()
    {
        await PublishTypeAsync("b.job", "q2");
        await PublishTypeAsync("a.job", schemaJson: """{"type":"object","properties":{"n":{"type":"integer"}}}""");

        var types = await Store.ListJobTypesAsync(Ct);

        Assert.Equal(["a.job", "b.job"], types.Select(t => t.Name));
        var a = types[0];
        Assert.Equal(("default", 3, (short)0, (int[]?)null, "tests"), (a.DefaultQueue, a.DefaultMaxAttempts, a.DefaultPriority,
            a.DefaultRetryDelaysSeconds, a.PublishedBy));
        Assert.Contains("\"integer\"", a.ArgsSchemaJson);
        Assert.Equal("q2", types[1].DefaultQueue);
        Assert.Equivalent(a, await Store.GetJobTypeAsync("a.job", Ct));
        Assert.Null(await Store.GetJobTypeAsync("nope", Ct));
    }

    [Fact]
    public async Task Statements_run_read_only_with_statement_timeout()
    {
        var store = new DashboardStore(Db.DataSource, Db.Schema, usePreparedStatements: false,
            new PgBoltDashboardOptions { QueryTimeout = TimeSpan.FromMilliseconds(50) }, Time);
        static async Task<int> ReadOne(NpgsqlDataReader r, CancellationToken ct)
        {
            await r.ReadAsync(ct);
            return 1;
        }

        var timeout = await Assert.ThrowsAsync<PostgresException>(() => store.ReadAsync("SELECT pg_sleep(1)", _ => { }, ReadOne, Ct));
        var readOnly = await Assert.ThrowsAsync<PostgresException>(() => store.ReadAsync(
            Db.Sql("INSERT INTO {schema}.queue_stats VALUES ('x', 0, 0, 0, 0, now()) RETURNING 1"), _ => { }, ReadOne, Ct));

        Assert.Equal("57014", timeout.SqlState);
        Assert.Equal("25006", readOnly.SqlState);
    }
}

public sealed class OverviewStoreTests_Direct(PostgresFixture pg) : OverviewStoreTests(pg, ConnectionMode.Direct);
public sealed class OverviewStoreTests_Bouncer(PostgresFixture pg) : OverviewStoreTests(pg, ConnectionMode.Bouncer);
