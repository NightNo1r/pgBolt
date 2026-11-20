using Npgsql;
using PgBolt.Postgres;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.E2E.Tests;

public abstract class SlimDataSourceTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    /// <summary>Минимальный набор, который спека §1 требует от пользователя.</summary>
    internal static void MinimalSlim(NpgsqlSlimDataSourceBuilder builder) => builder.EnableArrays();

    private static readonly Guid Server = Guid.NewGuid();
    private TestDb _db = null!;
    private NpgsqlDataSource _slim = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        _db = await TestDb.CreateAsync(pg, mode, migrate: false);
        var builder = new NpgsqlSlimDataSourceBuilder(pg.ConnectionString(mode));
        MinimalSlim(builder);
        _slim = builder.Build();
    }

    public async ValueTask DisposeAsync()
    {
        await _slim.DisposeAsync();
        await _db.DisposeAsync();
    }

    [Fact]
    public async Task Every_store_operation_works_on_slim_data_source()
    {
        await new MigrationRunner(_slim, _db.Schema).MigrateAsync(Ct);
        Assert.False((await new PartitionManager(_slim, _db.Schema, 80, TimeSpan.FromSeconds(2)).EnsureAsync(1, Ct)).TimedOut);
        var store = new PgBoltStore(_slim, _db.Schema, usePreparedStatements: false);
        var lease = TimeSpan.FromMinutes(1);

        EnqueueBatch One(string? key = null, TimeSpan? delay = null, int max = 3) =>
            new("slim.job", "slim", 1, max, [1, 2], "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                delay ?? TimeSpan.Zero, [Guid.CreateVersion7()], ["""{"a":1}"""], [null], [key]);

        var a = (await store.EnqueueAsync(One("k"), null, Ct))[0].Id!.Value;
        Assert.False((await store.EnqueueAsync(One("k"), null, Ct))[0].Created);
        var delayed = (await store.EnqueueAsync(One(delay: TimeSpan.FromHours(1)), null, Ct))[0].Id!.Value;
        Assert.Equal("slim", await store.TriggerAsync(delayed, Ct));
        await store.PromoteDueAsync(100, Ct);

        var leased = await store.DequeueAsync("slim", 10, Server, lease, Ct);
        Assert.Equal(2, leased.Count);
        Assert.Equal(2, (await store.HeartbeatAsync(Server, leased.Select(j => new LeaseRef(j.Id, j.LeaseId)).ToArray(), lease, Ct)).Leases.Count);
        Assert.Empty((await store.HeartbeatAsync(Server, [], lease, Ct)).Leases);
        var first = leased.Single(j => j.Id == a);
        var second = leased.Single(j => j.Id == delayed);
        Assert.Equal(FailOutcome.Retried, await store.RetryAsync(a, first.LeaseId, Server, TimeSpan.Zero, JobEventKind.Retry, "m", "e", Ct));
        Assert.Single(await store.AckAsync(Server, [new LeaseRef(delayed, second.LeaseId)], Ct));

        var again = Assert.Single(await store.DequeueAsync("slim", 10, Server, lease, Ct));
        Assert.Equal(FailOutcome.Failed, await store.FailAsync(a, again.LeaseId, Server, JobEventKind.Failed, null, null, Ct));
        Assert.Equal(RequeueResult.Requeued, (await store.RequeueAsync(a, Ct)).Result);
        Assert.Equal(CancelResult.Cancelled, await store.CancelAsync(a, "stop", Ct));
        Assert.Equal(0, await store.SweepExpiredLeasesAsync(100, Ct));

        var info = await store.GetJobAsync(a, Ct);
        Assert.Equal(JobState.Cancelled, info!.State);
        Assert.NotEmpty(info.Events);

        await store.PublishJobTypesAsync([new JobTypeRecord("slim.job", """{"type":"object"}""", "slim", 3, 1, [1])], "slim", Ct);
        Assert.NotNull(await store.GetJobTypeAsync("slim.job", Ct));

        var fresh = (await store.EnqueueAsync(One(), null, Ct))[0].Id!.Value;
        var toRelease = Assert.Single(await store.DequeueAsync("slim", 10, Server, lease, Ct));
        Assert.Equal(fresh, toRelease.Id);
        Assert.Equal(ReleaseOutcome.Released, await store.ReleaseAsync(fresh, toRelease.LeaseId, Server, ran: false, "slim", Ct));

        await store.RegisterServerAsync(Server, "slim", "slim-host", ["slim", "other"], 2, Ct);
        Assert.Equal(new[] { "slim.job" }, await store.GetJobTypeNamesByQueuesAsync(["slim", "other"], Ct));
        await store.RemoveServerAsync(Server, Ct);
    }

    [Fact]
    public async Task Maintenance_operations_work_on_slim_data_source()
    {
        await new MigrationRunner(_slim, _db.Schema).MigrateAsync(Ct);
        Assert.False((await new PartitionManager(_slim, _db.Schema, 80, TimeSpan.FromSeconds(2)).EnsureAsync(1, Ct)).TimedOut);
        var store = new PgBoltStore(_slim, _db.Schema, usePreparedStatements: false);

        var claimedAt = await store.TryClaimMaintenanceAsync("retention", TimeSpan.FromHours(1), Ct);
        Assert.True(await store.CompleteMaintenanceAsync("retention", claimedAt!.Value, null, TimeSpan.FromMinutes(1), Ct));
        Assert.Equal(0, await store.RemoveDeadServersAsync(TimeSpan.FromMinutes(30), Ct));
        await store.RegisterServerAsync(Server, "s", "h", ["slim"], 1, Ct);
        Assert.True((await store.HeartbeatAsync(Server, [], TimeSpan.FromMinutes(1), Ct)).ServerPresent);

        var today = await _db.ScalarAsync<DateOnly>("SELECT (now() AT TIME ZONE 'UTC')::date");
        var day = today.AddDays(-10);
        var suffix = day.ToString("yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
        await _db.ExecAsync($$"""
            CREATE TABLE {schema}.jobs_{{suffix}} (LIKE {schema}.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS) WITH (fillfactor = 80);
            ALTER TABLE {schema}.jobs ATTACH PARTITION {schema}.jobs_{{suffix}}
                FOR VALUES FROM ('{{UuidV7.LowerBound(day)}}') TO ('{{UuidV7.LowerBound(day.AddDays(1))}}');
            """);
        Guid IdOn() => Guid.CreateVersion7(new DateTimeOffset(day.ToDateTime(new TimeOnly(12, 0)), TimeSpan.Zero));
        Guid[] live = [IdOn(), IdOn()];
        await store.EnqueueAsync(new EnqueueBatch("slim.job", "slim", 0, 3, null, null, TimeSpan.Zero, live,
            ["{}", "{}"], [null, null], [null, null]), null, Ct);
        await _db.ExecAsync("""
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, attempt, max_attempts, created_at, finished_at)
            SELECT id, 'slim.job', 'slim', 0, '{}', 2, 1, 3, now(), now() FROM unnest(@ids) AS id
            """, ("ids", Enumerable.Range(0, 150).Select(_ => IdOn()).ToArray()));

        var options = new PgBoltOptions { ConnectionString = "Host=unused", Schema = _db.Schema, JobsRetention = TimeSpan.FromDays(3), EventsRetention = null };
        var r = await new RetentionRunner(_slim, options, RetentionTuning.Default).ApplyAsync(Ct);

        Assert.True(Assert.Single(r.Pinned).Compacted);
        Assert.Equal(2L, await _db.ScalarAsync<long>($"SELECT count(*) FROM {{schema}}.jobs_{suffix}"));
    }
}

public sealed class SlimDataSourceTests_Direct(PostgresFixture pg) : SlimDataSourceTests(pg, ConnectionMode.Direct);
public sealed class SlimDataSourceTests_Bouncer(PostgresFixture pg) : SlimDataSourceTests(pg, ConnectionMode.Bouncer);
