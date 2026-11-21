using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Jobs;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public sealed record HostingArgs(int N);

public sealed class HostingJob : IJob<HostingArgs>
{
    public Task ExecuteAsync(HostingArgs args, JobContext context, CancellationToken cancellationToken) => Task.CompletedTask;
}

[JsonSerializable(typeof(HostingArgs))]
internal sealed partial class HostingJson : JsonSerializerContext;

public abstract class HostingTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, migrate: false);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    private ServiceProvider Build(Action<PgBoltOptions>? extra = null)
    {
        var services = new ServiceCollection();
        services.AddPgBolt(o =>
        {
            o.ConnectionString = pg.ConnectionString(mode);
            o.Schema = _db.Schema;
            extra?.Invoke(o);
        });
        return services.BuildServiceProvider();
    }

    private Task<long> PartitionCountAsync() =>
        _db.ScalarAsync<long>("SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass(@t)", ("t", $"{_db.Schema}.jobs"));

    [Fact]
    public async Task Start_migrates_creates_partitions_and_client_works()
    {
        await using var sp = Build(o => o.AddJobs(b => b.AddExecutor<HostingArgs, HostingJob>(
            new JobDescriptor("hosting.job", "h", 3, 0, null), HostingJson.Default.HostingArgs)));
        foreach (var hosted in sp.GetServices<IHostedService>())
            await hosted.StartAsync(Ct);

        Assert.Equal(8L, await PartitionCountAsync()); // сегодня + 7
        Assert.True(sp.GetRequiredService<JobRegistry>().TryGet("hosting.job", out _));
        var r = await sp.GetRequiredService<IJobClient>().EnqueueAsync(new HostingArgs(1), ct: Ct);
        Assert.True(r.Created);
    }

    [Fact]
    public async Task Ready_completes_after_successful_start()
    {
        await using var sp = Build();
        var initializer = sp.GetRequiredService<PgBoltInitializer>();
        Assert.False(initializer.Ready.IsCompleted);

        await initializer.StartAsync(Ct);

        await initializer.Ready.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.True(initializer.Ready.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Ready_faults_with_start_exception()
    {
        await using var sp = Build(o => o.AutoMigrate = false);
        var initializer = sp.GetRequiredService<PgBoltInitializer>();

        var startError = await Assert.ThrowsAsync<InvalidOperationException>(() => initializer.StartAsync(Ct));

        Assert.True(initializer.Ready.IsFaulted);
        var readyError = await Assert.ThrowsAsync<InvalidOperationException>(() => initializer.Ready);
        Assert.Same(startError, readyError);
        Assert.Contains("schema version", readyError.Message);
    }

    [Fact]
    public async Task Ready_is_cancelled_when_start_token_is_cancelled()
    {
        await using var sp = Build();
        var initializer = sp.GetRequiredService<PgBoltInitializer>();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => initializer.StartAsync(cts.Token));

        Assert.True(initializer.Ready.IsCanceled);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => initializer.Ready);
    }

    [Fact]
    public async Task AutoMigrate_off_on_unmigrated_schema_fails_start()
    {
        await using var sp = Build(o => o.AutoMigrate = false);
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct));
        Assert.Contains("schema version", e.Message);
    }

    [Fact]
    public async Task User_data_source_is_not_disposed_with_container()
    {
        var ds = NpgsqlDataSource.Create(pg.ConnectionString(mode));
        var services = new ServiceCollection();
        services.AddPgBolt(o => { o.DataSource = ds; o.Schema = _db.Schema; });
        await using (var sp = services.BuildServiceProvider())
            await sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct);

        await using var cmd = ds.CreateCommand("SELECT 1");
        Assert.Equal(1, (int)(await cmd.ExecuteScalarAsync(Ct))!);
        await ds.DisposeAsync();
    }

    [Fact]
    public void Second_AddPgBolt_throws()
    {
        var services = new ServiceCollection();
        services.AddPgBolt(o => o.ConnectionString = "Host=x");
        Assert.Throws<InvalidOperationException>(() => services.AddPgBolt(o => o.ConnectionString = "Host=x"));
    }

    [Fact]
    public async Task Ensure_partitions_without_argument_uses_partition_days_ahead()
    {
        await using var sp = Build(o => o.PartitionDaysAhead = 3);

        await sp.GetRequiredService<IPgBoltMaintenance>().EnsurePartitionsAsync(ct: Ct);

        Assert.Equal(4L, await PartitionCountAsync());
    }

    [Fact]
    public async Task Missing_future_partitions_are_retried_in_background()
    {
        await using var sp = Build(o => { o.PartitionLockTimeout = TimeSpan.FromMilliseconds(200); o.PartitionDaysAhead = 3; });
        await sp.GetRequiredService<IPgBoltMaintenance>().EnsurePartitionsAsync(0, Ct); // миграция + сегодня
        var initializer = sp.GetRequiredService<PgBoltInitializer>();

        await using (var holder = await _db.Admin.OpenConnectionAsync(Ct))
        {
            await using (var lk = new NpgsqlCommand(_db.Sql("BEGIN; LOCK TABLE {schema}.jobs IN SHARE UPDATE EXCLUSIVE MODE"), holder))
                await lk.ExecuteNonQueryAsync(Ct);
            await initializer.StartAsync(Ct);
            Assert.NotNull(initializer.BackgroundRetry);
            Assert.True(initializer.Ready.IsCompletedSuccessfully); // фоновый повтор не ждётся (§8.3)
        }

        await initializer.BackgroundRetry!.WaitAsync(TimeSpan.FromSeconds(15), Ct);
        Assert.Equal(4L, await PartitionCountAsync());
        await initializer.StopAsync(Ct);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Newer_schema_version_starts_with_warning(bool autoMigrate)
    {
        await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).MigrateAsync(Ct);
        await _db.ExecAsync("INSERT INTO {schema}.schema_version (version, applied_at) VALUES (@v, now())",
            ("v", PgBolt.Postgres.Schema.PgBoltMigrations.LatestVersion + 1));
        var log = new ListLogger<PgBoltInitializer>();
        var services = new ServiceCollection();
        services.AddSingleton<ILogger<PgBoltInitializer>>(log);
        services.AddPgBolt(o => { o.ConnectionString = pg.ConnectionString(mode); o.Schema = _db.Schema; o.AutoMigrate = autoMigrate; });
        await using var sp = services.BuildServiceProvider();

        await sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct);

        Assert.Contains(log.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("newer"));
        Assert.True(await PartitionCountAsync() > 0);
    }

    [Theory]
    [InlineData(true, 1)]
    [InlineData(false, 1)]
    [InlineData(true, 2)]
    [InlineData(false, 2)]
    public async Task Pre_release_schema_fails_start(bool autoMigrate, int version)   // часть 4 §3, §10
    {
        await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).MigrateAsync(Ct);
        await _db.ExecAsync("DROP TABLE {schema}.recurring_jobs");        // схема прошлой сборки: версия есть, объектов части 4 нет
        // У исторической V002 была maintenance_tasks, но ещё не было функции проверки выдачи.
        await _db.ExecAsync("DROP FUNCTION {schema}.missing_job(uuid), {schema}.read_jobs(uuid[])");
        await _db.ExecAsync("DELETE FROM {schema}.schema_version WHERE version > @v", ("v", version));
        await using var sp = Build(o => o.AutoMigrate = autoMigrate);

        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct));

        Assert.Contains("pre-release", e.Message);
        Assert.Contains($"DROP SCHEMA {_db.Schema} CASCADE", e.Message);
    }

    [Theory]
    [InlineData("batches", true)]
    [InlineData("batches", false)]
    [InlineData("stats_minutes", true)]
    [InlineData("stats_minutes", false)]
    [InlineData("queue_stats", true)]
    [InlineData("queue_stats", false)]
    [InlineData("queue_settings", true)]
    [InlineData("queue_settings", false)]
    public async Task Schema_without_pre_release_tables_fails_start(string table, bool autoMigrate)
    {
        await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).MigrateAsync(Ct);
        await _db.ExecAsync($"DROP TABLE {{schema}}.{table}");      // схема прошлой сборки: версия 1, одной из таблиц нет
        await using var sp = Build(o => o.AutoMigrate = autoMigrate);

        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct));

        Assert.Contains("pre-release", e.Message);
        Assert.Contains($"DROP SCHEMA {_db.Schema} CASCADE", e.Message);
    }

    [Fact]
    public async Task Initializer_rejects_queue_stats_without_new_column()
    {
        await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).MigrateAsync(Ct);
        await _db.ExecAsync("ALTER TABLE {schema}.queue_stats DROP COLUMN oldest_ready_seconds");
        await using var sp = Build();

        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct));

        Assert.Contains("pre-release", e.Message);
    }

    [Fact]
    public async Task Ensure_partitions_migrates_missing_schema()
    {
        await using var sp = Build();

        await sp.GetRequiredService<IPgBoltMaintenance>().EnsurePartitionsAsync(1, Ct);

        Assert.Equal(PgBolt.Postgres.Schema.PgBoltMigrations.LatestVersion,
            await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).GetCurrentVersionAsync(Ct));
        Assert.Equal(2L, await PartitionCountAsync());
    }

    [Fact]
    public async Task Ensure_partitions_works_on_newer_schema()
    {
        await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).MigrateAsync(Ct);
        await _db.ExecAsync("INSERT INTO {schema}.schema_version (version, applied_at) VALUES (@v, now())",
            ("v", PgBolt.Postgres.Schema.PgBoltMigrations.LatestVersion + 1));
        await using var sp = Build();

        var result = await sp.GetRequiredService<IPgBoltMaintenance>().EnsurePartitionsAsync(1, Ct);

        Assert.False(result.TimedOut);
        Assert.Equal(2L, await PartitionCountAsync());
    }

    [Theory]
    [InlineData(true, null, true)]
    [InlineData(true, 1, true)]
    [InlineData(true, 2, true)]
    [InlineData(true, 3, false)]
    [InlineData(true, 4, false)]
    [InlineData(false, null, false)]
    [InlineData(false, 1, false)]
    public void Needs_migration_only_for_missing_or_older_schema(bool autoMigrate, int? version, bool expected) =>
        Assert.Equal(expected, PgBoltMaintenance.NeedsMigration(autoMigrate, version));

    [Fact]
    public async Task Missing_today_partition_under_lock_fails_start()
    {
        await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).MigrateAsync(Ct);
        await using var sp = Build(o => o.PartitionLockTimeout = TimeSpan.FromMilliseconds(200));
        await using var holder = await _db.Admin.OpenConnectionAsync(Ct);
        await using (var lk = new NpgsqlCommand(_db.Sql("BEGIN; LOCK TABLE {schema}.jobs IN SHARE UPDATE EXCLUSIVE MODE"), holder))
            await lk.ExecuteNonQueryAsync(Ct);

        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct));
        Assert.Contains("partition", e.Message);
    }
}

public sealed class HostingTests_Direct(PostgresFixture pg) : HostingTests(pg, ConnectionMode.Direct);
public sealed class HostingTests_Bouncer(PostgresFixture pg) : HostingTests(pg, ConnectionMode.Bouncer);

/// <summary>Только Direct: PgBouncer не пропускает стартовый параметр options.</summary>
public sealed class HostingIsolationTests(PostgresFixture pg) : IAsyncLifetime
{
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, ConnectionMode.Direct, migrate: false);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Non_read_committed_default_isolation_fails_start_before_migrations()
    {
        var cs = new NpgsqlConnectionStringBuilder(pg.DirectConnectionString) { Options = "-c default_transaction_isolation=serializable" };
        var services = new ServiceCollection();
        services.AddPgBolt(o => { o.ConnectionString = cs.ConnectionString; o.Schema = _db.Schema; });
        await using var sp = services.BuildServiceProvider();

        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct));
        Assert.Contains("default_transaction_isolation", e.Message);
        Assert.Contains("serializable", e.Message);
        Assert.Null(await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).GetCurrentVersionAsync(Ct));
    }
}
