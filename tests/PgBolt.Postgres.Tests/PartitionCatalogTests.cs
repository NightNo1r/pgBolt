using System.Globalization;
using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public sealed class PartitionCatalogUnitTests
{
    private static readonly DateOnly Day = new(2026, 10, 2);

    [Theory]
    [InlineData("jobs_20261002", "jobs", "JobsPartition")]
    [InlineData("jobs_20261002", null, "DetachedJobs")]
    [InlineData("jobs_20261002_r16384", null, "CompactionRemnant")]
    [InlineData("job_events_20261002", "job_events", "EventsPartition")]
    [InlineData("job_events_20261002", null, "DetachedEvents")]
    public void Classifies_our_tables(string name, string? parent, string kind)   // CatalogKind — internal: в сигнатуре публичного теста нельзя
    {
        var t = PartitionCatalog.Classify(42, name, parent);
        Assert.Equal(new CatalogTable(42, name, Enum.Parse<CatalogKind>(kind), Day), t);
    }

    [Theory]
    [InlineData("jobs_2026101", null)]
    [InlineData("jobs_20261002_old", null)]
    [InlineData("jobs_20261399", null)]
    [InlineData("jobs_20261002", "other.jobs")]
    [InlineData("jobs_20261002", "job_events")]
    [InlineData("jobs_20261002_r1", "jobs")]
    [InlineData("jobs_20261002_r12345678901", null)]
    [InlineData("job_events_20261002", "jobs")]
    [InlineData("job_events_20261002_r1", null)]
    public void Ignores_foreign_tables(string name, string? parent) =>
        Assert.Null(PartitionCatalog.Classify(42, name, parent));

    [Fact]
    public void Table_and_kind_helpers()
    {
        Assert.Equal("jobs", new CatalogTable(1, "x", CatalogKind.CompactionRemnant, Day).Table);
        Assert.True(new CatalogTable(1, "x", CatalogKind.DetachedJobs, Day).IsJobs);
        Assert.Equal("job_events", new CatalogTable(1, "x", CatalogKind.DetachedEvents, Day).Table);
    }

    [Theory]
    [InlineData(7 * 24 * 60, 7)]
    [InlineData(36 * 60, 2)]
    [InlineData(1, 1)]
    [InlineData(24 * 60, 1)]
    [InlineData(24 * 60 + 1, 2)]
    public void Retention_rounds_up_to_days(int minutes, int days) =>
        Assert.Equal(days, PartitionCatalog.RetentionDays(TimeSpan.FromMinutes(minutes)));

    [Fact]
    public void Horizon_and_overdue_follow_spec_example()
    {
        var today = new DateOnly(2026, 10, 10);
        var horizon = PartitionCatalog.Horizon(today, TimeSpan.FromDays(7));
        Assert.Equal(new DateOnly(2026, 10, 2), horizon);           // §5.2: Rd = 7, today = 2026-10-10 → D ≤ 2026-10-02
        Assert.Equal(0, PartitionCatalog.Overdue(horizon!.Value, new DateOnly(2026, 10, 2)));
        Assert.Equal(3, PartitionCatalog.Overdue(horizon.Value, new DateOnly(2026, 9, 29)));
        Assert.Null(PartitionCatalog.Horizon(today, null));
        Assert.Equal(new DateOnly(2026, 10, 7), PartitionCatalog.Horizon(today, TimeSpan.FromHours(36)));
        Assert.Equal(new DateOnly(2016, 10, 11), PartitionCatalog.Horizon(today, TimeSpan.FromDays(3650)));
    }

    [Fact]
    public void Tuning_defaults_match_spec()
    {
        Assert.Equal(new RetentionTuning(10, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1)), RetentionTuning.Default);
        Assert.Null(RetentionTuning.Default.Hook);
        Assert.Null(RetentionTuning.Default.BatchBTestPrefix);
    }
}

public abstract class PartitionCatalogTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private sealed record NameKind(string Name, CatalogKind Kind);

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, partitionDaysAhead: 1);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Lists_partitions_detached_tables_and_remnants_of_this_schema_only()
    {
        var today = (await PartitionCatalog.ListAsync(_db.DataSource, _db.Schema, Ct)).Today;
        var d = today.AddDays(1).ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        await _db.ExecAsync($"ALTER TABLE {{schema}}.jobs DETACH PARTITION {{schema}}.jobs_{d}");
        await _db.ExecAsync($"CREATE TABLE {{schema}}.jobs_{d}_r777 (LIKE {{schema}}.jobs)");
        await _db.ExecAsync("CREATE TABLE {schema}.jobs_2026101 (LIKE {schema}.jobs)");
        await using var other = await TestDb.CreateAsync(pg, mode, partitionDaysAhead: 1);   // Review Focus 4

        var listing = await PartitionCatalog.ListAsync(_db.DataSource, _db.Schema, Ct);

        Assert.Equal(today, listing.Today);
        var t = today.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        Assert.Equal(
            new[]
            {
                new NameKind($"job_events_{t}", CatalogKind.EventsPartition), new NameKind($"job_events_{d}", CatalogKind.EventsPartition),
                new NameKind($"jobs_{t}", CatalogKind.JobsPartition), new NameKind($"jobs_{d}", CatalogKind.DetachedJobs),
                new NameKind($"jobs_{d}_r777", CatalogKind.CompactionRemnant),
            },
            listing.Tables.Select(x => new NameKind(x.Name, x.Kind)).OrderBy(x => x.Name, StringComparer.Ordinal));
        Assert.All(listing.Tables, x => Assert.True(x.Oid > 0));
    }
}

public sealed class PartitionCatalogTests_Direct(PostgresFixture pg) : PartitionCatalogTests(pg, ConnectionMode.Direct);
public sealed class PartitionCatalogTests_Bouncer(PostgresFixture pg) : PartitionCatalogTests(pg, ConnectionMode.Bouncer);

/// <summary>
/// Только Direct: search_path задаётся стартовым параметром соединения, PgBouncer его отвергает
/// (08P01 unsupported startup parameter), а ошибка — в SQL списка и от режима соединения не зависит.
/// </summary>
public sealed class PartitionCatalogSearchPathTests(PostgresFixture pg) : IAsyncLifetime
{
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, ConnectionMode.Direct, partitionDaysAhead: 1);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Partition_of_foreign_jobs_visible_through_search_path_is_not_ours()
    {
        var day = (await PartitionCatalog.ListAsync(_db.DataSource, _db.Schema, Ct)).Today.AddDays(-10);
        var name = $"jobs_{day.ToString("yyyyMMdd", CultureInfo.InvariantCulture)}";
        var builder = new NpgsqlConnectionStringBuilder(_db.ConnectionString) { SearchPath = $"{_db.Schema}_x,public" };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        try
        {
            await _db.ExecAsync($$"""
                CREATE SCHEMA {schema}_x;
                CREATE TABLE {schema}_x.jobs (LIKE {schema}.jobs) PARTITION BY RANGE (id);
                CREATE TABLE {schema}.{{name}} (LIKE {schema}.jobs);
                ALTER TABLE {schema}_x.jobs ATTACH PARTITION {schema}.{{name}}
                    FOR VALUES FROM ('{{UuidV7.LowerBound(day)}}') TO ('{{UuidV7.LowerBound(day.AddDays(1))}}');
                """);
            Assert.Equal("jobs", await ScalarAsync<string>(dataSource,
                $"SELECT i.inhparent::regclass::text FROM pg_inherits i WHERE i.inhrelid = '{_db.Schema}.{name}'::regclass"));

            var listing = await PartitionCatalog.ListAsync(dataSource, _db.Schema, Ct);

            Assert.DoesNotContain(listing.Tables, t => t.Name == name);
            Assert.Contains(listing.Tables, t => t.Kind == CatalogKind.JobsPartition);
        }
        finally
        {
            await _db.ExecAsync("DROP SCHEMA IF EXISTS {schema}_x CASCADE");
        }
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlDataSource dataSource, string sql)
    {
        await using var cmd = dataSource.CreateCommand(sql);
        return (T)(await cmd.ExecuteScalarAsync(Ct))!;
    }
}
