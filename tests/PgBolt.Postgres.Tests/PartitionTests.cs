using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class PartitionTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private TestDb _db = null!;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, partitionDaysAhead: -1);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private PartitionManager Manager(TimeSpan? lockTimeout = null) =>
        new(_db.DataSource, _db.Schema, 80, lockTimeout ?? TimeSpan.FromSeconds(2));

    private Task<DateOnly> TodayAsync() => _db.ScalarAsync<DateOnly>("SELECT (now() AT TIME ZONE 'UTC')::date");

    private Task<long> PartitionCountAsync(string parent) =>
        _db.ScalarAsync<long>("SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass(@t)", ("t", $"{_db.Schema}.{parent}"));

    [Fact]
    public async Task Creates_partitions_for_today_and_ahead_idempotently()
    {
        var first = await Manager().EnsureAsync(3, Ct);
        Assert.Equal(new PartitionEnsureResult(8, false, true), first);
        Assert.Equal(4L, await PartitionCountAsync("jobs"));
        Assert.Equal(4L, await PartitionCountAsync("job_events"));
        Assert.Equal(new PartitionEnsureResult(0, false, true), await Manager().EnsureAsync(3, Ct));
    }

    [Fact]
    public async Task Parallel_calls_are_safe()
    {
        await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => Manager().EnsureAsync(2, Ct)));
        Assert.Equal(3L, await PartitionCountAsync("jobs"));
    }

    [Fact]
    public async Task Jobs_partitions_have_fillfactor_and_events_do_not()
    {
        await Manager().EnsureAsync(1, Ct); // запас в день: полночь UTC между EnsureAsync и TodayAsync не ломает тест
        var today = await TodayAsync();
        var jobsOptions = await _db.ScalarAsync<string[]>("SELECT reloptions FROM pg_class WHERE oid = to_regclass(@t)", ("t", $"{_db.Schema}.jobs_{today:yyyyMMdd}"));
        Assert.Contains("fillfactor=80", jobsOptions);
        var eventsOptions = await _db.ScalarAsync<string[]?>("SELECT reloptions FROM pg_class WHERE oid = to_regclass(@t)", ("t", $"{_db.Schema}.job_events_{today:yyyyMMdd}"));
        Assert.Null(eventsOptions);
    }

    [Fact]
    public async Task Row_with_uuid_of_future_day_is_accepted_after_ensure()
    {
        await Manager().EnsureAsync(9, Ct); // запас в день на случай полуночи UTC между EnsureAsync и TodayAsync
        var today = await TodayAsync();
        var id = Guid.CreateVersion7(new DateTimeOffset(today.AddDays(8).ToDateTime(new TimeOnly(12, 0)), TimeSpan.Zero));
        await _db.ExecAsync("""
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at)
            VALUES (@id, 't', 'q', 0, '{}', 0, 1, now())
            """, ("id", id));
        Assert.Equal(1L, await _db.ScalarAsync<long>($"SELECT count(*) FROM {_db.Schema}.jobs_{today.AddDays(8):yyyyMMdd}"));
    }

    [Fact]
    public async Task Long_reader_on_jobs_blocks_neither_attach_nor_inserts()
    {
        await Manager().EnsureAsync(0, Ct);
        await using var reader = await _db.Admin.OpenConnectionAsync(Ct);
        await using var tx = await reader.BeginTransactionAsync(Ct);
        await using (var cmd = new NpgsqlCommand(_db.Sql("SELECT count(*) FROM {schema}.jobs"), reader, tx))
            await cmd.ExecuteScalarAsync(Ct);

        var result = await Manager().EnsureAsync(4, Ct).WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.False(result.TimedOut);
        Assert.Equal(5L, await PartitionCountAsync("jobs"));

        // Вставка в jobs, как у enqueue (§5.1), при всё ещё открытом читателе: в сегодняшнюю и в только что присоединённую партицию.
        var today = await TodayAsync();
        var ids = new[] { Guid.CreateVersion7(), Guid.CreateVersion7(new DateTimeOffset(today.AddDays(4).ToDateTime(new TimeOnly(12, 0)), TimeSpan.Zero)) };
        foreach (var id in ids)
            await _db.ExecAsync("""
                INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at)
                VALUES (@id, 't', 'q', 0, '{}', 0, 1, now())
                """, ("id", id)).WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.Equal(2L, await _db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
    }

    [Fact]
    public async Task Lock_timeout_reports_timed_out_with_today_present()
    {
        await Manager().EnsureAsync(0, Ct);
        await using var holder = await HoldShareUpdateExclusiveAsync();
        var result = await Manager(TimeSpan.FromMilliseconds(200)).EnsureAsync(3, Ct);
        Assert.True(result.TimedOut);
        Assert.True(result.TodayPresent);
    }

    [Fact]
    public async Task Lock_timeout_reports_today_missing()
    {
        await using var holder = await HoldShareUpdateExclusiveAsync();
        var result = await Manager(TimeSpan.FromMilliseconds(200)).EnsureAsync(1, Ct);
        Assert.Equal(new PartitionEnsureResult(0, true, false), result);
    }

    private async Task<NpgsqlConnection> HoldShareUpdateExclusiveAsync()
    {
        var conn = await _db.Admin.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(_db.Sql("BEGIN; LOCK TABLE {schema}.jobs IN SHARE UPDATE EXCLUSIVE MODE"), conn);
        await cmd.ExecuteNonQueryAsync();
        return conn; // Dispose закрывает соединение и откатывает транзакцию
    }
}

public sealed class PartitionTests_Direct(PostgresFixture pg) : PartitionTests(pg, ConnectionMode.Direct);
public sealed class PartitionTests_Bouncer(PostgresFixture pg) : PartitionTests(pg, ConnectionMode.Bouncer);
