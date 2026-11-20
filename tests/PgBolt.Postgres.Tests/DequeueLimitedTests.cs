using Npgsql;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class DequeueLimitedTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private Task SetDefaultLimitAsync(string queue, int limit) =>
        Db.ExecAsync("INSERT INTO {schema}.queue_settings (queue, default_max_concurrency) VALUES (@q, @l)", ("q", queue), ("l", limit));

    private Task SetOverrideAsync(string queue, int? limit, int? defaultLimit = null) =>
        Db.ExecAsync("""
            INSERT INTO {schema}.queue_settings (queue, default_max_concurrency, override_concurrency_set, override_max_concurrency)
            VALUES (@q, @d, true, @l)
            """, ("q", queue), ("d", defaultLimit), ("l", limit));

    private async Task BacklogAsync(int count, string queue = "q")
    {
        for (var i = 0; i < count; i++)
            await EnqueueAsync(queue);
    }

    [Fact]
    public async Task Takes_up_to_room()
    {
        await SetDefaultLimitAsync("q", 2);
        await BacklogAsync(5);

        var first = await Store.DequeueLimitedAsync("q", 4, ServerA, Lease, Ct);
        var second = await Store.DequeueLimitedAsync("q", 4, ServerA, Lease, Ct);

        Assert.Equal((2, true), (first.Jobs.Count, first.LimitReached));
        Assert.Equal((0, true), (second.Jobs.Count, second.LimitReached));
    }

    [Fact]
    public async Task Takes_max_when_unlimited_or_no_row()
    {
        await BacklogAsync(5);
        await SetOverrideAsync("u", null, defaultLimit: 1);
        await BacklogAsync(5, "u");

        var noRow = await Store.DequeueLimitedAsync("q", 3, ServerA, Lease, Ct);
        var unlimited = await Store.DequeueLimitedAsync("u", 3, ServerA, Lease, Ct);

        Assert.Equal((3, false), (noRow.Jobs.Count, noRow.LimitReached));
        Assert.Equal((3, false), (unlimited.Jobs.Count, unlimited.LimitReached));
    }

    [Fact]
    public async Task Pause_takes_nothing()
    {
        await SetOverrideAsync("q", 0, defaultLimit: 5);
        await BacklogAsync(3);

        var r = await Store.DequeueLimitedAsync("q", 3, ServerA, Lease, Ct);

        Assert.Equal((0, true), (r.Jobs.Count, r.LimitReached));
    }

    [Fact]
    public async Task Empty_queue_is_not_limit_reached()
    {
        await SetDefaultLimitAsync("q", 5);

        var r = await Store.DequeueLimitedAsync("q", 3, ServerA, Lease, Ct);

        Assert.Equal((0, false), (r.Jobs.Count, r.LimitReached));
    }

    [Fact]
    public async Task Lowered_limit_below_leased_takes_nothing()
    {
        await BacklogAsync(5);
        Assert.Equal(3, (await Store.DequeueAsync("q", 3, ServerB, Lease, Ct)).Count);
        await SetDefaultLimitAsync("q", 2);

        var r = await Store.DequeueLimitedAsync("q", 3, ServerA, Lease, Ct);

        Assert.Equal((0, true), (r.Jobs.Count, r.LimitReached));
    }

    [Fact]
    public async Task Expired_lease_occupies_room()
    {
        await SetDefaultLimitAsync("q", 1);
        await BacklogAsync(2);
        var leased = Assert.Single((await Store.DequeueLimitedAsync("q", 1, ServerB, Lease, Ct)).Jobs);
        await ExpireLeaseAsync(leased.Id);

        var blocked = await Store.DequeueLimitedAsync("q", 1, ServerA, Lease, Ct);
        await Store.SweepExpiredLeasesAsync(100, Ct);
        var freed = await Store.DequeueLimitedAsync("q", 1, ServerA, Lease, Ct);

        Assert.Equal((0, true), (blocked.Jobs.Count, blocked.LimitReached));
        Assert.Single(freed.Jobs);
    }

    [Fact]
    public async Task Extra_candidates_are_unlocked_after_dequeue()
    {
        await SetDefaultLimitAsync("q", 1);
        await BacklogAsync(5);

        Assert.Single((await Store.DequeueLimitedAsync("q", 5, ServerA, Lease, Ct)).Jobs);

        await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await using var cmd = new NpgsqlCommand(Db.Sql(
            "SELECT count(*) FROM (SELECT job_id FROM {schema}.queue WHERE queue = 'q' AND lease_owner IS NULL FOR UPDATE NOWAIT) t"),
            conn, tx);
        Assert.Equal(4L, (long)(await cmd.ExecuteScalarAsync(Ct))!);
    }

    [Fact]
    public async Task Lock_is_released_with_the_batch()
    {
        await SetDefaultLimitAsync("q", 2);
        await BacklogAsync(3);

        await Store.DequeueLimitedAsync("q", 1, ServerA, Lease, Ct);

        await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT pg_try_advisory_xact_lock(hashtextextended('pgbolt:' || @schema || ':' || @q, 0))", conn, tx);
        cmd.Parameters.AddWithValue("schema", Db.Schema);
        cmd.Parameters.AddWithValue("q", "q");
        Assert.True((bool)(await cmd.ExecuteScalarAsync(Ct))!);
    }

    [Fact]
    public async Task Queue_names_are_ordinal_and_unicode()
    {
        await SetDefaultLimitAsync("Отчёты", 1);
        await BacklogAsync(3, "Отчёты");
        await BacklogAsync(3, "отчёты");

        var limited = await Store.DequeueLimitedAsync("Отчёты", 3, ServerA, Lease, Ct);
        var free = await Store.DequeueLimitedAsync("отчёты", 3, ServerA, Lease, Ct);

        Assert.Equal((1, true), (limited.Jobs.Count, limited.LimitReached));
        Assert.Equal((3, false), (free.Jobs.Count, free.LimitReached));
    }

    [Fact]
    public async Task Returned_jobs_match_DequeueAsync_columns()
    {
        var at = new DateTime(2026, 9, 30, 3, 0, 0, DateTimeKind.Utc);
        foreach (var queue in new[] { "plain", "limited" })
            await Store.EnqueueAsync(Batch(Guid.CreateVersion7(), queue, args: """{"a":"b"}""", maxAttempts: 7, retryDelays: [2, 9],
                    traceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                with { RecurringIds = ["daily"], ScheduledAtUtc = [at] }, null, Ct);
        await SetDefaultLimitAsync("limited", 3);

        var plain = Assert.Single(await Store.DequeueAsync("plain", 1, ServerA, Lease, Ct));
        var limited = Assert.Single((await Store.DequeueLimitedAsync("limited", 1, ServerA, Lease, Ct)).Jobs);

        Assert.Equal(new[] { 2, 9 }, limited.RetryDelaysSeconds);
        Assert.Equal(plain.RetryDelaysSeconds, limited.RetryDelaysSeconds);
        Assert.Equal(plain with { Id = limited.Id, LeaseId = limited.LeaseId, RetryDelaysSeconds = null, WaitSeconds = 0 },
            limited with { RetryDelaysSeconds = null, WaitSeconds = 0 });
        Assert.Equal(7, limited.MaxAttempts);
    }
}

public sealed class DequeueLimitedTests_Direct(PostgresFixture pg) : DequeueLimitedTests(pg, ConnectionMode.Direct);
public sealed class DequeueLimitedTests_Bouncer(PostgresFixture pg) : DequeueLimitedTests(pg, ConnectionMode.Bouncer);
