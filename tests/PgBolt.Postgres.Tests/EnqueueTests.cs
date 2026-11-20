using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class EnqueueTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Enqueue_creates_ready_queue_row_and_pending_job()
    {
        var id = Guid.CreateVersion7();
        var result = await Store.EnqueueAsync(Batch(id, queue: "emails", priority: 5, maxAttempts: 4,
            retryDelays: [1, 30], traceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"), null, Ct);

        Assert.Equal(new[] { new EnqueueRowResult(id, true) }, result);
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT ready AND lease_owner IS NULL AND attempt = 0 AND max_attempts = 4 AND queue = 'emails' AND priority = 5
                   AND abs(extract(epoch FROM run_at - now())) < 5
            FROM {schema}.queue WHERE job_id = @id
            """, ("id", id)));
        Assert.Equal((short)0, await JobStateAsync(id));
        Assert.Equal(new[] { 1, 30 }, await Db.ScalarAsync<int[]>("SELECT retry_delays FROM {schema}.jobs WHERE id = @id", ("id", id)));
        Assert.Equal("""{"n": 1}""", await Db.ScalarAsync<string>("SELECT args::text FROM {schema}.jobs WHERE id = @id", ("id", id)));
    }

    // Покрывает §11 «тест со сдвигом часов клиента»: в SQL уходит только интервал @delay, а run_at
    // считается от now() сервера, поэтому часы процесса не участвуют. Сдвинуть системные часы
    // процесса из теста нельзя, а TimeProvider клиентом для run_at не используется — отдельного теста нет.
    [Fact]
    public async Task Delay_is_counted_from_database_now_and_row_is_not_ready()
    {
        var id = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(id, delay: TimeSpan.FromHours(1)), null, Ct);
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT NOT ready AND abs(extract(epoch FROM run_at - (now() + interval '1 hour'))) < 5
            FROM {schema}.queue WHERE job_id = @id
            """, ("id", id)));
    }

    [Fact]
    public async Task Absolute_run_at_is_stored_as_is()
    {
        var past = Guid.CreateVersion7();
        var future = Guid.CreateVersion7();
        var pastAt = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var futureAt = DateTime.UtcNow.AddDays(1);
        await Store.EnqueueAsync(Batch(past, runAtUtc: pastAt), null, Ct);
        await Store.EnqueueAsync(Batch(future, runAtUtc: futureAt), null, Ct);

        Assert.True(await Db.ScalarAsync<bool>("SELECT ready FROM {schema}.queue WHERE job_id = @id", ("id", past)));
        Assert.False(await Db.ScalarAsync<bool>("SELECT ready FROM {schema}.queue WHERE job_id = @id", ("id", future)));
        Assert.Equal(pastAt, await Db.ScalarAsync<DateTime>("SELECT run_at FROM {schema}.queue WHERE job_id = @id", ("id", past)));
    }

    [Fact]
    public async Task Unique_key_conflict_returns_existing_active_job()
    {
        var first = Guid.CreateVersion7();
        var second = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(first, uniqueKey: "user:42"), null, Ct);
        var r = await Store.EnqueueAsync(Batch(second, uniqueKey: "user:42"), null, Ct);

        Assert.Equal(new[] { new EnqueueRowResult(first, false) }, r);
        Assert.Null(await JobStateAsync(second));
    }

    [Fact]
    public async Task Unique_key_is_free_after_job_leaves_queue()
    {
        var first = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(first, uniqueKey: "k"), null, Ct);
        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE job_id = @id", ("id", first));
        var second = Guid.CreateVersion7();
        Assert.Equal(new[] { new EnqueueRowResult(second, true) }, await Store.EnqueueAsync(Batch(second, uniqueKey: "k"), null, Ct));
    }

    [Fact]
    public async Task Duplicate_keys_inside_batch_resolve_to_first_inserted()
    {
        var a = Guid.CreateVersion7();
        var b = Guid.CreateVersion7();
        var batch = new EnqueueBatch("t", "q", 0, 3, null, null, TimeSpan.Zero,
            [a, b], ["{}", "{}"], [null, null], ["dup", "dup"]);
        var r = await Store.EnqueueAsync(batch, null, Ct);

        Assert.Single(r, x => x.Created);
        Assert.All(r, x => Assert.Equal(r.Single(y => y.Created).Id, x.Id));
    }

    [Fact]
    public async Task Batch_preserves_input_order()
    {
        var ids = Enumerable.Range(0, 100).Select(_ => Guid.CreateVersion7()).ToArray();
        var batch = new EnqueueBatch("t", "q", 0, 3, null, null, TimeSpan.Zero,
            ids, ids.Select(i => $$"""{"id":"{{i}}"}""").ToArray(), new DateTime?[100], new string?[100]);
        var r = await Store.EnqueueAsync(batch, null, Ct);

        Assert.Equal(ids.Select(i => new EnqueueRowResult(i, true)), r);
        Assert.Equal(100L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.queue"));
    }

    [Fact]
    public async Task Mismatched_arrays_are_rejected()
    {
        var batch = new EnqueueBatch("t", "q", 0, 3, null, null, TimeSpan.Zero,
            [Guid.CreateVersion7()], [], [null], [null]);
        await Assert.ThrowsAsync<ArgumentException>(() => Store.EnqueueAsync(batch, null, Ct));
    }

    [Fact]
    public async Task Recurring_variant_writes_schedule_columns()
    {
        var at = new DateTime(2026, 9, 30, 3, 0, 0, DateTimeKind.Utc);
        Guid[] ids = [Guid.CreateVersion7(), Guid.CreateVersion7()];
        var batch = new EnqueueBatch("test.job", "q", 0, 3, null, null, TimeSpan.Zero, ids, ["{}", "{}"],
            [null, null], [null, null])
        {
            RecurringIds = ["daily", "hourly"],
            ScheduledAtUtc = [at, null],
        };

        var rows = await Store.EnqueueAsync(batch, null, Ct);

        Assert.All(rows, r => Assert.True(r.Created));
        Assert.True(await Db.ScalarAsync<bool>(
            "SELECT recurring_id = 'daily' AND scheduled_at = @at FROM {schema}.jobs WHERE id = @id", ("id", ids[0]), ("at", at)));
        Assert.True(await Db.ScalarAsync<bool>(   // null момент — COALESCE(scheduled_at, now()): время БД (Trigger, §5.4, §8)
            "SELECT recurring_id = 'hourly' AND scheduled_at = created_at FROM {schema}.jobs WHERE id = @id", ("id", ids[1])));
    }

    [Fact]
    public async Task Recurring_variant_without_scheduled_at_uses_database_now()
    {
        var id = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(id) with { RecurringIds = ["daily"] }, null, Ct);
        Assert.True(await Db.ScalarAsync<bool>(
            "SELECT recurring_id = 'daily' AND scheduled_at = created_at FROM {schema}.jobs WHERE id = @id", ("id", id)));
    }

    [Fact]
    public async Task Plain_enqueue_leaves_recurring_columns_null()
    {
        var id = await EnqueueAsync();
        Assert.True(await Db.ScalarAsync<bool>(
            "SELECT recurring_id IS NULL AND scheduled_at IS NULL FROM {schema}.jobs WHERE id = @id", ("id", id)));
    }

    [Fact]
    public void Client_enqueue_keeps_original_sql()                     // §8: внешний enqueue — прежний SQL, колонки — внутренний вариант
    {
        var plain = Store.EnqueueSqlFor(Batch(Guid.CreateVersion7()));
        var recurring = Store.EnqueueSqlFor(Batch(Guid.CreateVersion7()) with { RecurringIds = ["daily"] });

        Assert.DoesNotContain("recurring_id", plain);
        Assert.DoesNotContain("scheduled_at", plain);
        Assert.Contains("@recurring_ids::text[]", recurring);
        Assert.Contains("COALESCE(i.scheduled_at, now())", recurring);
    }

    [Fact]
    public async Task Recurring_arrays_must_match_ids() =>
        await Assert.ThrowsAsync<ArgumentException>(() =>
            Store.EnqueueAsync(Batch(Guid.CreateVersion7()) with { RecurringIds = ["a", "b"] }, null, Ct));

    [Fact]
    public async Task Scheduled_at_requires_recurring_ids() =>
        await Assert.ThrowsAsync<ArgumentException>(() =>
            Store.EnqueueAsync(Batch(Guid.CreateVersion7()) with { ScheduledAtUtc = [DateTime.UtcNow] }, null, Ct));
}

public sealed class EnqueueTests_Direct(PostgresFixture pg) : EnqueueTests(pg, ConnectionMode.Direct);
public sealed class EnqueueTests_Bouncer(PostgresFixture pg) : EnqueueTests(pg, ConnectionMode.Bouncer);
