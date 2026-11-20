using System.Text.Json;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class PromoterTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Promotes_only_due_rows_and_reports_queues()
    {
        var due = await EnqueueAsync(queue: "a", delay: TimeSpan.FromHours(1));
        var later = await EnqueueAsync(queue: "b", delay: TimeSpan.FromHours(1));
        await Db.ExecAsync("UPDATE {schema}.queue SET run_at = now() - interval '1 second' WHERE job_id = @id", ("id", due));

        var result = await Store.PromoteDueAsync(1000, Ct);

        Assert.Equal(1, result.Promoted);
        Assert.Equal(new[] { "a" }, result.Queues);
        Assert.True(await Db.ScalarAsync<bool>("SELECT ready FROM {schema}.queue WHERE job_id = @id", ("id", due)));
        Assert.False(await Db.ScalarAsync<bool>("SELECT ready FROM {schema}.queue WHERE job_id = @id", ("id", later)));
    }

    [Fact]
    public async Task Loops_until_partial_batch()
    {
        for (var i = 0; i < 25; i++)
            await EnqueueAsync(delay: TimeSpan.FromHours(1));
        await Db.ExecAsync("UPDATE {schema}.queue SET run_at = now() - interval '1 second'");

        Assert.Equal(25, (await Store.PromoteDueAsync(10, Ct)).Promoted);
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.queue WHERE NOT ready"));
    }

    [Fact]
    public async Task Delayed_job_is_invisible_to_dequeue_until_promoted()
    {
        await EnqueueAsync(delay: TimeSpan.FromMilliseconds(200));
        await Task.Delay(500, Ct);
        Assert.Empty(await Store.DequeueAsync("q", 1, ServerA, Lease, Ct));
        await Store.PromoteDueAsync(1000, Ct);
        Assert.Single(await Store.DequeueAsync("q", 1, ServerA, Lease, Ct));
    }
}

public sealed class PromoterTests_Direct(PostgresFixture pg) : PromoterTests(pg, ConnectionMode.Direct);
public sealed class PromoterTests_Bouncer(PostgresFixture pg) : PromoterTests(pg, ConnectionMode.Bouncer);

/// <summary>§10/§11: стоимость dequeue не зависит от числа отложенных строк. Только Direct (EXPLAIN).</summary>
[Trait("Category", "Slow")]
public sealed class DequeuePlanTests(PostgresFixture pg) : StoreTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Dequeue_reads_only_ready_entries_with_million_delayed_rows()
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts)
            SELECT gen_random_uuid(), 'q', (g % 3)::smallint, now() + interval '1 day', false, 3
            FROM generate_series(1, 1000000) g
            """);
        for (var i = 0; i < 5; i++)
            await EnqueueAsync();
        await Db.ExecAsync("ANALYZE {schema}.queue");

        var plan = await Db.ScalarAsync<string>("""
            EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            SELECT job_id FROM {schema}.queue
            WHERE queue = 'q' AND lease_owner IS NULL AND ready
            ORDER BY priority DESC, run_at
            LIMIT 10
            FOR UPDATE SKIP LOCKED
            """);

        using var doc = JsonDocument.Parse(plan);
        var root = doc.RootElement[0].GetProperty("Plan");
        Assert.Contains(Flatten(root), n => n.TryGetProperty("Index Name", out var name) && name.GetString() == "queue_fetch");
        var blocks = root.GetProperty("Shared Hit Blocks").GetInt64() + root.GetProperty("Shared Read Blocks").GetInt64();
        Assert.True(blocks < 100, $"dequeue touched {blocks} buffers");
    }

    private static IEnumerable<JsonElement> Flatten(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var children))
            foreach (var child in children.EnumerateArray())
                foreach (var n in Flatten(child))
                    yield return n;
    }
}
