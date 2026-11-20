using System.Collections.Concurrent;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class DequeueTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Dequeue_leases_job_and_returns_definition()
    {
        var id = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(id, args: """{"a":"b"}""", maxAttempts: 7, retryDelays: [2],
            traceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"), null, Ct);

        var job = await LeaseOneAsync();

        Assert.Equal(id, job.Id);
        Assert.Equal("test.job", job.Type);
        Assert.Equal("""{"a": "b"}""", job.ArgsJson);
        Assert.Equal((1, 7), (job.Attempt, job.MaxAttempts));
        Assert.Equal(new[] { 2 }, job.RetryDelaysSeconds);
        Assert.Equal("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", job.TraceParent);
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT lease_owner = @s AND lease_id = @l AND attempt = 1 AND leased_at IS NOT NULL
                   AND abs(extract(epoch FROM lease_until - (now() + interval '5 minutes'))) < 5
            FROM {schema}.queue WHERE job_id = @id
            """, ("s", ServerA), ("l", job.LeaseId), ("id", id)));
        Assert.True(await Db.ScalarAsync<bool>("SELECT state = 0 AND started_at IS NULL FROM {schema}.jobs WHERE id = @id", ("id", id)));
    }

    [Fact]
    public async Task Not_ready_jobs_are_not_dequeued()
    {
        await EnqueueAsync(delay: TimeSpan.FromHours(1));
        Assert.Empty(await Store.DequeueAsync("q", 10, ServerA, Lease, Ct));
    }

    [Fact]
    public async Task Order_is_priority_desc_then_run_at()
    {
        var lowEarly = await EnqueueAsync(priority: 0);
        var high = await EnqueueAsync(priority: 5);
        var lowLate = await EnqueueAsync(priority: 0);

        var jobs = await Store.DequeueAsync("q", 10, ServerA, Lease, Ct);

        Assert.Equal(new[] { high, lowEarly, lowLate }, jobs.Select(j => j.Id));
    }

    [Fact]
    public async Task Respects_limit_queue_and_existing_leases()
    {
        for (var i = 0; i < 5; i++)
            await EnqueueAsync();
        await EnqueueAsync(queue: "other");

        Assert.Equal(3, (await Store.DequeueAsync("q", 3, ServerA, Lease, Ct)).Count);
        Assert.Equal(2, (await Store.DequeueAsync("q", 10, ServerB, Lease, Ct)).Count);
        Assert.Empty(await Store.DequeueAsync("q", 10, ServerA, Lease, Ct));
    }

    [Fact]
    public async Task Concurrent_dequeuers_get_disjoint_jobs()
    {
        var ids = new HashSet<Guid>();
        for (var i = 0; i < 200; i++)
            ids.Add(await EnqueueAsync());

        var seen = new ConcurrentBag<Guid>();
        await Task.WhenAll(Enumerable.Range(0, 8).Select(async w =>
        {
            var server = Guid.NewGuid();
            while (true)
            {
                var batch = await Store.DequeueAsync("q", 10, server, Lease, Ct);
                if (batch.Count == 0)
                    return;
                foreach (var j in batch)
                    seen.Add(j.Id);
            }
        }));

        Assert.Equal(200, seen.Count);
        Assert.True(ids.SetEquals(seen));
    }

    [Fact]
    public async Task Leased_job_carries_recurring_columns()
    {
        var at = new DateTime(2026, 9, 30, 3, 0, 0, DateTimeKind.Utc);
        await Store.EnqueueAsync(Batch(Guid.CreateVersion7()) with { RecurringIds = ["daily"], ScheduledAtUtc = [at] }, null, Ct);

        var job = await LeaseOneAsync();

        Assert.Equal("daily", job.RecurringId);
        Assert.Equal(at, job.ScheduledAtUtc);
        Assert.Equal(DateTimeKind.Utc, job.ScheduledAtUtc!.Value.Kind);
    }
}

public sealed class DequeueTests_Direct(PostgresFixture pg) : DequeueTests(pg, ConnectionMode.Direct);
public sealed class DequeueTests_Bouncer(PostgresFixture pg) : DequeueTests(pg, ConnectionMode.Bouncer);
