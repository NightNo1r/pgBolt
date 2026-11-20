using Npgsql;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class DequeueWaitTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Immediate_job_wait_is_time_since_enqueue()
    {
        await EnqueueAsync();
        await Db.ExecAsync("SELECT pg_sleep(0.2)");

        var job = await LeaseOneAsync();

        Assert.InRange(job.WaitSeconds, 0.2, 5);
    }

    [Fact]
    public async Task Delayed_job_wait_counts_from_run_at()
    {
        var id = await EnqueueAsync(delay: TimeSpan.FromHours(1));
        await Db.ExecAsync("UPDATE {schema}.queue SET run_at = now() - interval '30 seconds', ready = true WHERE job_id = @id", ("id", id));

        var job = await LeaseOneAsync();

        Assert.InRange(job.WaitSeconds, 30, 60);
    }

    [Fact]
    public async Task Limited_dequeue_returns_wait()
    {
        await Db.ExecAsync("INSERT INTO {schema}.queue_settings (queue, default_max_concurrency) VALUES ('q', 5)");
        var id = await EnqueueAsync(delay: TimeSpan.FromHours(1));
        await Db.ExecAsync("UPDATE {schema}.queue SET run_at = now() - interval '30 seconds', ready = true WHERE job_id = @id", ("id", id));

        var job = Assert.Single((await Store.DequeueLimitedAsync("q", 1, ServerA, Lease, Ct)).Jobs);

        Assert.InRange(job.WaitSeconds, 30, 60);
    }

    [Fact]
    public async Task Wait_counts_from_trigger_commit_during_lock_wait()
    {
        const string lockKey = "hashtextextended('pgbolt:{schema}:' || @q, 0)";
        var id = await EnqueueAsync(delay: TimeSpan.FromHours(1));

        // A держит блокировку очереди, как dequeue с лимитом, в открытой транзакции.
        await using var a = await Db.Admin.OpenConnectionAsync(Ct);
        await using var txA = await a.BeginTransactionAsync(Ct);
        await using (var lk = new NpgsqlCommand(Db.Sql($"SELECT pg_advisory_xact_lock({lockKey})"), a, txA))
        {
            lk.Parameters.AddWithValue("q", "q");
            await lk.ExecuteNonQueryAsync(Ct);
        }

        var dequeue = Store.DequeueLimitedAsync("q", 1, ServerA, Lease, Ct);
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        while (!await Db.ScalarAsync<bool>($"""
                   SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted
                                  AND ((classid::bigint << 32) | objid::bigint) = {lockKey})
                   """, ("q", "q")))
        {
            Assert.False(dequeue.IsCompleted, "Dequeue finished without waiting for the lock");
            Assert.True(System.Diagnostics.Stopwatch.GetElapsedTime(started) < TimeSpan.FromSeconds(10), "Dequeue never waited for the lock");
            await Task.Delay(20, Ct);
        }

        // Задача становится готовой и коммитится, пока dequeue ждёт; после этого A держит lock ещё 0.2 с.
        await Db.ExecAsync("UPDATE {schema}.queue SET run_at = now(), ready = true WHERE job_id = @id", ("id", id));
        await using (var sleep = new NpgsqlCommand("SELECT pg_sleep(0.2)", a, txA))
            await sleep.ExecuteNonQueryAsync(Ct);
        await txA.CommitAsync(Ct);

        var job = Assert.Single((await dequeue).Jobs);
        Assert.InRange(job.WaitSeconds, 0.2, 5);
    }
}

public sealed class DequeueWaitTests_Direct(PostgresFixture pg) : DequeueWaitTests(pg, ConnectionMode.Direct);
public sealed class DequeueWaitTests_Bouncer(PostgresFixture pg) : DequeueWaitTests(pg, ConnectionMode.Bouncer);
