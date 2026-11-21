using Npgsql;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

[Trait("Category", "Slow")]
public abstract class DequeueLimitedConcurrencyTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private const string LockKey = "hashtextextended('pgbolt:{schema}:' || @q, 0)";

    private async Task BacklogAsync(int count)
    {
        var ids = Enumerable.Range(0, count).Select(_ => Guid.CreateVersion7()).ToArray();
        await Store.EnqueueAsync(new EnqueueBatch("test.job", "q", 0, 3, null, null, TimeSpan.Zero, ids,
            [.. ids.Select(_ => "{}")], new DateTime?[count], new string?[count]), null, Ct);
    }

    private async Task LockQueueAsync(NpgsqlConnection conn, NpgsqlTransaction tx)
    {
        await using var lk = new NpgsqlCommand(Db.Sql($"SELECT pg_advisory_xact_lock({LockKey})"), conn, tx);
        lk.Parameters.AddWithValue("q", "q");
        await lk.ExecuteNonQueryAsync(Ct);
    }

    private Task SetLimitAsync(int limit) =>
        Db.ExecAsync("INSERT INTO {schema}.queue_settings (queue, default_max_concurrency) VALUES ('q', @l)", ("l", limit));

    [Fact]
    public async Task Never_exceeds_limit_under_contention()
    {
        await SetLimitAsync(4);
        await BacklogAsync(2000);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        stop.CancelAfter(TimeSpan.FromSeconds(5));

        var workers = Enumerable.Range(0, 8).Select(w => Task.Run(async () =>
        {
            var server = Guid.NewGuid();
            var random = new Random(w);
            while (!stop.IsCancellationRequested)
            {
                var r = await Store.DequeueLimitedAsync("q", random.Next(1, 5), server, Lease, Ct);
                foreach (var job in r.Jobs)
                {
                    await Task.Delay(random.Next(0, 21), Ct);
                    if (random.Next(2) == 0)
                        await Store.AckAsync(server, [new LeaseRef(job.Id, job.LeaseId)], Ct);
                    else
                        await Store.ReleaseAsync(job.Id, job.LeaseId, server, ran: false, null, Ct);
                }
            }
        })).ToArray();

        var max = 0L;
        var samples = 0;
        await using (var conn = await Db.Admin.OpenConnectionAsync(Ct))
        {
            while (!stop.IsCancellationRequested)
            {
                await using (var tx = await conn.BeginTransactionAsync(Ct))
                {
                    await LockQueueAsync(conn, tx);
                    await using var count = new NpgsqlCommand(Db.Sql(
                        "SELECT count(*) FROM {schema}.queue WHERE queue = 'q' AND lease_owner IS NOT NULL"), conn, tx);
                    max = Math.Max(max, (long)(await count.ExecuteScalarAsync(Ct))!);
                    await tx.CommitAsync(Ct);
                }
                samples++;
                await Task.Delay(5, Ct);
            }
        }
        await Task.WhenAll(workers);

        Assert.True(samples > 10, $"{samples} samples");
        Assert.Equal(4L, max);
    }

    [Fact]
    public async Task MeasuresQueueLockWait()
    {
        var samples = new List<long>();
        Store.QueueLockMeasured = samples.Add;
        await Store.DequeueLimitedAsync("q", 1, ServerB, Lease, Ct);
        await using var holder = await Db.Admin.OpenConnectionAsync(Ct);
        await using var tx = await holder.BeginTransactionAsync(Ct);
        await LockQueueAsync(holder, tx);
        var pending = Store.DequeueLimitedAsync("q", 1, ServerB, Lease, Ct);
        await WaitForQueueLockAsync(pending);
        await Task.Delay(100, Ct);
        await tx.CommitAsync(Ct);
        await pending;
        Assert.Equal(2, samples.Count);
        Assert.True(samples[1] >= 100_000, $"free={samples[0]} µs, held={samples[1]} µs");
    }

    /// <summary>
    /// Контроль: lock и подсчёт в одном statement'е превышают лимит — snapshot statement'а взят до ожидания lock и не видит
    /// строки, закоммиченные держателем. Поэтому лимитированный dequeue берёт lock отдельной командой батча.
    /// </summary>
    [Fact]
    public async Task Single_statement_variant_overshoots()
    {
        await SetLimitAsync(2);
        await BacklogAsync(10);

        await using var a = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var txA = await a.BeginTransactionAsync(Ct);
        await LockQueueAsync(a, txA);
        await using (var take = new NpgsqlCommand(Db.Sql("""
            UPDATE {schema}.queue SET lease_owner = @s, lease_id = gen_random_uuid(), lease_until = now() + interval '5 minutes'
            WHERE job_id IN (SELECT job_id FROM {schema}.queue WHERE queue = 'q' ORDER BY run_at, job_id LIMIT 2)
            """), a, txA))
        {
            take.Parameters.AddWithValue("s", ServerA);
            Assert.Equal(2, await take.ExecuteNonQueryAsync(Ct));
        }

        var b = Task.Run(async () =>
        {
            await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
            await using var cmd = new NpgsqlCommand(Db.Sql($$"""
                WITH k AS (
                    SELECT count(*) AS leased FROM {schema}.queue
                    WHERE (SELECT pg_advisory_xact_lock({{LockKey}})) IS NOT NULL AND queue = @q AND lease_owner IS NOT NULL
                ),
                c AS (
                    SELECT job_id FROM {schema}.queue
                    WHERE queue = @q AND lease_owner IS NULL AND ready
                    ORDER BY priority DESC, run_at
                    LIMIT 2 - (SELECT leased FROM k)
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE {schema}.queue q SET lease_owner = @s, lease_id = gen_random_uuid(), lease_until = now() + interval '5 minutes'
                FROM c WHERE q.job_id = c.job_id
                """), conn);
            cmd.Parameters.AddWithValue("q", "q");
            cmd.Parameters.AddWithValue("s", ServerB);
            return await cmd.ExecuteNonQueryAsync(Ct);
        }, Ct);

        await WaitForQueueLockAsync(b);
        await txA.CommitAsync(Ct);

        Assert.Equal(2, await b);
        Assert.Equal(4L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.queue WHERE queue = 'q' AND lease_owner IS NOT NULL"));
    }
    private async Task WaitForQueueLockAsync(Task pending)
    {
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        while (!await Db.ScalarAsync<bool>($"""
                   SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted
                                  AND ((classid::bigint << 32) | objid::bigint) = {LockKey})
                   """, ("q", "q")))
        {
            Assert.False(pending.IsCompleted, "B finished without waiting for the lock");
            Assert.True(System.Diagnostics.Stopwatch.GetElapsedTime(started) < TimeSpan.FromSeconds(10), "B never waited for the lock");
            await Task.Delay(20, Ct);
        }
    }

}

public sealed class DequeueLimitedConcurrencyTests_Direct(PostgresFixture pg) : DequeueLimitedConcurrencyTests(pg, ConnectionMode.Direct);
public sealed class DequeueLimitedConcurrencyTests_Bouncer(PostgresFixture pg) : DequeueLimitedConcurrencyTests(pg, ConnectionMode.Bouncer);
