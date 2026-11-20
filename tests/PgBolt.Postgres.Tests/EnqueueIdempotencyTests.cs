using Npgsql;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class EnqueueIdempotencyTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Theory]
    [InlineData(null)]
    [InlineData("key-1")]
    public async Task Repeat_while_job_is_in_queue_returns_created(string? key)
    {
        var id = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(id, uniqueKey: key), null, Ct);
        var again = await Store.EnqueueAsync(Batch(id, uniqueKey: key), null, Ct);

        Assert.Equal(new[] { new EnqueueRowResult(id, true) }, again);
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE id = @id", ("id", id)));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("key-2")]
    public async Task Repeat_after_job_finished_does_not_requeue(string? key)
    {
        var id = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(id, uniqueKey: key), null, Ct);
        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE job_id = @id", ("id", id));
        await Db.ExecAsync("UPDATE {schema}.jobs SET state = 2, finished_at = now() WHERE id = @id", ("id", id));

        var again = await Store.EnqueueAsync(Batch(id, uniqueKey: key), null, Ct);

        Assert.Equal(new[] { new EnqueueRowResult(id, true) }, again);
        Assert.False(await InQueueAsync(id));
        Assert.Equal((short)2, await JobStateAsync(id));
    }

    [Fact]
    public async Task Repeat_while_original_transaction_is_uncommitted_waits_and_returns_created()
    {
        var id = Guid.CreateVersion7();
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await Store.EnqueueAsync(Batch(id), tx, Ct);

        var retry = Store.EnqueueAsync(Batch(id), null, Ct);
        await Task.Delay(300, Ct);
        Assert.False(retry.IsCompleted);
        await tx.CommitAsync(Ct);

        Assert.Equal(new[] { new EnqueueRowResult(id, true) }, await retry.WaitAsync(TimeSpan.FromSeconds(10), Ct));
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE id = @id", ("id", id)));
    }

    [Fact]
    public async Task Rolled_back_transaction_leaves_nothing()
    {
        var id = Guid.CreateVersion7();
        await using (var conn = await Db.DataSource.OpenConnectionAsync(Ct))
        await using (var tx = await conn.BeginTransactionAsync(Ct))
        {
            await Store.EnqueueAsync(Batch(id), tx, Ct);
            await tx.RollbackAsync(Ct);
        }
        Assert.False(await InQueueAsync(id));
        Assert.Null(await JobStateAsync(id));
    }

    [Fact]
    public async Task Completed_transaction_argument_is_rejected()
    {
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        var tx = await conn.BeginTransactionAsync(Ct);
        await tx.CommitAsync(Ct);
        // Npgsql 10 оставляет tx.Connection ненулевым после Commit и сам бросает InvalidOperationException при
        // выполнении команды на завершённой транзакции; store даёт то же исключение, если Connection == null.
        await Assert.ThrowsAsync<InvalidOperationException>(() => Store.EnqueueAsync(Batch(Guid.CreateVersion7()), tx, Ct));
    }
}

public sealed class EnqueueIdempotencyTests_Direct(PostgresFixture pg) : EnqueueIdempotencyTests(pg, ConnectionMode.Direct);
public sealed class EnqueueIdempotencyTests_Bouncer(PostgresFixture pg) : EnqueueIdempotencyTests(pg, ConnectionMode.Bouncer);

/// <summary>Без повторов при 40P01: deadlock должен всплыть исключением, если порядок вставки неединый (§5.1).</summary>
public sealed class EnqueueDeadlockTests(PostgresFixture pg) : StoreTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Parallel_batches_with_overlapping_keys_in_different_order_do_not_deadlock()
    {
        var store = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: false);
        for (var round = 0; round < 10; round++)
        {
            var keys = Enumerable.Range(0, 40).Select(i => $"r{round}-k{i}").ToArray();
            var tasks = Enumerable.Range(0, 8).Select(w =>
            {
                var shuffled = keys.ToArray();
                new Random(round * 100 + w).Shuffle(shuffled);
                var ids = shuffled.Select(_ => Guid.CreateVersion7()).ToArray();
                return store.EnqueueAsync(new EnqueueBatch("t", "q", 0, 3, null, null, TimeSpan.Zero,
                    ids, ids.Select(_ => "{}").ToArray(), new DateTime?[ids.Length], shuffled), null, Ct);
            });
            var results = await Task.WhenAll(tasks); // PostgresException 40P01 провалит тест
            Assert.Equal(40, results.SelectMany(r => r).Count(r => r.Created));
        }
    }
}
