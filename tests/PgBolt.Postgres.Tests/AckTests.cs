using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class AckTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Ack_deletes_queue_row_and_finalizes_job()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        var leasedAt = await Db.ScalarAsync<DateTime>("SELECT leased_at FROM {schema}.queue WHERE job_id = @id", ("id", id));

        Assert.Single(await Store.AckAsync(ServerA, [new LeaseRef(id, job.LeaseId)], Ct));

        Assert.False(await InQueueAsync(id));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT state = 2 AND attempt = 1 AND started_at = @leased AND finished_at IS NOT NULL AND server_id = @s
            FROM {schema}.jobs WHERE id = @id
            """, ("leased", leasedAt), ("s", ServerA), ("id", id)));
        Assert.Empty(await EventKindsAsync(id));
    }

    [Fact]
    public async Task Ack_with_foreign_lease_id_is_ignored()
    {
        var id = await EnqueueAsync();
        await LeaseOneAsync();
        Assert.Empty(await Store.AckAsync(ServerA, [new LeaseRef(id, Guid.NewGuid())], Ct));
        Assert.True(await InQueueAsync(id));
    }

    [Fact]
    public async Task Stale_lease_of_same_server_cannot_ack_new_attempt()
    {
        var id = await EnqueueAsync();
        var first = await LeaseOneAsync();
        await Db.ExecAsync("UPDATE {schema}.queue SET lease_owner = NULL, lease_id = NULL, lease_until = NULL, leased_at = NULL WHERE job_id = @id", ("id", id));
        var second = await LeaseOneAsync();

        Assert.Empty(await Store.AckAsync(ServerA, [new LeaseRef(id, first.LeaseId)], Ct));
        Assert.Single(await Store.AckAsync(ServerA, [new LeaseRef(id, second.LeaseId)], Ct));
        Assert.Equal((short)2, await JobStateAsync(id));
        Assert.Equal(2, await Db.ScalarAsync<int>("SELECT attempt FROM {schema}.jobs WHERE id = @id", ("id", id)));
    }

    [Fact]
    public async Task Batch_ack()
    {
        for (var i = 0; i < 50; i++)
            await EnqueueAsync();
        var jobs = await Store.DequeueAsync("q", 50, ServerA, Lease, Ct);
        Assert.Equal(50, (await Store.AckAsync(ServerA, jobs.Select(j => new LeaseRef(j.Id, j.LeaseId)).ToList(), Ct)).Count);
        Assert.Equal(50L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE state = 2"));
    }

    [Fact]
    public async Task Ack_returns_ids_of_confirmed_jobs_only()   // часть 6 §4.1
    {
        var good = await EnqueueAsync();
        var lost = await EnqueueAsync();
        var leased = await Store.DequeueAsync("q", 2, ServerA, Lease, Ct);
        var leases = leased.Select(j => new LeaseRef(j.Id, j.Id == lost ? Guid.NewGuid() : j.LeaseId)).ToList();

        Assert.Equal([good], await Store.AckAsync(ServerA, leases, Ct));
        Assert.Empty(await Store.AckAsync(ServerA, [], Ct));
    }

    [Fact]
    public async Task Empty_ack_is_noop()
    {
        Assert.Empty(await Store.AckAsync(ServerA, [], Ct));
    }
}

public sealed class AckTests_Direct(PostgresFixture pg) : AckTests(pg, ConnectionMode.Direct);
public sealed class AckTests_Bouncer(PostgresFixture pg) : AckTests(pg, ConnectionMode.Bouncer);

/// <summary>§1 `UsePreparedStatements = true`: смоук цикла enqueue → dequeue → ack с явным PrepareAsync. Только Direct: PgBouncer в тестах — с max_prepared_statements = 0.</summary>
public sealed class PreparedStatementsTests(PostgresFixture pg) : StoreTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Enqueue_dequeue_ack_with_prepared_statements()
    {
        var store = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: true, retryOnConflict: true);
        for (var round = 0; round < 2; round++) // второй круг выполняет уже подготовленные statement'ы
        {
            var id = Guid.CreateVersion7();
            Assert.True((await store.EnqueueAsync(Batch(id), null, Ct))[0].Created);
            var job = Assert.Single(await store.DequeueAsync("q", 10, ServerA, Lease, Ct));
            Assert.Equal(id, job.Id);
            Assert.Single(await store.AckAsync(ServerA, [new LeaseRef(id, job.LeaseId)], Ct));
            Assert.Equal((short)2, await JobStateAsync(id));
        }
    }
}
