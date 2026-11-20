using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class HeartbeatTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private static readonly TimeSpan LongLease = TimeSpan.FromMinutes(10);

    private static LeaseRef Ref(LeasedJob job) => new(job.Id, job.LeaseId);

    private Task ShortenLeasesAsync() =>
        Db.ExecAsync("UPDATE {schema}.queue SET lease_until = now() + interval '1 second' WHERE lease_owner IS NOT NULL");

    private Task<bool> LeaseExtendedAsync(Guid id) =>
        Db.ScalarAsync<bool>("SELECT lease_until > now() + interval '9 minutes' FROM {schema}.queue WHERE job_id = @id", ("id", id));

    private Task InsertServerRowAsync(Guid id) =>
        Db.ExecAsync("""
            INSERT INTO {schema}.servers (id, name, host, queues, capacity, started_at, heartbeat_at)
            VALUES (@id, 's', 'h', ARRAY['q'], 1, now() - interval '1 hour', now() - interval '1 hour')
            """, ("id", id));

    private Task<bool> ServerHeartbeatFreshAsync(Guid id) =>
        Db.ScalarAsync<bool>("SELECT heartbeat_at > now() - interval '1 minute' FROM {schema}.servers WHERE id = @id", ("id", id));

    [Fact]
    public async Task Extends_only_listed_leases_of_this_server()
    {
        for (var i = 0; i < 4; i++)
            await EnqueueAsync();
        var leasedA = await Store.DequeueAsync("q", 3, ServerA, Lease, Ct);
        var leasedB = Assert.Single(await Store.DequeueAsync("q", 1, ServerB, Lease, Ct));
        Assert.Equal(3, leasedA.Count);
        var listed = new[] { leasedA[0], leasedA[1] };
        var orphan = leasedA[2];
        await ShortenLeasesAsync();

        // lease сервера B в списке сервера A не продлевается: фильтр lease_owner = @server
        var result = (await Store.HeartbeatAsync(ServerA, [Ref(listed[0]), Ref(listed[1]), Ref(leasedB)], LongLease, Ct)).Leases;

        Assert.Equal(listed.Select(j => (j.Id, j.LeaseId)).OrderBy(x => x.Id), result.Select(h => (h.JobId, h.LeaseId)).OrderBy(x => x.JobId));
        Assert.All(result, h => Assert.False(h.CancelRequested));
        Assert.True(await LeaseExtendedAsync(listed[0].Id));
        Assert.True(await LeaseExtendedAsync(listed[1].Id));
        Assert.False(await LeaseExtendedAsync(orphan.Id));   // «сиротская» строка сервера A — вне списка, истекает (§6.2)
        Assert.False(await LeaseExtendedAsync(leasedB.Id));  // чужой lease_owner
    }

    [Fact]
    public async Task Released_and_re_leased_job_is_not_extended_by_stale_lease()
    {
        var id = await EnqueueAsync();
        var first = await LeaseOneAsync();
        Assert.Equal(FailOutcome.Retried, await Store.RetryAsync(id, first.LeaseId, ServerA, TimeSpan.Zero, JobEventKind.Retry, null, null, Ct));
        var second = await LeaseOneAsync();
        Assert.NotEqual(first.LeaseId, second.LeaseId);
        await ShortenLeasesAsync();

        Assert.Empty((await Store.HeartbeatAsync(ServerA, [Ref(first)], LongLease, Ct)).Leases);
        Assert.False(await LeaseExtendedAsync(id));

        var hb = Assert.Single((await Store.HeartbeatAsync(ServerA, [Ref(first), Ref(second)], LongLease, Ct)).Leases);
        Assert.Equal((id, second.LeaseId), (hb.JobId, hb.LeaseId));
        Assert.True(await LeaseExtendedAsync(id));
    }

    [Fact]
    public async Task Reports_cancel_requests()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id));

        var hb = Assert.Single((await Store.HeartbeatAsync(ServerA, [Ref(job)], Lease, Ct)).Leases);
        Assert.Equal((id, job.LeaseId), (hb.JobId, hb.LeaseId));
        Assert.True(hb.CancelRequested);
    }

    [Fact]
    public async Task Updates_server_heartbeat_when_registered()
    {
        await InsertServerRowAsync(ServerA);
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();

        Assert.Single((await Store.HeartbeatAsync(ServerA, [Ref(job)], Lease, Ct)).Leases);
        Assert.True(await ServerHeartbeatFreshAsync(ServerA));
        Assert.True(await InQueueAsync(id));
    }

    [Fact]
    public async Task Empty_list_updates_server_heartbeat_and_extends_nothing()
    {
        await InsertServerRowAsync(ServerA);
        var id = await EnqueueAsync();
        await LeaseOneAsync();
        await ShortenLeasesAsync();

        Assert.Empty((await Store.HeartbeatAsync(ServerA, [], LongLease, Ct)).Leases);

        Assert.True(await ServerHeartbeatFreshAsync(ServerA));
        Assert.False(await LeaseExtendedAsync(id));
    }

    [Fact]
    public async Task No_leases_returns_empty()
    {
        Assert.Empty((await Store.HeartbeatAsync(ServerA, [], Lease, Ct)).Leases);
    }
}

public sealed class HeartbeatTests_Direct(PostgresFixture pg) : HeartbeatTests(pg, ConnectionMode.Direct);
public sealed class HeartbeatTests_Bouncer(PostgresFixture pg) : HeartbeatTests(pg, ConnectionMode.Bouncer);

/// <summary>Без повторов при 40P01: heartbeat и пакетный ack одного сервера не должны давать deadlock (§5.3).</summary>
public sealed class HeartbeatAckDeadlockTests(PostgresFixture pg) : StoreTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Concurrent_heartbeat_and_shuffled_acks_do_not_deadlock()
    {
        var store = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: false);
        for (var i = 0; i < 300; i += 100)
        {
            var ids = Enumerable.Range(0, 100).Select(_ => Guid.CreateVersion7()).ToArray();
            await store.EnqueueAsync(new EnqueueBatch("t", "q", 0, 3, null, null, TimeSpan.Zero,
                ids, ids.Select(_ => "{}").ToArray(), new DateTime?[100], new string?[100]), null, Ct);
        }
        var leased = (await store.DequeueAsync("q", 300, ServerA, Lease, Ct)).Select(j => new LeaseRef(j.Id, j.LeaseId)).ToArray();
        Random.Shared.Shuffle(leased);

        using var stop = new CancellationTokenSource();
        var heartbeats = Task.Run(async () =>
        {
            // Список в перемешанном порядке: порядок захвата задаёт ORDER BY job_id в SQL, а не порядок списка.
            while (!stop.IsCancellationRequested)
                await store.HeartbeatAsync(ServerA, leased, Lease, Ct);
        }, Ct);

        var acks = leased.Chunk(10).Select(chunk => store.AckAsync(ServerA, chunk, Ct));
        var acked = await Task.WhenAll(acks);   // PostgresException 40P01 провалит тест
        stop.Cancel();
        await heartbeats;

        Assert.Equal(300, acked.Sum(a => a.Count));
    }
}
