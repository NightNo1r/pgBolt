using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class ServersTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private const string ArgsSchema = """{"type":"object"}""";

    private Task<long> ServerRowsAsync(Guid id) =>
        Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.servers WHERE id = @id", ("id", id));

    [Fact]
    public async Task Register_inserts_server_row()
    {
        await Store.RegisterServerAsync(ServerA, "orders", "host-1", ["critical", "default"], 25, Ct);

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT name = 'orders' AND host = 'host-1' AND capacity = 25
                   AND started_at > now() - interval '1 minute' AND heartbeat_at = started_at
            FROM {schema}.servers WHERE id = @id
            """, ("id", ServerA)));
        Assert.Equal(new[] { "critical", "default" },
            await Db.ScalarAsync<string[]>("SELECT queues FROM {schema}.servers WHERE id = @id", ("id", ServerA)));
    }

    [Fact]
    public async Task Register_again_upserts_and_keeps_started_at()
    {
        await Store.RegisterServerAsync(ServerA, "a", "h1", ["q"], 1, Ct);
        await Db.ExecAsync("UPDATE {schema}.servers SET started_at = now() - interval '1 hour', heartbeat_at = now() - interval '1 hour' WHERE id = @id", ("id", ServerA));

        await Store.RegisterServerAsync(ServerA, "a2", "h2", ["q", "r"], 5, Ct);

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT name = 'a2' AND host = 'h2' AND capacity = 5 AND queues = ARRAY['q', 'r']
                   AND started_at < now() - interval '59 minutes' AND heartbeat_at > now() - interval '1 minute'
            FROM {schema}.servers WHERE id = @id
            """, ("id", ServerA)));
    }

    [Fact]
    public async Task Heartbeat_reports_server_row_presence_and_still_extends_leases()
    {
        await Store.RegisterServerAsync(ServerA, "a", "h", ["q"], 1, Ct);
        Assert.True((await Store.HeartbeatAsync(ServerA, [], Lease, Ct)).ServerPresent);

        await EnqueueAsync();
        var job = await LeaseOneAsync();
        var withLease = await Store.HeartbeatAsync(ServerA, [new LeaseRef(job.Id, job.LeaseId)], Lease, Ct);
        Assert.True(withLease.ServerPresent);                                       // §12: и с непустым списком lease
        Assert.Equal(job.LeaseId, Assert.Single(withLease.Leases).LeaseId);
        await Store.RemoveServerAsync(ServerA, Ct);
        await Db.ExecAsync("UPDATE {schema}.queue SET lease_until = now() + interval '1 second' WHERE job_id = @id", ("id", job.Id));

        var result = await Store.HeartbeatAsync(ServerA, [new LeaseRef(job.Id, job.LeaseId)], Lease, Ct);

        Assert.False(result.ServerPresent);
        Assert.Equal(job.LeaseId, Assert.Single(result.Leases).LeaseId);
        Assert.True(await Db.ScalarAsync<bool>("SELECT lease_until > now() + interval '4 minutes' FROM {schema}.queue WHERE job_id = @id", ("id", job.Id)));
        Assert.False((await Store.HeartbeatAsync(ServerA, [], Lease, Ct)).ServerPresent);
    }

    [Fact]
    public async Task Remove_deletes_only_its_row_and_missing_row_is_noop()
    {
        await Store.RegisterServerAsync(ServerA, "a", "h", ["q"], 1, Ct);
        await Store.RegisterServerAsync(ServerB, "b", "h", ["q"], 1, Ct);

        await Store.RemoveServerAsync(ServerA, Ct);
        await Store.RemoveServerAsync(ServerA, Ct);

        Assert.Equal(0L, await ServerRowsAsync(ServerA));
        Assert.Equal(1L, await ServerRowsAsync(ServerB));
    }

    [Fact]
    public async Task Registered_server_heartbeat_is_refreshed_by_empty_heartbeat()
    {
        await Store.RegisterServerAsync(ServerA, "a", "h", ["q"], 1, Ct);
        await Db.ExecAsync("UPDATE {schema}.servers SET heartbeat_at = now() - interval '1 hour' WHERE id = @id", ("id", ServerA));

        Assert.Empty((await Store.HeartbeatAsync(ServerA, [], Lease, Ct)).Leases);

        Assert.True(await Db.ScalarAsync<bool>("SELECT heartbeat_at > now() - interval '1 minute' FROM {schema}.servers WHERE id = @id", ("id", ServerA)));
    }

    [Fact]
    public async Task Job_type_names_by_queues_returns_sorted_names_of_matching_default_queues()
    {
        await Store.PublishJobTypesAsync(
        [
            new JobTypeRecord("c.job", ArgsSchema, "q1", 3, 0, null),
            new JobTypeRecord("a.job", ArgsSchema, "q1", 3, 0, null),
            new JobTypeRecord("b.job", ArgsSchema, "q2", 3, 0, null),
            new JobTypeRecord("d.job", ArgsSchema, "other", 3, 0, null),
        ], "test-host/1", Ct);

        Assert.Equal(new[] { "a.job", "b.job", "c.job" }, await Store.GetJobTypeNamesByQueuesAsync(["q1", "q2"], Ct));
        Assert.Equal(new[] { "d.job" }, await Store.GetJobTypeNamesByQueuesAsync(["other"], Ct));
        Assert.Empty(await Store.GetJobTypeNamesByQueuesAsync(["missing"], Ct));
        Assert.Empty(await Store.GetJobTypeNamesByQueuesAsync([], Ct));
    }
}

public sealed class ServersTests_Direct(PostgresFixture pg) : ServersTests(pg, ConnectionMode.Direct);
public sealed class ServersTests_Bouncer(PostgresFixture pg) : ServersTests(pg, ConnectionMode.Bouncer);
