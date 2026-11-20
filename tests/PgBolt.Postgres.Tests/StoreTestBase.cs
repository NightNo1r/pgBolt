using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class StoreTestBase(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    protected static readonly Guid ServerA = Guid.Parse("00000000-0000-0000-0000-0000000000a1");
    protected static readonly Guid ServerB = Guid.Parse("00000000-0000-0000-0000-0000000000b2");
    protected static readonly TimeSpan Lease = TimeSpan.FromMinutes(5);

    protected PostgresFixture Pg { get; } = pg;
    protected ConnectionMode Mode { get; } = mode;
    protected TestDb Db { get; private set; } = null!;
    protected PgBoltStore Store { get; private set; } = null!;
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    public virtual async ValueTask InitializeAsync()
    {
        Db = await CreateDbAsync();
        Store = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: true);
    }

    public virtual async ValueTask DisposeAsync() => await Db.DisposeAsync();

    protected virtual Task<TestDb> CreateDbAsync() => TestDb.CreateAsync(Pg, Mode);

    protected static EnqueueBatch Batch(Guid id, string queue = "q", short priority = 0, TimeSpan? delay = null,
        DateTime? runAtUtc = null, string? uniqueKey = null, int maxAttempts = 3, string args = """{"n":1}""",
        string type = "test.job", int[]? retryDelays = null, string? traceParent = null) =>
        new(type, queue, priority, maxAttempts, retryDelays, traceParent, delay ?? TimeSpan.Zero,
            [id], [args], [runAtUtc], [uniqueKey]);

    protected async Task<Guid> EnqueueAsync(string queue = "q", short priority = 0, TimeSpan? delay = null,
        string? uniqueKey = null, int maxAttempts = 3, string args = """{"n":1}""")
    {
        var id = Guid.CreateVersion7();
        var r = await Store.EnqueueAsync(Batch(id, queue, priority, delay, null, uniqueKey, maxAttempts, args), null, Ct);
        Assert.True(r[0].Created);
        return id;
    }

    protected Task<long> CountAsync(string table) => Db.ScalarAsync<long>($"SELECT count(*) FROM {{schema}}.{table}");

    protected Task<short?> JobStateAsync(Guid id) =>
        Db.ScalarAsync<short?>("SELECT state FROM {schema}.jobs WHERE id = @id", ("id", id));

    protected Task<bool> InQueueAsync(Guid id) =>
        Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM {schema}.queue WHERE job_id = @id)", ("id", id));

    protected async Task<List<short>> EventKindsAsync(Guid id)
    {
        await using var cmd = Db.Admin.CreateCommand(Db.Sql("SELECT kind FROM {schema}.job_events WHERE job_id = @id ORDER BY at, kind"));
        cmd.Parameters.AddWithValue("id", id);
        await using var r = await cmd.ExecuteReaderAsync(Ct);
        var list = new List<short>();
        while (await r.ReadAsync(Ct))
            list.Add(r.GetInt16(0));
        return list;
    }

    protected Task ExpireLeaseAsync(Guid id) =>
        Db.ExecAsync("UPDATE {schema}.queue SET lease_until = now() - interval '1 second' WHERE job_id = @id", ("id", id));

    protected async Task<LeasedJob> LeaseOneAsync(string queue = "q", Guid? server = null)
    {
        var leased = await Store.DequeueAsync(queue, 1, server ?? ServerA, Lease, Ct);
        return Assert.Single(leased);
    }
}
