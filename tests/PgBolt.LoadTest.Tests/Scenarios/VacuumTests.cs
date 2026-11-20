using Npgsql;
using PgBolt.LoadTest.Scenarios;
using PgBolt.Testing;

namespace PgBolt.LoadTest.Tests.Scenarios;

[Trait("Category", "Load")]
[Collection("Load")]
public sealed class VacuumTests(PostgresFixture pg)
{
    private const string HoldingSql = """
        SELECT count(*) FROM pg_stat_activity
        WHERE application_name = 'pgbolt-loadtest-hold-xmin' AND state = 'idle in transaction' AND backend_xmin IS NOT NULL
        """;

    [Fact]
    public async Task Hold_xmin_keeps_snapshot_until_disposed()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);

        await using (var hold = await Vacuum.HoldXminAsync(db.ConnectionString, db.Schema, ct))
        {
            Assert.Equal(1L, await db.ScalarAsync<long>(HoldingSql));
            await Vacuum.EnsureHeldAsync(hold, ct);
        }

        Assert.Equal(0L, await db.ScalarAsync<long>(HoldingSql));
    }

    [Fact]
    public async Task Ensure_held_throws_when_session_is_terminated()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var hold = await Vacuum.HoldXminAsync(db.ConnectionString, db.Schema, ct);

        await db.ExecAsync("SELECT pg_terminate_backend(@pid)", ("pid", hold.ProcessID));

        await Assert.ThrowsAnyAsync<NpgsqlException>(() => Vacuum.EnsureHeldAsync(hold, ct));
    }
}
