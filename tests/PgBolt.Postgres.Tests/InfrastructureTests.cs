using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class InfrastructureTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private TestDb _db = null!;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Connection_through_mode_executes_queries()
    {
        await using var cmd = _db.DataSource.CreateCommand("SELECT 41 + 1");
        Assert.Equal(42, (int)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!);
    }

    [Fact]
    public async Task Schema_is_created_and_isolated()
    {
        var exists = await _db.ScalarAsync<bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = @s)", ("s", _db.Schema));
        Assert.True(exists);
    }
}

public sealed class InfrastructureTests_Direct(PostgresFixture pg) : InfrastructureTests(pg, ConnectionMode.Direct);
public sealed class InfrastructureTests_Bouncer(PostgresFixture pg) : InfrastructureTests(pg, ConnectionMode.Bouncer);
