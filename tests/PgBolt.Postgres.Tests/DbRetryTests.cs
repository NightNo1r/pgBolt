using Npgsql;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Tests;

public sealed class DbRetryTests
{
    private static PostgresException Pg(string sqlState) => new("boom", "ERROR", "ERROR", sqlState);

    [Theory]
    [InlineData(PostgresErrorCodes.DeadlockDetected)]
    [InlineData(PostgresErrorCodes.SerializationFailure)]
    public async Task Retries_deadlock_and_serialization_failure(string state)
    {
        var calls = 0;
        var result = await DbRetry.RunAsync(_ => ++calls < 3 ? throw Pg(state) : Task.FromResult(42), CancellationToken.None);
        Assert.Equal(42, result);
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task Gives_up_after_three_attempts()
    {
        var calls = 0;
        await Assert.ThrowsAsync<PostgresException>(() =>
            DbRetry.RunAsync<int>(_ => { calls++; throw Pg(PostgresErrorCodes.DeadlockDetected); }, CancellationToken.None));
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task Does_not_retry_other_errors()
    {
        var calls = 0;
        await Assert.ThrowsAsync<PostgresException>(() =>
            DbRetry.RunAsync<int>(_ => { calls++; throw Pg(PostgresErrorCodes.UniqueViolation); }, CancellationToken.None));
        Assert.Equal(1, calls);
    }
}
