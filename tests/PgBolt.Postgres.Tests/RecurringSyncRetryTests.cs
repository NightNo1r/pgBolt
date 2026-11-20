using Microsoft.Extensions.Logging;
using Npgsql;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public sealed class RecurringSyncRetryTests
{
    private readonly ListLogger<RecurringSyncRetryTests> _log = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static PostgresException Error(string sqlState) => new("test", "ERROR", "ERROR", sqlState);

    private IEnumerable<LogEntry> DeadlockWarnings =>
        _log.Entries.Where(e => e.Level == LogLevel.Warning && e.Message.Contains("40P01") && e.Message.Contains("svc"));

    [Fact]
    public async Task Deadlock_is_retried_with_warning()                    // §6.2, §7: 40P01 — повтор транзакции, Warning
    {
        var calls = 0;
        var result = await PgBoltStore.RetrySyncDeadlockAsync(_ =>
            ++calls < 3 ? Task.FromException<int>(Error(PostgresErrorCodes.DeadlockDetected)) : Task.FromResult(42), 3, _log, "svc", Ct);

        Assert.Equal((42, 3), (result, calls));
        Assert.Equal(2, DeadlockWarnings.Count());
    }

    [Fact]
    public async Task Deadlock_after_last_attempt_is_thrown()
    {
        var calls = 0;
        var e = await Assert.ThrowsAsync<PostgresException>(() => PgBoltStore.RetrySyncDeadlockAsync<int>(_ =>
        {
            calls++;
            throw Error(PostgresErrorCodes.DeadlockDetected);
        }, 3, _log, "svc", Ct));

        Assert.Equal(PostgresErrorCodes.DeadlockDetected, e.SqlState);
        Assert.Equal(3, calls);
        Assert.Equal(2, DeadlockWarnings.Count());
    }

    [Fact]
    public async Task Other_errors_and_single_attempt_are_not_retried()
    {
        var calls = 0;
        await Assert.ThrowsAsync<PostgresException>(() => PgBoltStore.RetrySyncDeadlockAsync<int>(_ =>
        {
            calls++;
            throw Error(PostgresErrorCodes.UniqueViolation);
        }, 3, _log, "svc", Ct));
        await Assert.ThrowsAsync<PostgresException>(() => PgBoltStore.RetrySyncDeadlockAsync<int>(_ =>
        {
            calls++;
            throw Error(PostgresErrorCodes.DeadlockDetected);
        }, 1, _log, "svc", Ct));

        Assert.Equal(2, calls);
        Assert.Empty(_log.Entries);
    }
}
