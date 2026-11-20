using Npgsql;

namespace PgBolt.Postgres.Storage;

/// <summary>Повтор statement'а при 40P01/40001 вне пользовательской транзакции (§9).</summary>
internal static class DbRetry
{
    public const int MaxAttempts = 3;

    public static bool IsRetryable(PostgresException e) =>
        e.SqlState is PostgresErrorCodes.DeadlockDetected or PostgresErrorCodes.SerializationFailure;

    public static async Task<T> RunAsync<T>(Func<CancellationToken, Task<T>> op, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await op(ct);
            }
            catch (PostgresException e) when (attempt < MaxAttempts && IsRetryable(e))
            {
                await Task.Delay(TimeSpan.FromMilliseconds(5 * attempt + Random.Shared.Next(10)), ct);
            }
        }
    }
}
