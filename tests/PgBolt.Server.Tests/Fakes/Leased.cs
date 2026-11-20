using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Tests.Fakes;

/// <summary>Фабрика LeasedJob для unit-тестов.</summary>
internal static class Leased
{
    /// <summary>Новый lease (LeaseId = Guid.NewGuid()); тот же <paramref name="id"/> даёт повторно взятую задачу.</summary>
    public static LeasedJob Job(Guid? id = null, string type = "test.job", string args = "{}", int attempt = 1,
        int maxAttempts = 3, int[]? retryDelays = null) =>
        new(id ?? Guid.CreateVersion7(), Guid.NewGuid(), type, args, attempt, maxAttempts, retryDelays,
            ParentId: null, BatchId: null, TraceParent: null);
}
