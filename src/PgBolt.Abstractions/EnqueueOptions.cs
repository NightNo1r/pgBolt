using System.Data.Common;

namespace PgBolt;

public sealed record EnqueueOptions
{
    public string? Queue { get; init; }
    public short? Priority { get; init; }
    public DateTimeOffset? RunAt { get; init; }
    /// <summary>Взаимоисключимо с RunAt. Отсчитывается от now() сервера БД.</summary>
    public TimeSpan? Delay { get; init; }
    public string? UniqueKey { get; init; }
    public int? MaxAttempts { get; init; }
    public IReadOnlyList<TimeSpan>? RetryDelays { get; init; }
    /// <summary>Транзакционный enqueue: только NpgsqlTransaction с уровнем READ COMMITTED.</summary>
    public DbTransaction? Transaction { get; init; }
}
