using System.Data.Common;

namespace PgBolt;

public sealed record EnqueueOptions
{
    public string? Queue { get; init; }
    public short? Priority { get; init; }
    public DateTimeOffset? RunAt { get; init; }
    /// <summary>Delay measured from the database server's now(). Mutually exclusive with RunAt.</summary>
    public TimeSpan? Delay { get; init; }
    public string? UniqueKey { get; init; }
    public int? MaxAttempts { get; init; }
    public IReadOnlyList<TimeSpan>? RetryDelays { get; init; }
    /// <summary>Transaction to enqueue in. Requires an NpgsqlTransaction using READ COMMITTED isolation.</summary>
    public DbTransaction? Transaction { get; init; }
}
