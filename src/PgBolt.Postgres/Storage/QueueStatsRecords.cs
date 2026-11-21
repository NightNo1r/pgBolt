namespace PgBolt.Postgres.Storage;

/// <summary>One queue_stats row. OldestReadySeconds is the age of the oldest ready job on the PostgreSQL clock, or null if none are
/// ready.</summary>
internal sealed record QueueStatsRow(string Queue, long Scheduled, long Enqueued, long Processing, long Awaiting, double? OldestReadySeconds);

/// <summary>
/// A queue_stats snapshot. AgeSeconds is the age of the last successful snapshot on the PostgreSQL clock, or null if its time is unknown.
/// </summary>
internal sealed record QueueStatsSnapshot(IReadOnlyList<QueueStatsRow> Rows, double? AgeSeconds);
