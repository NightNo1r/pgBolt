namespace PgBolt.Postgres.Storage;

/// <summary>Строка снимка queue_stats; OldestReadySeconds — возраст старейшей готовой задачи на часах PG (null — готовых нет).</summary>
internal sealed record QueueStatsRow(string Queue, long Scheduled, long Enqueued, long Processing, long Awaiting, double? OldestReadySeconds);

/// <summary>Снимок queue_stats; AgeSeconds — возраст последнего успешного снимка на часах PG (null — момент неизвестен).</summary>
internal sealed record QueueStatsSnapshot(IReadOnlyList<QueueStatsRow> Rows, double? AgeSeconds);
