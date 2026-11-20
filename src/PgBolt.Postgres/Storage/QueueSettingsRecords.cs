namespace PgBolt.Postgres.Storage;

/// <summary>Значения очереди из кода: кластерный лимит и вес (null — не задан).</summary>
public readonly record struct QueueDefault(string Queue, int? MaxConcurrency, int? Weight);

/// <summary>Значения, записанные другим процессом недавно и отличающиеся от публикуемых сейчас.</summary>
public sealed record QueueDefaultConflict(string Queue, int? PreviousMaxConcurrency, int? PreviousWeight, string PreviousPublishedBy);

/// <summary>Эффективные настройки очереди: кластерный лимит (0 — пауза, null — без лимита) и вес (null — без веса).</summary>
public readonly record struct ActiveQueueSetting(string Queue, int? Limit, int? Weight);
