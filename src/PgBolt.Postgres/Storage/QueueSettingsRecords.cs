namespace PgBolt.Postgres.Storage;

/// <summary>Code-defined cluster concurrency limit and queue weight. Null means unspecified.</summary>
public readonly record struct QueueDefault(string Queue, int? MaxConcurrency, int? Weight);

/// <summary>Values recently written by another process that differ from those being published.</summary>
public sealed record QueueDefaultConflict(string Queue, int? PreviousMaxConcurrency, int? PreviousWeight, string PreviousPublishedBy);

/// <summary>Effective cluster-wide queue limit (0 for paused, null for unlimited) and weight (null for no weight).</summary>
public readonly record struct ActiveQueueSetting(string Queue, int? Limit, int? Weight);
