using System.ComponentModel;

namespace PgBolt.Dashboard.Views;

/// <summary>Internal dashboard job list row.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record JobListRow(Guid Id, string Type, string Queue, short Priority, int Attempt, int MaxAttempts, DateTime CreatedAt,
    DateTime? RunAt, DateTime? StartedAt, Guid? ServerId, DateTime? FinishedAt, bool CancelRequested, JobEventKind? EventKind, string? Message);

/// <summary>Internal dashboard row for a continuation, batch member or scheduled run.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record RelatedJobRow(Guid Id, string Type, string Queue, JobState State, short? Continuation, DateTime CreatedAt, DateTime? FinishedAt);

/// <summary>Internal dashboard batch list row.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record BatchRow(Guid Id, BatchState State, int Total, int Succeeded, int Failed, int Cancelled, short? Condition,
    bool CancelRequested, DateTime CreatedAt, DateTime? FinishedAt);

/// <summary>Internal dashboard queue snapshot from queue_stats with a count of live servers.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record QueueRow(string Name, long Scheduled, long Enqueued, long Processing, long Awaiting, int Servers, DateTime? MeasuredAt);

/// <summary>
/// Internal dashboard queue row with snapshot, settings and live lease count. Without a queue_settings row, both
/// settings and the lease count are null and no count is performed.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record QueueLine(QueueRow Stats, QueueSettings? Settings, int? Leased);

/// <summary>Internal dashboard server row with outcomes for the last hour.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record ServerRow(Guid Id, string Name, string Host, string[] Queues, int Capacity, DateTime StartedAt, DateTime HeartbeatAt,
    bool Stale, long Succeeded, long Failed, long Retried);

/// <summary>Internal dashboard maintenance task. Running indicates an active run.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record MaintenanceTaskRow(string Name, DateTime NextRunAt, DateTime? LastRunAt, DateTime? LastFinishedAt, string? LastError, bool Running);

/// <summary>Internal dashboard model for the servers screen.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record ServersData(IReadOnlyList<ServerRow> Servers, IReadOnlyList<MaintenanceTaskRow> Tasks);

/// <summary>Internal dashboard model for a published job type.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record JobTypeRow(string Name, string ArgsSchemaJson, string DefaultQueue, int DefaultMaxAttempts, short DefaultPriority,
    int[]? DefaultRetryDelaysSeconds, string PublishedBy, DateTime LastSeenAt);

/// <summary>Internal dashboard schedule list row.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record RecurringRow(string Id, string Type, string Cron, string TimeZone, string? Owner, bool Paused, DateTime? NextFireAt,
    DateTime? LastFireAt, Guid? LastJobId, string? LastError);
