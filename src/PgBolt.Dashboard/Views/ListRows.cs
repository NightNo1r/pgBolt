using System.ComponentModel;

namespace PgBolt.Dashboard.Views;

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: строка списка задач.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record JobListRow(Guid Id, string Type, string Queue, short Priority, int Attempt, int MaxAttempts, DateTime CreatedAt,
    DateTime? RunAt, DateTime? StartedAt, Guid? ServerId, DateTime? FinishedAt, bool CancelRequested, JobEventKind? EventKind, string? Message);

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: продолжение, член batch или запуск расписания.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record RelatedJobRow(Guid Id, string Type, string Queue, JobState State, short? Continuation, DateTime CreatedAt, DateTime? FinishedAt);

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: строка списка batches.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record BatchRow(Guid Id, BatchState State, int Total, int Succeeded, int Failed, int Cancelled, short? Condition,
    bool CancelRequested, DateTime CreatedAt, DateTime? FinishedAt);

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: очередь — снимок queue_stats и число живых серверов.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record QueueRow(string Name, long Scheduled, long Enqueued, long Processing, long Awaiting, int Servers, DateTime? MeasuredAt);

/// <summary>
/// Внутренний тип dashboard, не входит в контракт пакета: строка экрана очередей — снимок, настройки очереди (null — строки
/// queue_settings нет) и живое число взятых строк (null — без строки настроек, не считается).
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record QueueLine(QueueRow Stats, QueueSettings? Settings, int? Leased);

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: сервер и его исходы за час.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record ServerRow(Guid Id, string Name, string Host, string[] Queues, int Capacity, DateTime StartedAt, DateTime HeartbeatAt,
    bool Stale, long Succeeded, long Failed, long Retried);

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: задача обслуживания (Running — запуск идёт).</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record MaintenanceTaskRow(string Name, DateTime NextRunAt, DateTime? LastRunAt, DateTime? LastFinishedAt, string? LastError, bool Running);

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: экран серверов.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record ServersData(IReadOnlyList<ServerRow> Servers, IReadOnlyList<MaintenanceTaskRow> Tasks);

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: опубликованный тип задачи.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record JobTypeRow(string Name, string ArgsSchemaJson, string DefaultQueue, int DefaultMaxAttempts, short DefaultPriority,
    int[]? DefaultRetryDelaysSeconds, string PublishedBy, DateTime LastSeenAt);

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: строка списка расписаний.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record RecurringRow(string Id, string Type, string Cron, string TimeZone, string? Owner, bool Paused, DateTime? NextFireAt,
    DateTime? LastFireAt, Guid? LastJobId, string? LastError);
