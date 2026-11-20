using System.Text.Json;

namespace PgBolt;

/// <summary>
/// Задача для клиента. Awaiting — строка queue ждёт родителя (RunAt = null). ParentId — родитель-задача (continuation 1, 2),
/// ParentBatchId — родитель-batch (3, 4); у члена batch-продолжения (5) оба null, BatchId заполнен. Continuation: 1/3 — OnSuccess,
/// 2/4 — OnAnyFinished, 5 и NULL — null (часть 5 §5.3).
/// </summary>
public sealed record JobInfo(
    JobId Id,
    string Name,
    string Queue,
    short Priority,
    JobState State,
    int Attempt,
    int MaxAttempts,
    DateTimeOffset CreatedAt,
    DateTimeOffset? RunAt,
    DateTimeOffset? StartedAt,
    DateTimeOffset? FinishedAt,
    Guid? ServerId,
    string? UniqueKey,
    bool CancellationRequested,
    JsonElement Args,
    IReadOnlyList<JobEventInfo> Events,
    string? RecurringJobId = null,
    DateTimeOffset? ScheduledAt = null,
    JobId? ParentId = null,
    BatchId? ParentBatchId = null,
    BatchId? BatchId = null,
    ContinuationCondition? Continuation = null);

public sealed record JobEventInfo(JobEventKind Kind, DateTimeOffset At, Guid? ServerId, int? Attempt, string? Message, string? Exception);
