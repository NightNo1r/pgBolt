using System.Text.Json;

namespace PgBolt;

/// <summary>
/// Client view of a job. An Awaiting queue row is waiting for its parent and has RunAt = null. ParentId identifies a parent job (continuation
/// codes 1 and 2), and ParentBatchId identifies a parent batch (3 and 4). For a member of a batch continuation (5), both are null and BatchId
/// identifies the containing batch. Continuation is OnSuccess for 1 and 3, OnAnyFinished for 2 and 4, and null for 5 or NULL (Part 5 §5.3).
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
