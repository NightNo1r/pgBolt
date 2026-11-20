using System.Text.Json;

namespace PgBolt;

/// <summary>Строка recurring_jobs (часть 4 §4.1). Owner = null — расписание создано через API.</summary>
public sealed record RecurringJobInfo(
    string Id,
    string JobName,
    JsonElement Args,
    string Cron,
    string TimeZone,
    MisfirePolicy Misfire,
    TimeSpan CatchUpWindow,
    OverlapPolicy Overlap,
    string? Queue,
    short? Priority,
    int? MaxAttempts,
    IReadOnlyList<TimeSpan>? RetryDelays,
    string? Owner,
    bool Paused,
    DateTimeOffset? NextFireAt,
    DateTimeOffset? LastFireAt,
    JobId? LastJobId,
    string? LastError,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);
