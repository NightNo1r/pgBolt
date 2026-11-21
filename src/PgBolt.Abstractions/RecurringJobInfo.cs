using System.Text.Json;

namespace PgBolt;

/// <summary>A recurring_jobs record (Part 4 §4.1). Owner is null for schedules created through the API.</summary>
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
