using System.Text.Json;

namespace PgBolt;

/// <summary>
/// Manages recurring schedules at runtime (Part 4 §4.2). For code-defined schedules (Owner != null), the API supports reading, pausing,
/// resuming, and triggering. AddOrUpdate and Remove throw InvalidOperationException for these schedules.
/// </summary>
public interface IRecurringJobManager
{
    /// <remarks>The generator registers TArgs metadata (JobMetadata&lt;TArgs&gt;) at the call site, as it does for EnqueueAsync.</remarks>
    ValueTask AddOrUpdateAsync<TArgs>(string id, string cron, TArgs args, RecurringJobOptions? options = null, CancellationToken ct = default);

    ValueTask AddOrUpdateAsync(string id, string jobName, string cron, JsonElement args, RecurringJobOptions? options = null,
        CancellationToken ct = default);

    /// <returns>false if the schedule does not exist.</returns>
    ValueTask<bool> RemoveAsync(string id, CancellationToken ct = default);

    ValueTask<bool> PauseAsync(string id, CancellationToken ct = default);

    /// <summary>Resumes the schedule. Its misfire policy handles occurrences missed during the pause.</summary>
    ValueTask<bool> ResumeAsync(string id, CancellationToken ct = default);

    /// <summary>
    /// Enqueues a job immediately without advancing the cursor. Returns null if the schedule does not exist. Throws UnknownJobException if the type
    /// is absent from job_types, or InvalidOperationException if a direct SQL write left NULL elements in retry_delays.
    /// </summary>
    ValueTask<EnqueueResult?> TriggerAsync(string id, CancellationToken ct = default);

    /// <summary>
    /// Returns the full record, or null if the schedule does not exist. A direct SQL write can store values that RecurringJobInfo cannot represent,
    /// such as months in catch_up_window, an interval outside the TimeSpan range, or NULL elements in retry_delays. In that case,
    /// InvalidOperationException identifies the schedule, the field, and how to fix it.
    /// </summary>
    ValueTask<RecurringJobInfo?> GetAsync(string id, CancellationToken ct = default);
}
