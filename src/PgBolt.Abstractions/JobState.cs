namespace PgBolt;

/// <summary>Job state visible to clients. Scheduled, Enqueued, and Processing are derived from the queue row (§2).</summary>
public enum JobState
{
    Scheduled,
    Enqueued,
    Processing,
    Awaiting,
    Succeeded,
    Failed,
    Cancelled,
}
