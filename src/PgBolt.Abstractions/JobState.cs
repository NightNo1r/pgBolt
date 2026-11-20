namespace PgBolt;

/// <summary>Состояние задачи для клиента: Scheduled/Enqueued/Processing выводятся из строки queue (§2).</summary>
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
