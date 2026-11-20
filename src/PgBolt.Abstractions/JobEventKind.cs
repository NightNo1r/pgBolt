namespace PgBolt;

/// <summary>Коды job_events.kind (§4.3). Новые коды только добавляются.</summary>
public enum JobEventKind : short
{
    Retry = 1,
    Failed = 2,
    LeaseExpired = 3,
    Cancelled = 4,
    Requeued = 5,
    Triggered = 6,
    NoExecutor = 7,
}
