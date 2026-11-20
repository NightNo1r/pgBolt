using Microsoft.Extensions.Logging;

namespace PgBolt;

public sealed class JobContext(JobId jobId, string jobName, int attempt, int maxAttempts, JobId? parentId, BatchId? batchId, ILogger logger,
    DateTimeOffset? scheduledAt = null, string? recurringJobId = null, BatchId? parentBatchId = null)
{
    public JobId JobId { get; } = jobId;
    public string JobName { get; } = jobName;
    public int Attempt { get; } = attempt;
    public int MaxAttempts { get; } = maxAttempts;

    /// <summary>Родитель-задача продолжения (часть 5 §5.3); null у задач без родителя, у продолжений batch и членов batch-продолжения.</summary>
    public JobId? ParentId { get; } = parentId;

    /// <summary>Batch, членом которого задача является (часть 5).</summary>
    public BatchId? BatchId { get; } = batchId;

    public ILogger Logger { get; } = logger;

    /// <summary>Момент срабатывания cron, за который поставлена задача (часть 4); null — задача не из расписания.</summary>
    public DateTimeOffset? ScheduledAt { get; } = scheduledAt;

    /// <summary>Id расписания (часть 4); null — задача не из расписания.</summary>
    public string? RecurringJobId { get; } = recurringJobId;

    /// <summary>Родитель-batch продолжения (часть 5 §5.3); у членов batch-продолжения null (их batch — BatchId).</summary>
    public BatchId? ParentBatchId { get; } = parentBatchId;
}
