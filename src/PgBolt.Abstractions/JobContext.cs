using Microsoft.Extensions.Logging;

namespace PgBolt;

public sealed class JobContext(JobId jobId, string jobName, int attempt, int maxAttempts, JobId? parentId, BatchId? batchId, ILogger logger,
    DateTimeOffset? scheduledAt = null, string? recurringJobId = null, BatchId? parentBatchId = null)
{
    public JobId JobId { get; } = jobId;
    public string JobName { get; } = jobName;
    public int Attempt { get; } = attempt;
    public int MaxAttempts { get; } = maxAttempts;

    /// <summary>Parent job of a continuation (Part 5 §5.3). Null for root jobs, continuations of a batch, and members of a batch
    /// continuation.</summary>
    public JobId? ParentId { get; } = parentId;

    /// <summary>Batch containing this job (Part 5).</summary>
    public BatchId? BatchId { get; } = batchId;

    public ILogger Logger { get; } = logger;

    /// <summary>Cron occurrence for which this job was enqueued (Part 4). Null for jobs created outside a recurring schedule.</summary>
    public DateTimeOffset? ScheduledAt { get; } = scheduledAt;

    /// <summary>Recurring schedule ID (Part 4). Null for jobs created outside a recurring schedule.</summary>
    public string? RecurringJobId { get; } = recurringJobId;

    /// <summary>Parent batch of a continuation (Part 5 §5.3). Null for members of a batch continuation, whose containing batch is BatchId.</summary>
    public BatchId? ParentBatchId { get; } = parentBatchId;
}
