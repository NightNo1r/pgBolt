namespace PgBolt;

public enum BatchCancelResult { NotFound, AlreadyFinished, CancellationRequested }

/// <summary>Batch state (Part 5 §2.4). Values match batches.state.</summary>
public enum BatchState { Active, Awaiting, Succeeded, Failed, Cancelled }

/// <param name="Id">Batch identifier.</param>
/// <param name="State">Current batch state.</param>
/// <param name="Total">Number of direct member jobs and nested batches.</param>
/// <param name="Succeeded">
/// Number of direct members counted as succeeded. All outcome counters are nonnegative. They can temporarily undercount after a member
/// is requeued and before its previous signal is processed (§6.6).
/// </param>
/// <param name="Failed">Number of direct members counted as failed.</param>
/// <param name="Cancelled">Number of direct members counted as cancelled.</param>
/// <param name="OuterBatchId">Enclosing batch (batches.parent_batch_id).</param>
/// <param name="ParentId">Parent job of a batch continuation.</param>
/// <param name="ParentBatchId">Parent batch of a batch continuation.</param>
/// <param name="Condition">Condition on the parent outcome for a batch continuation.</param>
/// <param name="CancellationRequested">Whether cancellation has been requested for this batch.</param>
/// <param name="CreatedAt">Time the batch was created.</param>
/// <param name="FinishedAt">Time the batch reached a terminal state, or null while unfinished.</param>
public sealed record BatchInfo(
    BatchId Id,
    BatchState State,
    int Total,
    int Succeeded,
    int Failed,
    int Cancelled,
    BatchId? OuterBatchId,
    JobId? ParentId,
    BatchId? ParentBatchId,
    ContinuationCondition? Condition,
    bool CancellationRequested,
    DateTimeOffset CreatedAt,
    DateTimeOffset? FinishedAt);
