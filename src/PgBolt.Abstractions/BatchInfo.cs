namespace PgBolt;

public enum BatchCancelResult { NotFound, AlreadyFinished, CancellationRequested }

/// <summary>Состояние batch (часть 5 §2.4); значения совпадают с batches.state.</summary>
public enum BatchState { Active, Awaiting, Succeeded, Failed, Cancelled }

/// <param name="Succeeded">Счётчики ≥ 0; между requeue члена и учётом его прежнего сигнала временно могут быть ниже факта (§6.6).</param>
/// <param name="OuterBatchId">Внешний batch (batches.parent_batch_id).</param>
/// <param name="ParentId">Родитель-задача batch-продолжения.</param>
/// <param name="ParentBatchId">Родитель-batch batch-продолжения.</param>
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
