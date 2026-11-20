namespace PgBolt.Postgres.Storage;

/// <summary>
/// Хранимые коды части 5 (§2.4, §4): jobs.continuation = queue.await_condition = batches.condition; completions.kind
/// (2/3/4 — исход, 5 Activated, 6 CancelMembers, 7 Subscribed; в SQL — литералами); batches.state (Active/Awaiting;
/// терминальные совпадают с jobs.state и с kind исхода).
/// </summary>
internal static class WorkflowCodes
{
    public const short OnSuccessJob = 1;
    public const short OnAnyFinishedJob = 2;
    public const short OnSuccessBatch = 3;
    public const short OnAnyFinishedBatch = 4;
    public const short Activation = 5;

    public const short Subscribed = 7;

    public const short BatchActive = 0;
    public const short BatchAwaiting = 1;

    public static short Code(ContinuationCondition condition, bool batchParent) => (condition, batchParent) switch
    {
        (ContinuationCondition.OnSuccess, false) => OnSuccessJob,
        (ContinuationCondition.OnAnyFinished, false) => OnAnyFinishedJob,
        (ContinuationCondition.OnSuccess, true) => OnSuccessBatch,
        (ContinuationCondition.OnAnyFinished, true) => OnAnyFinishedBatch,
        _ => throw new ArgumentOutOfRangeException(nameof(condition), condition, "Unknown continuation condition."),
    };

    /// <summary>1/3 → OnSuccess, 2/4 → OnAnyFinished, 5 и NULL → null (§5.3).</summary>
    public static ContinuationCondition? Condition(short? code) => code switch
    {
        OnSuccessJob or OnSuccessBatch => ContinuationCondition.OnSuccess,
        OnAnyFinishedJob or OnAnyFinishedBatch => ContinuationCondition.OnAnyFinished,
        _ => null,
    };

    /// <summary>Родитель-задача: только коды 1, 2.</summary>
    public static JobId? ParentJob(short? code, Guid? parentId) =>
        code is OnSuccessJob or OnAnyFinishedJob && parentId is { } parent ? new JobId(parent) : null;

    /// <summary>Родитель-batch: только коды 3, 4 (у члена batch-продолжения, код 5, — null).</summary>
    public static BatchId? ParentBatch(short? code, Guid? parentId) =>
        code is OnSuccessBatch or OnAnyFinishedBatch && parentId is { } parent ? new BatchId(parent) : null;
}
