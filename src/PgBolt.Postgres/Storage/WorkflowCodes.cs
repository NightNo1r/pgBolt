namespace PgBolt.Postgres.Storage;

/// <summary>
/// Persisted workflow codes (Part 5 §2.4, §4). jobs.continuation = queue.await_condition = batches.condition. completions.kind uses SQL literals
/// 2/3/4 for outcomes, 5 for Activated, 6 for CancelMembers, and 7 for Subscribed. batches.state includes Active and Awaiting. Terminal codes
/// match jobs.state and completion outcome kinds.
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

    /// <summary>Maps 1/3 to OnSuccess, 2/4 to OnAnyFinished, and 5 or NULL to null (§5.3).</summary>
    public static ContinuationCondition? Condition(short? code) => code switch
    {
        OnSuccessJob or OnSuccessBatch => ContinuationCondition.OnSuccess,
        OnAnyFinishedJob or OnAnyFinishedBatch => ContinuationCondition.OnAnyFinished,
        _ => null,
    };

    /// <summary>Only codes 1 and 2 identify a parent job.</summary>
    public static JobId? ParentJob(short? code, Guid? parentId) =>
        code is OnSuccessJob or OnAnyFinishedJob && parentId is { } parent ? new JobId(parent) : null;

    /// <summary>Only codes 3 and 4 identify a parent batch. Code 5, a member of a batch continuation, returns null.</summary>
    public static BatchId? ParentBatch(short? code, Guid? parentId) =>
        code is OnSuccessBatch or OnAnyFinishedBatch && parentId is { } parent ? new BatchId(parent) : null;
}
