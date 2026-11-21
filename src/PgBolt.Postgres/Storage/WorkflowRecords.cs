namespace PgBolt.Postgres.Storage;

/// <summary>
/// Workflow batches row (§6.1, step 1). State is WorkflowCodes.BatchAwaiting for batch continuations and any batch nested inside them, or
/// BatchActive otherwise. Condition is 1–4 when ParentId is present. Total counts direct member jobs and nested batches.
/// </summary>
internal sealed record WorkflowBatchRow(Guid Id, Guid? OuterBatchId, Guid? ParentId, short? Condition, short State, int Total);

/// <summary>Existing parent job or batch selected by After (§6.2).</summary>
internal readonly record struct WorkflowParent(Guid Id, bool IsBatch);

/// <summary>
/// Graph for one SubmitAsync call, comprising batches, job groups (EnqueueBatch with graph arrays), and external parents in ID order.
/// </summary>
internal sealed record WorkflowPlan(IReadOnlyList<WorkflowBatchRow> Batches, IReadOnlyList<EnqueueBatch> Jobs, IReadOnlyList<WorkflowParent> External)
{
    /// <summary>The first graph row for retry detection (§6.1) is the first batch, or the first job if there are no batches.</summary>
    public Guid FirstNodeId => Batches.Count > 0 ? Batches[0].Id : Jobs[0].Ids[0];
}

/// <summary>Graph write result. SignalResolver indicates synthetic signals in the graph (§6.1, step 5).</summary>
internal readonly record struct WorkflowWriteResult(bool SignalResolver);

/// <summary>
/// An earlier attempt of the same SubmitAsync has written or is writing the graph rows (§6.1, Retries). SubmitWorkflowAsync uses this exception
/// internally for retry detection and wraps it in InvalidOperationException if exposed to the caller.
/// </summary>
internal sealed class WorkflowAlreadyWrittenException()
    : Exception("The workflow graph has already been written by a previous attempt of the same SubmitAsync.");

/// <summary>
/// T1 result (§6.4). Captured counts claimed signals (batchSize means full). Finished counts batches reaching a terminal state and producing
/// signals. Walking counts signals with children passed to T2. MissingBatches counts deltas for deleted batches rows (Debug). WalkReady and
/// CancelReady indicate a signal claimable by T2 or CancelMembers in the statement snapshot, excluding signals handed off by this T1.
/// </summary>
internal sealed record CompletionCountResult(int Captured, int Finished, int Walking, IReadOnlyList<Guid> MissingBatches,
    bool WalkReady, bool CancelReady)
{
    public static readonly CompletionCountResult Empty = new(0, 0, 0, [], false, false);
}

/// <summary>
/// T2 result (§6.4). Rows counts child rows in portions. Statements counts transaction commands independently of node count. MaxPortion is the
/// largest node portion. BudgetExhausted indicates nodes left pending due to the budget. NewSignals counts Activated, CancelMembers, and
/// cancelled-child signals. Deferred counts signals with retry_after because other transactions hold remaining rows. Queues identifies activated
/// children's queues.
/// </summary>
internal sealed record ResolveResult(int Captured, int Rows, int Statements, int MaxPortion, bool BudgetExhausted, int NewSignals,
    int Deferred, IReadOnlyCollection<string> Queues)
{
    public static readonly ResolveResult Empty = new(0, 0, 1, 0, false, 0, 0, []);
}

/// <summary>Resolver test hook points before T2 transaction COMMIT.</summary>
internal enum ResolverStep { Walked }

/// <summary>
/// CancelMembers iteration result (§6.5). Portions counts portion transactions performed. One or more causes an immediate next iteration. Rows
/// counts traversed rows. TimedOut counts portions rolled back on lock_timeout.
/// </summary>
internal sealed record CancelMembersResult(int Portions, int Rows, int TimedOut)
{
    public static readonly CancelMembersResult Empty = new(0, 0, 0);
}
