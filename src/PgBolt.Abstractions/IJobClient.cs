using System.Text.Json;

namespace PgBolt;

public interface IJobClient
{
    ValueTask<EnqueueResult> EnqueueAsync<TArgs>(TArgs args, EnqueueOptions? options = null, CancellationToken ct = default);

    ValueTask<IReadOnlyList<EnqueueResult>> EnqueueManyAsync<TArgs>(
        IReadOnlyList<TArgs> args, EnqueueOptions? options = null,
        Func<TArgs, string?>? uniqueKeySelector = null, CancellationToken ct = default);

    ValueTask<EnqueueResult> EnqueueAsync(string jobName, JsonElement args, EnqueueOptions? options = null, CancellationToken ct = default);

    ValueTask<CancelResult> CancelAsync(JobId id, string? reason = null, CancellationToken ct = default);
    ValueTask<bool> TriggerAsync(JobId id, CancellationToken ct = default);
    /// <summary>
    /// Moves a Failed or Cancelled job to Pending (core §5.9). For a batch member, the batch and any terminal enclosing batches return to Active
    /// synchronously. Their outcomes are recomputed on completion. Continuations already started or cancelled keep their state (Part 5 §6.6).
    /// Requeuing a cancelled member during batch cancellation preserves the cancellation flag. The member may run or be cancelled again, depending
    /// on how far cancellation has progressed. Waiting members of its branch are cancelled. For deterministic behavior, requeue after the batch
    /// reaches a terminal state. Requeuing a cancelled continuation moves it directly to Pending.
    /// </summary>
    ValueTask<RequeueResult> RequeueAsync(JobId id, CancellationToken ct = default);
    ValueTask<JobInfo?> GetAsync(JobId id, CancellationToken ct = default);

    /// <summary>Creates a graph of continuations and batches (Part 5 §5). SubmitAsync writes the graph in one transaction.</summary>
    IWorkflow Workflow(WorkflowOptions? options = null);

    /// <summary>
    /// Cancels a batch (Part 5 §6.5). Unclaimed members are cancelled immediately. Running jobs receive cancellation through their heartbeat.
    /// Cancellation propagates recursively to nested batches. A member that finishes successfully counts as Succeeded. Waiting members of a
    /// cancelled branch never start. Repeated calls return CancellationRequested without sending another signal. Requires a resolver on a server
    /// with RunMaintenance = true.
    /// </summary>
    ValueTask<BatchCancelResult> CancelBatchAsync(BatchId id, CancellationToken ct = default);

    /// <summary>Returns the batch record. Its nonnegative counters can temporarily undercount after a member is requeued (§6.6).</summary>
    ValueTask<BatchInfo?> GetBatchAsync(BatchId id, CancellationToken ct = default);
}
