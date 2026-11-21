using System.Data.Common;
using System.Text.Json;

namespace PgBolt;

/// <summary>Determines whether a continuation runs based on the outcome of its parent job or batch (Part 5 §2.2).</summary>
public enum ContinuationCondition
{
    /// <summary>
    /// Runs after Succeeded. A Failed parent leaves the continuation waiting for a requeue. A Cancelled parent cancels the continuation.
    /// </summary>
    OnSuccess,

    /// <summary>Runs after any terminal outcome.</summary>
    OnAnyFinished,
}

public sealed record WorkflowOptions
{
    /// <summary>
    /// Writes the graph in a caller-provided NpgsqlTransaction using READ COMMITTED (core §5.1). If SubmitAsync throws, graph writes roll back to
    /// the pgbolt_workflow savepoint and the transaction remains usable. Commit immediately after SubmitAsync. After locks the parent's queue row,
    /// or its jobs row if terminal, until commit. This blocks dequeue of a Pending parent, heartbeat, ack, and fail for a running parent, and
    /// RequeueAsync. A heartbeat delayed longer than LeaseDuration − HeartbeatInterval causes that server to cancel all its executions. Cancellation
    /// of the parent's batch also waits on the locked row (Part 5 §6.2, §6.5).
    /// </summary>
    public DbTransaction? Transaction { get; init; }
}

/// <summary>EnqueueOptions for a workflow step, without Transaction (Part 5 §5.1). Defaults come from [Job] or job_types.</summary>
public sealed record StepOptions
{
    public string? Queue { get; init; }
    public short? Priority { get; init; }

    /// <summary>Run time for root jobs and members of parentless batches. Mutually exclusive with Delay.</summary>
    public DateTimeOffset? RunAt { get; init; }

    /// <summary>Delay from the database server's now() for root jobs and members of parentless batches.</summary>
    public TimeSpan? Delay { get; init; }

    /// <summary>Unique within the graph. Batch members cannot specify a key. The pgbolt: prefix is reserved.</summary>
    public string? UniqueKey { get; init; }

    public int? MaxAttempts { get; init; }
    public IReadOnlyList<TimeSpan>? RetryDelays { get; init; }
}

/// <summary>
/// Methods shared by a workflow and a batch body (Part 5 §5.1). Steps build the graph synchronously. SubmitAsync writes it. Access the workflow,
/// its steps, and its batch bodies from a single thread (§5.2).
/// </summary>
public interface IWorkflowScope
{
    /// <remarks>The generator registers TArgs metadata at the call site, as it does for EnqueueAsync.</remarks>
    IJobStep Enqueue<TArgs>(TArgs args, StepOptions? options = null);

    IJobStep Enqueue(string jobName, JsonElement args, StepOptions? options = null);

    /// <summary>
    /// Starts a batch in this scope. Add its members inside <paramref name="build"/>. If <paramref name="build"/> throws, including
    /// ArgumentException for an empty batch, the incomplete batch remains in the graph. Subsequent calls on the workflow or its steps, including
    /// SubmitAsync, throw InvalidOperationException. Build a new graph to retry.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="build"/> is null. The workflow remains usable.</exception>
    /// <exception cref="ArgumentException">The batch or a nested batch has no members. The workflow is unusable.</exception>
    /// <exception cref="InvalidOperationException">
    /// The batch scope is closed, the workflow was submitted, or an earlier batch construction failed.
    /// </exception>
    IBatchStep StartBatch(Action<IBatchBuilder> build);
}

/// <summary>
/// Single-use graph of continuations and batches (Part 5 §5.1). Access it from one thread (§5.2) and discard it after SubmitAsync.
/// </summary>
public interface IWorkflow : IWorkflowScope
{
    /// <summary>
    /// Adds continuations to an existing job. SubmitAsync throws ParentNotFoundException if the job never existed, retention removed it, or the ID
    /// identifies a batch. A caller-provided transaction holds the parent row lock until commit (see WorkflowOptions.Transaction). If the parent is
    /// a batch member, CancelMembers waits for the lock before cancelling the remaining members.
    /// </summary>
    INodeStep After(JobId existing);

    /// <summary>
    /// Adds continuations to an existing batch. An OnSuccess continuation of a Failed batch waits for its Failed members to be requeued. Those
    /// members are retained while the batch row exists (Part 5 §7.1). SubmitAsync throws ParentNotFoundException if the batch is missing or the ID
    /// identifies a job.
    /// </summary>
    INodeStep After(BatchId existing);

    /// <summary>
    /// Writes the graph in one transaction. After submission, successful or otherwise, further calls on the workflow or its steps throw
    /// InvalidOperationException. The returned ValueTask reports exceptions. Any exception rolls back graph writes, using a savepoint in a
    /// caller-provided transaction (see WorkflowOptions.Transaction).
    /// </summary>
    /// <exception cref="ArgumentException">The workflow has no steps, or After(id) has no continuations.</exception>
    /// <exception cref="UnknownJobException">A dynamic step (Enqueue or ContinueWith by name) refers to a job type absent from
    /// job_types.</exception>
    /// <exception cref="ParentNotFoundException">The After parent never existed, has the wrong kind, or was removed by retention.</exception>
    /// <exception cref="UniqueKeyConflictException">An active job already uses a step's UniqueKey.</exception>
    /// <exception cref="NotSupportedException">WorkflowOptions.Transaction uses an isolation level other than READ COMMITTED.</exception>
    /// <exception cref="InvalidOperationException">
    /// Submission was repeated, called inside a batch callback, or attempted after a StartBatch or ContinueWithBatch callback threw. Catch
    /// ParentNotFoundException and UniqueKeyConflictException before this base exception.
    /// </exception>
    ValueTask<WorkflowResult> SubmitAsync(CancellationToken ct = default);
}

public interface IBatchBuilder : IWorkflowScope
{
    /// <summary>
    /// Adds one batch member per item, in list order. If any item fails serialization or <paramref name="options"/> fails validation, no members are
    /// added.
    /// </summary>
    IReadOnlyList<IJobStep> EnqueueMany<TArgs>(IReadOnlyList<TArgs> args, StepOptions? options = null);
}

/// <summary>A job, a batch, or an existing node selected by After. Its continuations belong to the same scope.</summary>
public interface INodeStep
{
    IJobStep ContinueWith<TArgs>(TArgs args, ContinuationCondition condition = ContinuationCondition.OnSuccess,
        StepOptions? options = null);

    IJobStep ContinueWith(string jobName, JsonElement args,
        ContinuationCondition condition = ContinuationCondition.OnSuccess, StepOptions? options = null);

    /// <summary>
    /// Adds a batch continuation in this node's scope. Add its members inside <paramref name="build"/>. If <paramref name="build"/> throws,
    /// including ArgumentException for an empty batch, the incomplete batch remains in the graph. Subsequent calls on the workflow or its steps,
    /// including SubmitAsync, throw InvalidOperationException.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="build"/> is null. The workflow remains usable.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The <paramref name="condition"/> value is unknown. The workflow remains usable.</exception>
    /// <exception cref="ArgumentException">The batch or a nested batch has no members. The workflow is unusable.</exception>
    /// <exception cref="InvalidOperationException">
    /// The node belongs to a closed batch, the workflow was submitted, or an earlier batch construction failed.
    /// </exception>
    IBatchStep ContinueWithBatch(Action<IBatchBuilder> build,
        ContinuationCondition condition = ContinuationCondition.OnSuccess);

    /// <summary>Delegates submission to <see cref="IWorkflow.SubmitAsync"/> and reports the same exceptions.</summary>
    /// <inheritdoc cref="IWorkflow.SubmitAsync" path="/exception"/>
    ValueTask<WorkflowResult> SubmitAsync(CancellationToken ct = default);
}

public interface IJobStep : INodeStep
{
    JobId Id { get; }
}

public interface IBatchStep : INodeStep
{
    BatchId Id { get; }
}

/// <summary>IDs of all jobs and batches in the graph, in step creation order.</summary>
public sealed record WorkflowResult(IReadOnlyList<JobId> Jobs, IReadOnlyList<BatchId> Batches);
