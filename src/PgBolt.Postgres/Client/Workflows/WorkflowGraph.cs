namespace PgBolt.Postgres.Client.Workflows;

/// <summary>Builder scope (§5.2), either the workflow root or a batch body. A batch body stays open only during its callback.</summary>
internal sealed class WorkflowScope(BatchNode? batch)
{
    public BatchNode? Batch { get; } = batch;
    public bool Open { get; set; } = batch is null;
}

/// <summary>A graph node representing a job, batch, or existing parent selected by After.</summary>
internal abstract class WorkflowNode(Guid id, WorkflowScope scope)
{
    public Guid Id { get; } = id;

    /// <summary>Scope in which the node was created and to which its continuations belong.</summary>
    public WorkflowScope Scope { get; } = scope;

    /// <summary>The node has children in the graph. For a job, this sets notify = true (§6.1, step 2).</summary>
    public bool HasChildren { get; set; }

    public abstract bool IsBatch { get; }
}

internal sealed class JobNode(Guid id, WorkflowScope scope, WorkflowNode? parent, ContinuationCondition condition,
    JobDescriptor? descriptor, string? jobName, string argsJson, StepOptions? options) : WorkflowNode(id, scope)
{
    public WorkflowNode? Parent { get; } = parent;
    public ContinuationCondition Condition { get; } = condition;

    /// <summary>Typed step metadata. Null denotes a dynamic step whose JobName defaults are loaded from job_types at submission.</summary>
    public JobDescriptor? Descriptor { get; } = descriptor;

    public string? JobName { get; } = jobName;
    public string ArgsJson { get; } = argsJson;
    public StepOptions? Options { get; } = options;
    public override bool IsBatch => false;
}

internal sealed class BatchNode : WorkflowNode
{
    public BatchNode(Guid id, WorkflowScope scope, WorkflowNode? parent, ContinuationCondition condition) : base(id, scope)
    {
        Parent = parent;
        Condition = condition;
        Awaiting = parent is not null || scope.Batch is { Awaiting: true };
        Body = new WorkflowScope(this);
    }

    public WorkflowNode? Parent { get; }
    public ContinuationCondition Condition { get; }

    /// <summary>Batch continuations and all batches nested inside them have state = 1 (§6.1, step 1).</summary>
    public bool Awaiting { get; }

    public WorkflowScope Body { get; }

    /// <summary>Direct members counted in total, including jobs and nested batches.</summary>
    public int Members { get; set; }

    public override bool IsBatch => true;
}

internal sealed class ExternalNode(Guid id, WorkflowScope scope, bool isBatch) : WorkflowNode(id, scope)
{
    public override bool IsBatch { get; } = isBatch;
}
