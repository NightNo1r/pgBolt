namespace PgBolt.Postgres.Client.Workflows;

/// <summary>Область построителя (§5.2): корень workflow или тело batch. Тело открыто только на время лямбды batch.</summary>
internal sealed class WorkflowScope(BatchNode? batch)
{
    public BatchNode? Batch { get; } = batch;
    public bool Open { get; set; } = batch is null;
}

/// <summary>Узел графа: задача, batch или существующий родитель (After).</summary>
internal abstract class WorkflowNode(Guid id, WorkflowScope scope)
{
    public Guid Id { get; } = id;

    /// <summary>Область, в которой узел создан; продолжения узла добавляются в неё же.</summary>
    public WorkflowScope Scope { get; } = scope;

    /// <summary>У узла есть дети в графе: у задачи — notify = true (§6.1 п. 2).</summary>
    public bool HasChildren { get; set; }

    public abstract bool IsBatch { get; }
}

internal sealed class JobNode(Guid id, WorkflowScope scope, WorkflowNode? parent, ContinuationCondition condition,
    JobDescriptor? descriptor, string? jobName, string argsJson, StepOptions? options) : WorkflowNode(id, scope)
{
    public WorkflowNode? Parent { get; } = parent;
    public ContinuationCondition Condition { get; } = condition;

    /// <summary>Типизированный шаг; null — динамический (JobName, умолчания из job_types при SubmitAsync).</summary>
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

    /// <summary>§6.1 п. 1: state = 1 у batch-продолжений и у вложенных в них на любой глубине.</summary>
    public bool Awaiting { get; }

    public WorkflowScope Body { get; }

    /// <summary>Прямые члены: задачи и вложенные batches (total).</summary>
    public int Members { get; set; }

    public override bool IsBatch => true;
}

internal sealed class ExternalNode(Guid id, WorkflowScope scope, bool isBatch) : WorkflowNode(id, scope)
{
    public override bool IsBatch { get; } = isBatch;
}
