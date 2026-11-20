using System.Text.Json;
using Npgsql;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Client.Workflows;

/// <summary>
/// Fluent-построитель workflow: синхронный, не потокобезопасный граф узлов; id — при создании шага;
/// запись — SubmitAsync одной транзакцией (PgBoltStore.SubmitWorkflowAsync).
/// </summary>
internal sealed class WorkflowBuilder(PgBoltStore store, JobTypesCache jobTypes, IWakeupSignal wakeup, NpgsqlTransaction? transaction,
    PgBoltInstrumentation instrumentation, PgBoltClientMetrics metrics) : IWorkflow
{
    private readonly List<JobNode> _jobs = [];
    private readonly List<BatchNode> _batches = [];
    private readonly List<ExternalNode> _external = [];
    private readonly HashSet<string> _uniqueKeys = new(StringComparer.Ordinal);
    private readonly WorkflowScope _root = new(null);
    private bool _submitted;
    private bool _broken;

    public IJobStep Enqueue<TArgs>(TArgs args, StepOptions? options = null) => EnqueueIn(_root, args, options, null, default);

    public IJobStep Enqueue(string jobName, JsonElement args, StepOptions? options = null) =>
        EnqueueIn(_root, jobName, args, options, null, default);

    public IBatchStep StartBatch(Action<IBatchBuilder> build) => StartBatchIn(_root, null, default, build);

    public INodeStep After(JobId existing) => AddExternal(existing.Value, isBatch: false);

    public INodeStep After(BatchId existing) => AddExternal(existing.Value, isBatch: true);

    public ValueTask<WorkflowResult> SubmitAsync(CancellationToken ct = default) => new(SubmitCoreAsync(ct));

    internal IJobStep EnqueueIn<TArgs>(WorkflowScope scope, TArgs args, StepOptions? options, WorkflowNode? parent,
        ContinuationCondition condition)
    {
        EnsureUsable(scope);
        var (descriptor, typeInfo) = JobMetadataAccess.Get<TArgs>();
        return AddJob(scope, parent, condition, descriptor, null, JsonSerializer.Serialize(args, typeInfo), options);
    }

    internal IJobStep EnqueueIn(WorkflowScope scope, string jobName, JsonElement args, StepOptions? options, WorkflowNode? parent,
        ContinuationCondition condition)
    {
        EnsureUsable(scope);
        ArgumentException.ThrowIfNullOrEmpty(jobName);
        return AddJob(scope, parent, condition, null, jobName, args.GetRawText(), options);
    }

    /// <summary>Атомарно: все элементы сериализуются до добавления первого члена — исключение не оставляет в batch части списка.</summary>
    internal IReadOnlyList<IJobStep> EnqueueManyIn<TArgs>(WorkflowScope scope, IReadOnlyList<TArgs> args, StepOptions? options)
    {
        EnsureUsable(scope);
        ArgumentNullException.ThrowIfNull(args);
        var (descriptor, typeInfo) = JobMetadataAccess.Get<TArgs>();
        var json = new string[args.Count];
        for (var i = 0; i < json.Length; i++)
            json[i] = JsonSerializer.Serialize(args[i], typeInfo);
        var steps = new IJobStep[json.Length];
        for (var i = 0; i < steps.Length; i++)
            steps[i] = AddJob(scope, null, default, descriptor, null, json[i], options);
        return steps;
    }

    internal IBatchStep StartBatchIn(WorkflowScope scope, WorkflowNode? parent, ContinuationCondition condition, Action<IBatchBuilder> build)
    {
        EnsureUsable(scope);
        ArgumentNullException.ThrowIfNull(build);
        EnsureCondition(condition);
        var node = new BatchNode(Guid.CreateVersion7(), scope, parent, condition);
        Attach(node, parent);
        _batches.Add(node);
        node.Body.Open = true;
        try
        {
            build(new BatchBuilder(this, node));
        }
        catch
        {
            _broken = true;   // недостроенный batch уже в графе
            throw;
        }
        finally
        {
            node.Body.Open = false;
        }
        if (node.Members == 0)
        {
            _broken = true;
            throw new ArgumentException("A workflow batch must have at least one member.", nameof(build));
        }
        return new BatchStep(this, node);
    }

    private ExternalStep AddExternal(Guid id, bool isBatch)
    {
        EnsureUsable(_root);
        var node = new ExternalNode(id, _root, isBatch);
        _external.Add(node);
        return new ExternalStep(this, node);
    }

    private JobStep AddJob(WorkflowScope scope, WorkflowNode? parent, ContinuationCondition condition, JobDescriptor? descriptor,
        string? jobName, string argsJson, StepOptions? options)
    {
        EnsureCondition(condition);
        ValidateOptions(scope, parent, options);
        var node = new JobNode(Guid.CreateVersion7(), scope, parent, condition, descriptor, jobName, argsJson, options);
        Attach(node, parent);
        _jobs.Add(node);
        return new JobStep(this, node);
    }

    private static void Attach(WorkflowNode node, WorkflowNode? parent)
    {
        if (parent is not null)
            parent.HasChildren = true;
        if (node.Scope.Batch is { } batch)
            batch.Members++;
    }

    private void EnsureUsable(WorkflowScope scope)
    {
        if (_submitted)
            throw new InvalidOperationException("The workflow has already been submitted; create a new one with IJobClient.Workflow().");
        if (_broken)
            throw new InvalidOperationException("The workflow is unusable after a failed batch definition; create a new one.");
        if (!scope.Open)
            throw new InvalidOperationException("The batch is closed: its members can be added only inside its build lambda.");
    }

    private static void EnsureCondition(ContinuationCondition condition)
    {
        if (!Enum.IsDefined(condition))
            throw new ArgumentOutOfRangeException(nameof(condition), condition, "Unknown continuation condition.");
    }

    /// <summary>Значения опций, RunAt/Delay только без родителя и вне ждущих batch, UniqueKey не у членов и без повторов в графе.</summary>
    private void ValidateOptions(WorkflowScope scope, WorkflowNode? parent, StepOptions? options)
    {
        if (options is null)
            return;
        EnqueueOptionsResolver.ValidateValues(nameof(StepOptions), options.RunAt, options.Delay, options.MaxAttempts, options.Queue,
            nameof(options));
        if (options.RetryDelays is { } delays && delays.Any(d => d < TimeSpan.Zero))
            throw new ArgumentException("StepOptions.RetryDelays must be non-negative.", nameof(options));
        if ((options.RunAt is not null || options.Delay is not null) && (parent is not null || scope.Batch is { Awaiting: true }))
            throw new ArgumentException(
                "StepOptions.RunAt and StepOptions.Delay are allowed only for root jobs and members of batches without a parent: " +
                "continuations start when their condition resolves.", nameof(options));
        if (options.UniqueKey is not { } key)
            return;
        if (scope.Batch is not null)
            throw new ArgumentException("StepOptions.UniqueKey is not allowed for batch members: a conflict would change the batch total.",
                nameof(options));
        ReservedUniqueKeys.EnsureUserKeys([key]);
        if (!_uniqueKeys.Add(key))
            throw new ArgumentException($"Unique key '{key}' is used by more than one step of the workflow.", nameof(options));
    }

    private async Task<WorkflowResult> SubmitCoreAsync(CancellationToken ct)
    {
        using var span = ProducerSpan.Start(instrumentation, operation: "workflow submit", queue: null, jobType: null, _jobs.Count, recurringId: null);
        try
        {
            EnsureUsable(_root);
            _submitted = true;
            if (_batches.Exists(b => b.Body.Open))
                throw new InvalidOperationException("SubmitAsync cannot be called inside a batch build lambda.");
            if (_jobs.Count == 0 && _batches.Count == 0)
                throw new ArgumentException("The workflow has no steps.");
            foreach (var e in _external)
                if (!e.HasChildren)
                    throw new ArgumentException($"After({e.Id}) has no continuations.");

            var dynamic = new Dictionary<string, JobDescriptor>(StringComparer.Ordinal);
            foreach (var job in _jobs)
            {
                if (job.Descriptor is not null || dynamic.ContainsKey(job.JobName!))
                    continue;
                dynamic[job.JobName!] = await jobTypes.GetDescriptorAsync(job.JobName!, ct);
            }
            if (transaction is not null)
                await PgBoltJobClient.EnsureReadCommittedAsync(transaction, ct);

            var (plan, readyQueues) = WorkflowPlanner.Build(_batches, _jobs, _external, dynamic, span.TraceParent);
            if (plan.Jobs is [var first, ..] && plan.Jobs.All(j => j.Queue == first.Queue))
                span.SetQueue(first.Queue);
            await store.SubmitWorkflowAsync(plan, transaction, ct);
            foreach (var g in plan.Jobs.GroupBy(j => (j.Queue, j.Type)))
                metrics.Enqueued(g.Key.Queue, g.Key.Type, EnqueueResults.Created, g.Sum(j => j.Ids.Length));
            if (transaction is null)
                foreach (var queue in readyQueues)
                    wakeup.Signal(queue);   // только после своего коммита; сигналы для resolver'а store шлёт сам (синтетические)

            return new WorkflowResult([.. _jobs.Select(j => new JobId(j.Id))], [.. _batches.Select(b => new BatchId(b.Id))]);
        }
        catch (Exception e) { span.Fail(e); throw; }
    }
}

/// <summary>Методы узла: продолжения добавляются в область узла, где бы их ни вызвали.</summary>
internal abstract class NodeStep(WorkflowBuilder workflow, WorkflowNode node) : INodeStep
{
    public IJobStep ContinueWith<TArgs>(TArgs args, ContinuationCondition condition = ContinuationCondition.OnSuccess,
        StepOptions? options = null) =>
        workflow.EnqueueIn(node.Scope, args, options, node, condition);

    public IJobStep ContinueWith(string jobName, JsonElement args, ContinuationCondition condition = ContinuationCondition.OnSuccess,
        StepOptions? options = null) =>
        workflow.EnqueueIn(node.Scope, jobName, args, options, node, condition);

    public IBatchStep ContinueWithBatch(Action<IBatchBuilder> build, ContinuationCondition condition = ContinuationCondition.OnSuccess) =>
        workflow.StartBatchIn(node.Scope, node, condition, build);

    public ValueTask<WorkflowResult> SubmitAsync(CancellationToken ct = default) => workflow.SubmitAsync(ct);
}

internal sealed class JobStep(WorkflowBuilder workflow, JobNode node) : NodeStep(workflow, node), IJobStep
{
    public JobId Id { get; } = new(node.Id);
}

internal sealed class BatchStep(WorkflowBuilder workflow, BatchNode node) : NodeStep(workflow, node), IBatchStep
{
    public BatchId Id { get; } = new(node.Id);
}

internal sealed class ExternalStep(WorkflowBuilder workflow, ExternalNode node) : NodeStep(workflow, node);

/// <summary>Тело batch: всё добавляется в его область (члены batch).</summary>
internal sealed class BatchBuilder(WorkflowBuilder workflow, BatchNode batch) : IBatchBuilder
{
    public IJobStep Enqueue<TArgs>(TArgs args, StepOptions? options = null) => workflow.EnqueueIn(batch.Body, args, options, null, default);

    public IJobStep Enqueue(string jobName, JsonElement args, StepOptions? options = null) =>
        workflow.EnqueueIn(batch.Body, jobName, args, options, null, default);

    public IBatchStep StartBatch(Action<IBatchBuilder> build) => workflow.StartBatchIn(batch.Body, null, default, build);

    public IReadOnlyList<IJobStep> EnqueueMany<TArgs>(IReadOnlyList<TArgs> args, StepOptions? options = null) =>
        workflow.EnqueueManyIn(batch.Body, args, options);
}
