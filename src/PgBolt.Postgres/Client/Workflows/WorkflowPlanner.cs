using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Client.Workflows;

/// <summary>
/// Граф → план записи (§6.1): batches с состоянием и total; задачи с кодом continuation (5 — член ждущего batch без родителя,
/// parent_id = его batch), notify (член batch или есть дети), группы enqueue по (type, queue, priority, max_attempts,
/// retry_delays, delay) по 10 000 строк; внешние родители — различные, в порядке id (Guid.CompareTo совпадает с порядком uuid).
/// </summary>
internal static class WorkflowPlanner
{
    internal const int MaxRowsPerCommand = 10_000;

    private sealed record JobRow(Guid Id, string Type, string Queue, short Priority, int MaxAttempts, int[]? RetryDelays,
        string ArgsJson, DateTime? RunAtUtc, TimeSpan Delay, string? UniqueKey, Guid? ParentId, short? Continuation, bool Notify,
        Guid? BatchId);

    public static (WorkflowPlan Plan, IReadOnlyCollection<string> ReadyQueues) Build(IReadOnlyList<BatchNode> batches,
        IReadOnlyList<JobNode> jobs, IReadOnlyList<ExternalNode> external, IReadOnlyDictionary<string, JobDescriptor> dynamic,
        string? traceParent)
    {
        var batchRows = batches
            .Select(b => new WorkflowBatchRow(b.Id, b.Scope.Batch?.Id, b.Parent?.Id,
                b.Parent is { } p ? WorkflowCodes.Code(b.Condition, p.IsBatch) : null,
                b.Awaiting ? WorkflowCodes.BatchAwaiting : WorkflowCodes.BatchActive, b.Members))
            .ToList();

        var rows = jobs.Select(j => Row(j, dynamic)).ToList();
        var groups = new List<EnqueueBatch>();
        foreach (var g in rows.GroupBy(r => (r.Type, r.Queue, r.Priority, r.MaxAttempts,
                     Retry: r.RetryDelays is { } d ? string.Join(',', d) : "-", r.Delay)))
        {
            foreach (var chunk in g.Chunk(MaxRowsPerCommand))
            {
                groups.Add(new EnqueueBatch(g.Key.Type, g.Key.Queue, g.Key.Priority, g.Key.MaxAttempts, chunk[0].RetryDelays, traceParent,
                    g.Key.Delay, [.. chunk.Select(r => r.Id)], [.. chunk.Select(r => r.ArgsJson)], [.. chunk.Select(r => r.RunAtUtc)],
                    [.. chunk.Select(r => r.UniqueKey)])
                {
                    ParentIds = [.. chunk.Select(r => r.ParentId)],
                    Continuations = [.. chunk.Select(r => r.Continuation)],
                    Notify = [.. chunk.Select(r => r.Notify)],
                    BatchIds = [.. chunk.Select(r => r.BatchId)],
                });
            }
        }

        var parents = external.Select(e => new WorkflowParent(e.Id, e.IsBatch)).Distinct().OrderBy(p => p.Id).ToList();
        var ready = rows.Where(r => r.Continuation is null && r.RunAtUtc is null && r.Delay == TimeSpan.Zero)
            .Select(r => r.Queue).ToHashSet(StringComparer.Ordinal);
        return (new WorkflowPlan(batchRows, groups, parents), ready);
    }

    private static JobRow Row(JobNode job, IReadOnlyDictionary<string, JobDescriptor> dynamic)
    {
        var d = job.Descriptor ?? dynamic[job.JobName!];
        var o = job.Options;
        var activation = job.Parent is null && job.Scope.Batch is { Awaiting: true };
        short? continuation = job.Parent is { } parent
            ? WorkflowCodes.Code(job.Condition, parent.IsBatch)
            : activation ? WorkflowCodes.Activation : (short?)null;
        var parentId = job.Parent?.Id ?? (activation ? job.Scope.Batch!.Id : (Guid?)null);
        return new JobRow(job.Id, d.Name, o?.Queue ?? d.Queue, o?.Priority ?? d.Priority, o?.MaxAttempts ?? d.MaxAttempts,
            o?.RetryDelays is { } delays ? [.. delays.Select(EnqueueOptionsResolver.ToSeconds)] : d.RetryDelaysSeconds, job.ArgsJson,
            o?.RunAt?.UtcDateTime, o?.Delay ?? TimeSpan.Zero, o?.UniqueKey, parentId, continuation,
            Notify: job.Scope.Batch is not null || job.HasChildren, job.Scope.Batch?.Id);
    }
}
