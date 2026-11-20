using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace PgBolt.Dashboard.Tests.Support;

/// <summary>IJobClient хоста, обёрнутый тестом: переопределённые члены подменяют поведение, остальные идут в core.</summary>
public class DelegatingJobClient(IJobClient inner) : IJobClient
{
    /// <summary>Заменяет IJobClient хоста обёрткой wrap над клиентом core.</summary>
    public static void Replace(WebApplicationBuilder b, Func<IJobClient, IJobClient> wrap)
    {
        var core = b.Services.Last(d => d.ServiceType == typeof(IJobClient)).ImplementationFactory!;
        b.Services.AddSingleton(sp => wrap((IJobClient)core(sp)));
    }

    public virtual ValueTask<EnqueueResult> EnqueueAsync(string jobName, JsonElement args, EnqueueOptions? options = null,
        CancellationToken ct = default) => inner.EnqueueAsync(jobName, args, options, ct);

    public ValueTask<EnqueueResult> EnqueueAsync<TArgs>(TArgs args, EnqueueOptions? options = null, CancellationToken ct = default) =>
        inner.EnqueueAsync(args, options, ct);

    public ValueTask<IReadOnlyList<EnqueueResult>> EnqueueManyAsync<TArgs>(IReadOnlyList<TArgs> args, EnqueueOptions? options = null,
        Func<TArgs, string?>? uniqueKeySelector = null, CancellationToken ct = default) =>
        inner.EnqueueManyAsync(args, options, uniqueKeySelector, ct);

    public ValueTask<CancelResult> CancelAsync(JobId id, string? reason = null, CancellationToken ct = default) => inner.CancelAsync(id, reason, ct);

    public ValueTask<bool> TriggerAsync(JobId id, CancellationToken ct = default) => inner.TriggerAsync(id, ct);

    public ValueTask<RequeueResult> RequeueAsync(JobId id, CancellationToken ct = default) => inner.RequeueAsync(id, ct);

    public virtual ValueTask<JobInfo?> GetAsync(JobId id, CancellationToken ct = default) => inner.GetAsync(id, ct);

    public IWorkflow Workflow(WorkflowOptions? options = null) => inner.Workflow(options);

    public ValueTask<BatchCancelResult> CancelBatchAsync(BatchId id, CancellationToken ct = default) => inner.CancelBatchAsync(id, ct);

    public ValueTask<BatchInfo?> GetBatchAsync(BatchId id, CancellationToken ct = default) => inner.GetBatchAsync(id, ct);
}
