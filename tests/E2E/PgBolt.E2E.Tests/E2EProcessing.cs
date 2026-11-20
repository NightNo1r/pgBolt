using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.Postgres.Jobs;
using PgBolt.Postgres.Storage;

namespace PgBolt.E2E.Tests;

/// <summary>Обработка без PgBolt.Server: выполнение задачи executor'ом из реестра и проход dequeue → выполнение → ack.</summary>
internal static class E2EProcessing
{
    /// <summary>Число подтверждённых задач из dequeue до 10 задач очереди e2e; пустой dequeue — падение теста.</summary>
    public static async Task<int> ProcessOnceAsync(IServiceProvider sp, Guid server, CancellationToken ct)
    {
        var store = sp.GetRequiredService<PgBoltStore>();
        var leased = await store.DequeueAsync("e2e", 10, server, TimeSpan.FromMinutes(1), ct);
        Assert.NotEmpty(leased);
        foreach (var job in leased)
            await InvokeAsync(sp, job, ct);
        return (await store.AckAsync(server, leased.Select(j => new LeaseRef(j.Id, j.LeaseId)).ToList(), ct)).Count;
    }

    /// <summary>Выполняет задачу в своём scope с JobContext как у PgBolt.Server: родители workflow, batch, поля расписания.</summary>
    public static async Task InvokeAsync(IServiceProvider sp, LeasedJob job, CancellationToken ct)
    {
        Assert.True(sp.GetRequiredService<JobRegistry>().TryGet(job.Type, out var executor), job.Type);
        await using var scope = sp.CreateAsyncScope();
        var context = new JobContext(new JobId(job.Id), job.Type, job.Attempt, job.MaxAttempts,
            WorkflowCodes.ParentJob(job.Continuation, job.ParentId), job.BatchId is { } batch ? new BatchId(batch) : null,
            NullLogger.Instance, job.ScheduledAtUtc is { } scheduled ? new DateTimeOffset(scheduled, TimeSpan.Zero) : null,
            job.RecurringId, WorkflowCodes.ParentBatch(job.Continuation, job.ParentId));
        await executor.Invoke(scope.ServiceProvider, Encoding.UTF8.GetBytes(job.ArgsJson), context, ct);
    }
}
