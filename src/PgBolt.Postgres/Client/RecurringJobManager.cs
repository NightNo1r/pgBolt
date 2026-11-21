using System.Text.Json;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Client;

/// <summary>Runtime API for recurring schedules. Validates before accessing the database.</summary>
internal sealed class RecurringJobManager(PgBoltStore store, JobTypesCache jobTypes, IWakeupSignal wakeup,
    PgBoltInstrumentation instrumentation, PgBoltClientMetrics metrics) : IRecurringJobManager
{
    public async ValueTask AddOrUpdateAsync<TArgs>(string id, string cron, TArgs args, RecurringJobOptions? options = null,
        CancellationToken ct = default)
    {
        RecurringDefinition.Validate(id, cron, options);
        var (descriptor, typeInfo) = JobMetadataAccess.Get<TArgs>();
        await UpsertAsync(RecurringDefinition.Create(id, descriptor.Name, JsonSerializer.Serialize(args, typeInfo), cron, options), ct);
    }

    public async ValueTask AddOrUpdateAsync(string id, string jobName, string cron, JsonElement args, RecurringJobOptions? options = null,
        CancellationToken ct = default)
    {
        var definition = RecurringDefinition.Create(id, jobName, args.GetRawText(), cron, options);
        if (await jobTypes.GetAsync(jobName, ct) is null)
            throw new UnknownJobException(jobName);
        await UpsertAsync(definition, ct);
    }

    public async ValueTask<bool> RemoveAsync(string id, CancellationToken ct = default) =>
        await store.RemoveRecurringAsync(id, ct) switch
        {
            RecurringRemoveResult.Removed => true,
            RecurringRemoveResult.NotFound => false,
            _ => throw OwnedByCodeFirst(id),
        };

    public async ValueTask<bool> PauseAsync(string id, CancellationToken ct = default) => await store.PauseRecurringAsync(id, ct);

    public async ValueTask<bool> ResumeAsync(string id, CancellationToken ct = default) => await store.ResumeRecurringAsync(id, ct);

    public async ValueTask<RecurringJobInfo?> GetAsync(string id, CancellationToken ct = default) => await store.GetRecurringAsync(id, ct);

    /// <summary>
    /// Uses the normal client enqueue path, including conflict resolution, and sets scheduled_at to the database's now(). Leaves the schedule row
    /// untouched to preserve the tick's lock order. Ignores paused.
    /// </summary>
    public async ValueTask<EnqueueResult?> TriggerAsync(string id, CancellationToken ct = default)
    {
        using var span = ProducerSpan.Start(instrumentation, operation: "recurring trigger", queue: null, jobType: null, messageCount: 1, recurringId: id);
        try
        {
            if (await store.GetRecurringForTriggerAsync(id, ct) is not { } row)
                return null;
            span.SetQueue(row.Queue);
            if (row.UnknownType)
                throw new UnknownJobException(row.Type);

            var batch = new EnqueueBatch(row.Type, row.Queue!, row.Priority!.Value, row.MaxAttempts!.Value, row.RetryDelaysSeconds,
                span.TraceParent, TimeSpan.Zero, [Guid.CreateVersion7()], [row.ArgsJson], [null],
                [row.Overlap == OverlapPolicy.Skip ? ReservedUniqueKeys.Recurring(id) : null])
            {
                RecurringIds = [id],
                ScheduledAtUtc = [null],
            };
            var result = (await store.EnqueueAsync(batch, null, ct))[0];
            metrics.Enqueued(row.Queue!, row.Type, result.Created ? EnqueueResults.Created : EnqueueResults.Duplicate, 1);
            if (result.Created)
                wakeup.Signal(row.Queue!);
            return new EnqueueResult(result.Id is { } jobId ? new JobId(jobId) : null, result.Created);
        }
        catch (Exception e) { span.Fail(e); throw; }
    }

    private async Task UpsertAsync(RecurringDefinition definition, CancellationToken ct)
    {
        if (await store.UpsertRecurringAsync(definition, ct) == RecurringUpsertResult.OwnedByCodeFirst)
            throw OwnedByCodeFirst(definition.Id);
    }

    private static InvalidOperationException OwnedByCodeFirst(string id) =>
        new($"Recurring job '{id}' is declared in code (AddRecurring) and managed by its owner's synchronization; " +
            "only Pause, Resume, Trigger and Get are allowed through IRecurringJobManager.");
}
