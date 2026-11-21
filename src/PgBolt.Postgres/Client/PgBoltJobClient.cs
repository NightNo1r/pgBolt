using System.Data;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Npgsql;
using PgBolt.Postgres.Client.Workflows;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Client;

internal sealed partial class PgBoltJobClient(PgBoltStore store, JobTypesCache jobTypes, IWakeupSignal wakeup,
    PgBoltInstrumentation instrumentation, PgBoltClientMetrics metrics) : IJobClient
{
    private ProducerSpan StartSpan(string type, int messageCount) =>
        ProducerSpan.Start(instrumentation, operation: "enqueue", queue: null, jobType: type, messageCount, recurringId: null);

    public async ValueTask<EnqueueResult> EnqueueAsync<TArgs>(TArgs args, EnqueueOptions? options = null, CancellationToken ct = default)
    {
        var (descriptor, typeInfo) = JobMetadataAccess.Get<TArgs>();
        using var span = StartSpan(descriptor.Name, 1);
        try
        {
            var results = await EnqueueCoreAsync(descriptor, [JsonSerializer.Serialize(args, typeInfo)], [options?.UniqueKey], options, span, ct);
            return results[0];
        }
        catch (Exception e) { span.Fail(e); throw; }
    }

    public async ValueTask<IReadOnlyList<EnqueueResult>> EnqueueManyAsync<TArgs>(
        IReadOnlyList<TArgs> args, EnqueueOptions? options = null,
        Func<TArgs, string?>? uniqueKeySelector = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        if (options?.UniqueKey is not null)
            throw new ArgumentException("EnqueueOptions.UniqueKey is not allowed in EnqueueManyAsync; use uniqueKeySelector.", nameof(options));
        if (args.Count == 0)
            return [];

        var (descriptor, typeInfo) = JobMetadataAccess.Get<TArgs>();
        using var span = StartSpan(descriptor.Name, args.Count);
        try
        {
            var json = new string[args.Count];
            var keys = new string?[args.Count];
            for (var i = 0; i < args.Count; i++)
            {
                json[i] = JsonSerializer.Serialize(args[i], typeInfo);
                keys[i] = uniqueKeySelector?.Invoke(args[i]);
            }
            return await EnqueueCoreAsync(descriptor, json, keys, options, span, ct);
        }
        catch (Exception e) { span.Fail(e); throw; }
    }

    public async ValueTask<EnqueueResult> EnqueueAsync(string jobName, JsonElement args, EnqueueOptions? options = null, CancellationToken ct = default)
    {
        using var span = StartSpan(jobName, 1);
        try
        {
            var descriptor = await jobTypes.GetDescriptorAsync(jobName, ct);
            var results = await EnqueueCoreAsync(descriptor, [args.GetRawText()], [options?.UniqueKey], options, span, ct);
            return results[0];
        }
        catch (Exception e) { span.Fail(e); throw; }
    }

    private async ValueTask<IReadOnlyList<EnqueueResult>> EnqueueCoreAsync(
        JobDescriptor descriptor, string[] argsJson, string?[] uniqueKeys, EnqueueOptions? options, ProducerSpan span, CancellationToken ct)
    {
        ReservedUniqueKeys.EnsureUserKeys(uniqueKeys);
        var o = EnqueueOptionsResolver.Resolve(descriptor, options);
        span.SetQueue(o.Queue);
        if (o.Transaction is not null)
            await EnsureReadCommittedAsync(o.Transaction, ct);

        var ids = new Guid[argsJson.Length];
        for (var i = 0; i < ids.Length; i++)
            ids[i] = Guid.CreateVersion7();
        var runAt = new DateTime?[argsJson.Length];
        if (o.RunAtUtc is { } at)
            Array.Fill(runAt, at);

        var batch = new EnqueueBatch(descriptor.Name, o.Queue, o.Priority, o.MaxAttempts, o.RetryDelaysSeconds,
            span.TraceParent, o.Delay, ids, argsJson, runAt, uniqueKeys);
        var rows = await store.EnqueueAsync(batch, o.Transaction, ct);

        var created = rows.Count(r => r.Created);
        metrics.Enqueued(o.Queue, descriptor.Name, EnqueueResults.Created, created);
        metrics.Enqueued(o.Queue, descriptor.Name, EnqueueResults.Duplicate, rows.Count - created);

        if (o.Transaction is null && created > 0)
            wakeup.Signal(o.Queue);

        var results = new EnqueueResult[rows.Count];
        for (var i = 0; i < rows.Count; i++)
            results[i] = new EnqueueResult(rows[i].Id is { } id ? new JobId(id) : null, rows[i].Created);
        return results;
    }

    /// <summary>
    /// Accepts only READ COMMITTED. Explicitly requesting another level fails immediately. Otherwise reads isolation from the server every time.
    /// Npgsql reports ReadCommitted for BeginTransaction() without a level, and SET TRANSACTION ISOLATION LEVEL can change it without updating
    /// NpgsqlTransaction.IsolationLevel.
    /// </summary>
    internal static async Task EnsureReadCommittedAsync(NpgsqlTransaction tx, CancellationToken ct)
    {
        if (tx.IsolationLevel is not (IsolationLevel.ReadCommitted or IsolationLevel.Unspecified))
            throw new NotSupportedException($"PgBolt transactional enqueue requires READ COMMITTED isolation, got {tx.IsolationLevel}.");

        await using var cmd = new NpgsqlCommand("SELECT current_setting('transaction_isolation')", tx.Connection, tx);
        if (await cmd.ExecuteScalarAsync(ct) is not "read committed")
            throw new NotSupportedException("PgBolt transactional enqueue requires READ COMMITTED isolation.");
    }

    public async ValueTask<CancelResult> CancelAsync(JobId id, string? reason = null, CancellationToken ct = default) =>
        await store.CancelAsync(id.Value, reason, ct);

    public async ValueTask<bool> TriggerAsync(JobId id, CancellationToken ct = default)
    {
        var queue = await store.TriggerAsync(id.Value, ct);
        if (queue is null)
            return false;
        wakeup.Signal(queue);
        return true;
    }

    public async ValueTask<RequeueResult> RequeueAsync(JobId id, CancellationToken ct = default)
    {
        var (result, queue) = await store.RequeueAsync(id.Value, ct);
        if (result == RequeueResult.Requeued && queue is not null)
            wakeup.Signal(queue);
        return result;
    }

    public async ValueTask<JobInfo?> GetAsync(JobId id, CancellationToken ct = default) =>
        await store.GetJobAsync(id.Value, ct);

    public IWorkflow Workflow(WorkflowOptions? options = null)
    {
        var transaction = options?.Transaction switch
        {
            null => null,
            NpgsqlTransaction t => t,
            _ => throw new ArgumentException("WorkflowOptions.Transaction must be an NpgsqlTransaction.", nameof(options)),
        };
        return new WorkflowBuilder(store, jobTypes, wakeup, transaction, instrumentation, metrics);
    }

    public async ValueTask<BatchCancelResult> CancelBatchAsync(BatchId id, CancellationToken ct = default) =>
        await store.CancelBatchAsync(id.Value, ct);

    public async ValueTask<BatchInfo?> GetBatchAsync(BatchId id, CancellationToken ct = default) =>
        await store.GetBatchAsync(id.Value, ct);
}
