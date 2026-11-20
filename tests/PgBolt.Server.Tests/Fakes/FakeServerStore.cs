using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Fakes;

internal sealed record StoreCall(string Op, Guid? JobId = null, Guid? LeaseId = null, object? Detail = null);

/// <summary>Detail вызова TryClaimMaintenanceAsync.</summary>
internal sealed record ClaimCall(string Name, TimeSpan Interval);

/// <summary>Detail вызова CompleteMaintenanceAsync.</summary>
internal sealed record CompleteCall(string Name, DateTime ClaimedAt, string? Error, TimeSpan? RetryDelay);

/// <summary>Программируемый IServerStore: поведение задаётся делегатами, все вызовы пишутся в Calls до вызова делегата.</summary>
internal sealed class FakeServerStore : IServerStore
{
    public ConcurrentQueue<StoreCall> Calls { get; } = new();

    public Func<string, int, CancellationToken, Task<IReadOnlyList<LeasedJob>>> OnDequeue { get; set; } =
        (_, _, _) => Task.FromResult<IReadOnlyList<LeasedJob>>([]);

    public Func<string, int, CancellationToken, Task<LimitedDequeueResult>> OnDequeueLimited { get; set; } =
        (_, _, _) => Task.FromResult(new LimitedDequeueResult([], false));

    public Func<IReadOnlyList<QueueDefault>, string, CancellationToken, Task<IReadOnlyList<QueueDefaultConflict>>> OnSyncQueueDefaults { get; set; } =
        (_, _, _) => Task.FromResult<IReadOnlyList<QueueDefaultConflict>>([]);

    public Func<CancellationToken, Task<IReadOnlyList<ActiveQueueSetting>>> OnGetActiveQueueSettings { get; set; } =
        _ => Task.FromResult<IReadOnlyList<ActiveQueueSetting>>([]);

    public Func<IReadOnlyList<LeaseRef>, CancellationToken, Task<IReadOnlyList<Guid>>> OnAck { get; set; } =
        (leases, _) => AckAll(leases);

    public Func<Guid, Guid, CancellationToken, Task<FailOutcome>> OnRetry { get; set; } =
        (_, _, _) => Task.FromResult(FailOutcome.Retried);

    public Func<Guid, Guid, CancellationToken, Task<FailOutcome>> OnFail { get; set; } =
        (_, _, _) => Task.FromResult(FailOutcome.Failed);

    public Func<Guid, Guid, CancellationToken, Task<FailOutcome>> OnConfirmCancel { get; set; } =
        (_, _, _) => Task.FromResult(FailOutcome.Cancelled);

    public Func<Guid, Guid, bool, CancellationToken, Task<ReleaseOutcome>> OnRelease { get; set; } =
        (_, _, _, _) => Task.FromResult(ReleaseOutcome.Released);

    public Func<IReadOnlyCollection<LeaseRef>, CancellationToken, Task<IReadOnlyList<HeartbeatLease>>> OnHeartbeat { get; set; } =
        (leases, _) => Task.FromResult(AllAlive(leases));

    public Func<int, CancellationToken, Task<PromoteResult>> OnPromote { get; set; } =
        (_, _) => Task.FromResult(new PromoteResult(0, []));

    public Func<int, CancellationToken, Task<int>> OnSweep { get; set; } = (_, _) => Task.FromResult(0);

    public Func<IReadOnlyList<string>, CancellationToken, Task<IReadOnlyList<string>>> OnJobTypeNames { get; set; } =
        (_, _) => Task.FromResult<IReadOnlyList<string>>([]);

    public Func<ServerRegistration, CancellationToken, Task> OnRegisterServer { get; set; } = (_, _) => Task.CompletedTask;

    public Func<Guid, CancellationToken, Task> OnRemoveServer { get; set; } = (_, _) => Task.CompletedTask;

    /// <summary>По умолчанию задачи обслуживания не захватываются (ни один тест, кроме housekeeping, их не ждёт).</summary>
    public Func<string, TimeSpan, CancellationToken, Task<DateTime?>> OnClaim { get; set; } =
        (_, _, _) => Task.FromResult<DateTime?>(null);

    public Func<string, DateTime, string?, TimeSpan?, CancellationToken, Task> OnComplete { get; set; } =
        (_, _, _, _, _) => Task.CompletedTask;

    public Func<IReadOnlyList<OutcomeStatsRow>, CancellationToken, Task> OnFlushStats { get; set; } = (_, _) => Task.CompletedTask;

    public Func<CancellationToken, Task> OnSnapshot { get; set; } = _ => Task.CompletedTask;

    public Func<CancellationToken, Task<QueueStatsSnapshot>> OnReadQueueStats { get; set; } =
        _ => Task.FromResult(new QueueStatsSnapshot([], 0));

    public Func<CancellationToken, Task<IReadOnlyList<TableStatsRow>>> OnReadTableStats { get; set; } =
        _ => Task.FromResult<IReadOnlyList<TableStatsRow>>([]);

    public Func<TimeSpan, int, CancellationToken, Task<int>> OnTrim { get; set; } = (_, _, _) => Task.FromResult(0);

    /// <summary>Ответ ack «подтверждены все задачи пакета».</summary>
    public static Task<IReadOnlyList<Guid>> AckAll(IReadOnlyList<LeaseRef> leases) =>
        Task.FromResult<IReadOnlyList<Guid>>([.. leases.Select(l => l.JobId)]);

    /// <summary>Ответ heartbeat «все lease живы, отмены нет».</summary>
    public static IReadOnlyList<HeartbeatLease> AllAlive(IEnumerable<LeaseRef> leases) =>
        [.. leases.Select(l => new HeartbeatLease(l.JobId, l.LeaseId, false))];

    public IEnumerable<StoreCall> CallsOf(string op) => Calls.Where(c => c.Op == op);

    public Task<IReadOnlyList<LeasedJob>> DequeueAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Dequeue", Detail: (queue, max, serverId, lease)));
        return OnDequeue(queue, max, ct);
    }

    public Task<LimitedDequeueResult> DequeueLimitedAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("DequeueLimited", Detail: (queue, max, serverId, lease)));
        return OnDequeueLimited(queue, max, ct);
    }

    public Task<IReadOnlyList<Guid>> AckAsync(Guid serverId, IReadOnlyList<LeaseRef> leases, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Ack", Detail: leases.ToArray()));
        return OnAck(leases, ct);
    }

    public Task<FailOutcome> RetryAsync(Guid jobId, Guid leaseId, Guid serverId, TimeSpan delay, JobEventKind kind,
        string? message, string? exception, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Retry", jobId, leaseId, (delay, kind, message)));
        return OnRetry(jobId, leaseId, ct);
    }

    public Task<FailOutcome> FailAsync(Guid jobId, Guid leaseId, Guid serverId, JobEventKind kind,
        string? message, string? exception, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Fail", jobId, leaseId, (kind, message)));
        return OnFail(jobId, leaseId, ct);
    }

    public Task<FailOutcome> ConfirmCancelAsync(Guid jobId, Guid leaseId, Guid serverId, string? message, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("ConfirmCancel", jobId, leaseId, message));
        return OnConfirmCancel(jobId, leaseId, ct);
    }

    public Task<ReleaseOutcome> ReleaseAsync(Guid jobId, Guid leaseId, Guid serverId, bool ran, string? message, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Release", jobId, leaseId, ran));
        return OnRelease(jobId, leaseId, ran, ct);
    }

    /// <summary>ServerPresent ответа heartbeat (строка servers есть).</summary>
    public bool ServerPresent { get; set; } = true;

    public async Task<HeartbeatResult> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Heartbeat", Detail: leases.ToArray()));
        return new HeartbeatResult(ServerPresent, await OnHeartbeat(leases, ct));
    }

    public Task RegisterServerAsync(ServerRegistration server, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("RegisterServer", Detail: server));
        return OnRegisterServer(server, ct);
    }

    public Task RemoveServerAsync(Guid serverId, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("RemoveServer", Detail: serverId));
        return OnRemoveServer(serverId, ct);
    }

    public Task<PromoteResult> PromoteDueAsync(int batchSize, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Promote", Detail: batchSize));
        return OnPromote(batchSize, ct);
    }

    public Task<int> SweepExpiredLeasesAsync(int batchSize, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Sweep", Detail: batchSize));
        return OnSweep(batchSize, ct);
    }

    public Task<IReadOnlyList<string>> PublishJobTypesAsync(IReadOnlyList<JobTypeRecord> types, string publishedBy, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("PublishJobTypes", Detail: (types, publishedBy)));
        return Task.FromResult<IReadOnlyList<string>>([]);
    }

    public Task<IReadOnlyList<string>> GetJobTypeNamesByQueuesAsync(IReadOnlyList<string> queues, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("JobTypeNames", Detail: queues.ToArray()));
        return OnJobTypeNames(queues, ct);
    }

    public Task<IReadOnlyList<QueueDefaultConflict>> SyncQueueDefaultsAsync(IReadOnlyList<QueueDefault> defaults, string publishedBy,
        CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("SyncQueueDefaults", Detail: (defaults, publishedBy)));
        return OnSyncQueueDefaults(defaults, publishedBy, ct);
    }

    public Task<IReadOnlyList<ActiveQueueSetting>> GetActiveQueueSettingsAsync(CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("GetActiveQueueSettings"));
        return OnGetActiveQueueSettings(ct);
    }

    public Task<DateTime?> TryClaimMaintenanceAsync(string name, TimeSpan interval, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Claim", Detail: new ClaimCall(name, interval)));
        return OnClaim(name, interval, ct);
    }

    public Task CompleteMaintenanceAsync(string name, DateTime claimedAt, string? error, TimeSpan? retryDelay, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Complete", Detail: new CompleteCall(name, claimedAt, error, retryDelay)));
        return OnComplete(name, claimedAt, error, retryDelay, ct);
    }

    public Func<int, int, TimeSpan, CancellationToken, Task<RecurringTickResult>> OnTick { get; set; } =
        (_, _, _, _) => Task.FromResult(RecurringTickResult.Empty);

    public Func<string, IReadOnlyList<RecurringDefinition>, CancellationToken, Task<RecurringSyncResult>> OnSync { get; set; } =
        (_, _, _) => Task.FromResult(new RecurringSyncResult([], [], [], []));

    public Task<RecurringTickResult> TickRecurringAsync(int batchSize, int maxFires, TimeSpan misfireThreshold, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Tick", Detail: (batchSize, maxFires, misfireThreshold)));
        return OnTick(batchSize, maxFires, misfireThreshold, ct);
    }

    public Task<RecurringSyncResult> SyncRecurringAsync(string owner, IReadOnlyList<RecurringDefinition> definitions, ILogger logger,
        CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Sync", Detail: owner));
        return OnSync(owner, definitions, ct);
    }

    public Func<int, CancellationToken, Task<CompletionCountResult>> OnCount { get; set; } =
        (_, _) => Task.FromResult(CompletionCountResult.Empty);

    public Func<int, int, TimeSpan, CancellationToken, Task<ResolveResult>> OnResolve { get; set; } =
        (_, _, _, _) => Task.FromResult(ResolveResult.Empty);

    public Func<int, int, int, TimeSpan, CancellationToken, Task<CancelMembersResult>> OnCancelMembers { get; set; } =
        (_, _, _, _, _) => Task.FromResult(CancelMembersResult.Empty);

    public Func<CancellationToken, Task<TimeSpan?>> OnOldest { get; set; } = _ => Task.FromResult<TimeSpan?>(null);

    /// <summary>Готовые сигналы resolver'а; по умолчанию нет (простой).</summary>
    public Func<CancellationToken, Task<bool>> OnPending { get; set; } = _ => Task.FromResult(false);

    public Task<bool> HasReadyCompletionsAsync(CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Pending"));
        return OnPending(ct);
    }

    public Task<CompletionCountResult> CountCompletionsAsync(int batchSize, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Count", Detail: batchSize));
        return OnCount(batchSize, ct);
    }

    public Task<ResolveResult> ResolveAsync(int batchSize, int rowBudget, TimeSpan retryDelay, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Resolve", Detail: (batchSize, rowBudget, retryDelay)));
        return OnResolve(batchSize, rowBudget, retryDelay, ct);
    }

    public Task<CancelMembersResult> CancelMembersAsync(int portion, int rowBudget, int maxTransactions, TimeSpan retryDelay,
        CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("CancelMembers", Detail: (portion, rowBudget, maxTransactions, retryDelay)));
        return OnCancelMembers(portion, rowBudget, maxTransactions, retryDelay, ct);
    }

    public Task<TimeSpan?> OldestCompletionAgeAsync(CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Oldest"));
        return OnOldest(ct);
    }

    public Task FlushOutcomeStatsAsync(IReadOnlyList<OutcomeStatsRow> rows, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("FlushStats", Detail: rows.Select(r => r.ToString()).ToArray()));
        return OnFlushStats(rows, ct);
    }

    public Task SnapshotQueueStatsAsync(CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Snapshot"));
        return OnSnapshot(ct);
    }

    public Task<QueueStatsSnapshot> ReadQueueStatsAsync(CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("ReadQueueStats"));
        return OnReadQueueStats(ct);
    }

    public Task<IReadOnlyList<TableStatsRow>> ReadTableStatsAsync(CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("ReadTableStats"));
        return OnReadTableStats(ct);
    }

    public Task<int> TrimStatsAsync(TimeSpan retention, int limit, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Trim", Detail: (retention, limit)));
        return OnTrim(retention, limit, ct);
    }
}
