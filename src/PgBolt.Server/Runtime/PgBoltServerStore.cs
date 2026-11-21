using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>Forwards <see cref="IServerStore"/> calls to <see cref="PgBoltStore"/>.</summary>
internal sealed class PgBoltServerStore(PgBoltStore store) : IServerStore
{
    private readonly PgBoltStore _store = store ?? throw new ArgumentNullException(nameof(store));

    public Task<IReadOnlyList<LeasedJob>> DequeueAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct) =>
        _store.DequeueAsync(queue, max, serverId, lease, ct);

    public Task<LimitedDequeueResult> DequeueLimitedAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct) =>
        _store.DequeueLimitedAsync(queue, max, serverId, lease, ct);

    public Task<IReadOnlyList<Guid>> AckAsync(Guid serverId, IReadOnlyList<LeaseRef> leases, CancellationToken ct) =>
        _store.AckAsync(serverId, leases, ct);

    public Task<FailOutcome> RetryAsync(Guid jobId, Guid leaseId, Guid serverId, TimeSpan delay, JobEventKind kind,
        string? message, string? exception, CancellationToken ct) =>
        _store.RetryAsync(jobId, leaseId, serverId, delay, kind, message, exception, ct);

    public Task<FailOutcome> FailAsync(Guid jobId, Guid leaseId, Guid serverId, JobEventKind kind,
        string? message, string? exception, CancellationToken ct) =>
        _store.FailAsync(jobId, leaseId, serverId, kind, message, exception, ct);

    public Task<FailOutcome> ConfirmCancelAsync(Guid jobId, Guid leaseId, Guid serverId, string? message, CancellationToken ct) =>
        _store.ConfirmCancelAsync(jobId, leaseId, serverId, message, ct);

    public Task<ReleaseOutcome> ReleaseAsync(Guid jobId, Guid leaseId, Guid serverId, bool ran, string? message, CancellationToken ct) =>
        _store.ReleaseAsync(jobId, leaseId, serverId, ran, message, ct);

    public Task<HeartbeatResult> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct) =>
        _store.HeartbeatAsync(serverId, leases, lease, ct);

    public Task RegisterServerAsync(ServerRegistration server, CancellationToken ct) =>
        _store.RegisterServerAsync(server.Id, server.Name, server.Host, server.Queues, server.Capacity, ct);

    public Task RemoveServerAsync(Guid serverId, CancellationToken ct) =>
        _store.RemoveServerAsync(serverId, ct);

    public Task<PromoteResult> PromoteDueAsync(int batchSize, CancellationToken ct) =>
        _store.PromoteDueAsync(batchSize, ct);

    public Task<int> SweepExpiredLeasesAsync(int batchSize, CancellationToken ct) =>
        _store.SweepExpiredLeasesAsync(batchSize, ct);

    public Task<IReadOnlyList<string>> PublishJobTypesAsync(IReadOnlyList<JobTypeRecord> types, string publishedBy, CancellationToken ct) =>
        _store.PublishJobTypesAsync(types, publishedBy, ct);

    public Task<IReadOnlyList<string>> GetJobTypeNamesByQueuesAsync(IReadOnlyList<string> queues, CancellationToken ct) =>
        _store.GetJobTypeNamesByQueuesAsync(queues, ct);

    public Task<IReadOnlyList<QueueDefaultConflict>> SyncQueueDefaultsAsync(IReadOnlyList<QueueDefault> defaults, string publishedBy,
        CancellationToken ct) =>
        _store.SyncQueueDefaultsAsync(defaults, publishedBy, ct);

    public Task<IReadOnlyList<ActiveQueueSetting>> GetActiveQueueSettingsAsync(CancellationToken ct) =>
        _store.GetActiveQueueSettingsAsync(ct);

    public Task<DateTime?> TryClaimMaintenanceAsync(string name, TimeSpan interval, CancellationToken ct) =>
        _store.TryClaimMaintenanceAsync(name, interval, ct);

    /// <summary>
    /// A false result is expected when another pod has reclaimed the row (§5.7). The overlapping run's result is
    /// discarded.
    /// </summary>
    public Task CompleteMaintenanceAsync(string name, DateTime claimedAt, string? error, TimeSpan? retryDelay, CancellationToken ct) =>
        _store.CompleteMaintenanceAsync(name, claimedAt, error, retryDelay, ct);

    public Task<RecurringTickResult> TickRecurringAsync(int batchSize, int maxFires, TimeSpan misfireThreshold, CancellationToken ct) =>
        _store.TickRecurringAsync(batchSize, maxFires, misfireThreshold, ct);

    public Task<RecurringSyncResult> SyncRecurringAsync(string owner, IReadOnlyList<RecurringDefinition> definitions, ILogger logger,
        CancellationToken ct) =>
        _store.SyncRecurringAsync(owner, definitions, logger, ct);

    public Task<CompletionCountResult> CountCompletionsAsync(int batchSize, CancellationToken ct) =>
        _store.CountCompletionsAsync(batchSize, ct);

    public Task<ResolveResult> ResolveAsync(int batchSize, int rowBudget, TimeSpan retryDelay, CancellationToken ct) =>
        _store.ResolveAsync(batchSize, rowBudget, retryDelay, ct);

    public Task<CancelMembersResult> CancelMembersAsync(int portion, int rowBudget, int maxTransactions, TimeSpan retryDelay,
        CancellationToken ct) =>
        _store.CancelMembersAsync(portion, rowBudget, maxTransactions, retryDelay, ct);

    public Task<TimeSpan?> OldestCompletionAgeAsync(CancellationToken ct) => _store.OldestCompletionAgeAsync(ct);

    public Task<bool> HasReadyCompletionsAsync(CancellationToken ct) => _store.HasReadyCompletionsAsync(ct);

    public Task FlushOutcomeStatsAsync(IReadOnlyList<OutcomeStatsRow> rows, CancellationToken ct) =>
        _store.FlushOutcomeStatsAsync(rows, ct);

    public Task SnapshotQueueStatsAsync(CancellationToken ct) => _store.SnapshotQueueStatsAsync(ct);

    public Task<QueueStatsSnapshot> ReadQueueStatsAsync(CancellationToken ct) => _store.ReadQueueStatsAsync(ct);

    public Task<IReadOnlyList<TableStatsRow>> ReadTableStatsAsync(CancellationToken ct) => _store.ReadTableStatsAsync(ct);

    public Task<int> TrimStatsAsync(TimeSpan retention, int limit, CancellationToken ct) =>
        _store.TrimStatsAsync(retention, limit, ct);
}
