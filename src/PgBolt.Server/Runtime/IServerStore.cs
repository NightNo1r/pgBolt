using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>A row in servers (§7.2).</summary>
internal sealed record ServerRegistration(Guid Id, string Name, string Host, string[] Queues, int Capacity);

/// <summary>
/// Database access for the runtime. <see cref="PgBoltServerStore"/> is the production implementation; tests use a fake.
/// </summary>
internal interface IServerStore
{
    Task<IReadOnlyList<LeasedJob>> DequeueAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct);
    Task<LimitedDequeueResult> DequeueLimitedAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct);
    Task<IReadOnlyList<Guid>> AckAsync(Guid serverId, IReadOnlyList<LeaseRef> leases, CancellationToken ct);
    Task<FailOutcome> RetryAsync(Guid jobId, Guid leaseId, Guid serverId, TimeSpan delay, JobEventKind kind, string? message, string? exception, CancellationToken ct);
    Task<FailOutcome> FailAsync(Guid jobId, Guid leaseId, Guid serverId, JobEventKind kind, string? message, string? exception, CancellationToken ct);
    Task<FailOutcome> ConfirmCancelAsync(Guid jobId, Guid leaseId, Guid serverId, string? message, CancellationToken ct);
    Task<ReleaseOutcome> ReleaseAsync(Guid jobId, Guid leaseId, Guid serverId, bool ran, string? message, CancellationToken ct);
    Task<HeartbeatResult> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct);
    Task RegisterServerAsync(ServerRegistration server, CancellationToken ct);
    Task RemoveServerAsync(Guid serverId, CancellationToken ct);
    Task<PromoteResult> PromoteDueAsync(int batchSize, CancellationToken ct);
    Task<int> SweepExpiredLeasesAsync(int batchSize, CancellationToken ct);
    Task<IReadOnlyList<string>> PublishJobTypesAsync(IReadOnlyList<JobTypeRecord> types, string publishedBy, CancellationToken ct);
    Task<IReadOnlyList<string>> GetJobTypeNamesByQueuesAsync(IReadOnlyList<string> queues, CancellationToken ct);
    Task<IReadOnlyList<QueueDefaultConflict>> SyncQueueDefaultsAsync(IReadOnlyList<QueueDefault> defaults, string publishedBy, CancellationToken ct);
    Task<IReadOnlyList<ActiveQueueSetting>> GetActiveQueueSettingsAsync(CancellationToken ct);
    Task<DateTime?> TryClaimMaintenanceAsync(string name, TimeSpan interval, CancellationToken ct);
    Task CompleteMaintenanceAsync(string name, DateTime claimedAt, string? error, TimeSpan? retryDelay, CancellationToken ct);
    Task<RecurringTickResult> TickRecurringAsync(int batchSize, int maxFires, TimeSpan misfireThreshold, CancellationToken ct);
    Task<RecurringSyncResult> SyncRecurringAsync(string owner, IReadOnlyList<RecurringDefinition> definitions, ILogger logger, CancellationToken ct);
    Task<CompletionCountResult> CountCompletionsAsync(int batchSize, CancellationToken ct);
    Task<ResolveResult> ResolveAsync(int batchSize, int rowBudget, TimeSpan retryDelay, CancellationToken ct);
    Task<CancelMembersResult> CancelMembersAsync(int portion, int rowBudget, int maxTransactions, TimeSpan retryDelay, CancellationToken ct);
    Task<TimeSpan?> OldestCompletionAgeAsync(CancellationToken ct);
    Task<bool> HasReadyCompletionsAsync(CancellationToken ct);
    Task FlushOutcomeStatsAsync(IReadOnlyList<OutcomeStatsRow> rows, CancellationToken ct);
    Task SnapshotQueueStatsAsync(CancellationToken ct);
    Task<QueueStatsSnapshot> ReadQueueStatsAsync(CancellationToken ct);
    Task<IReadOnlyList<TableStatsRow>> ReadTableStatsAsync(CancellationToken ct);
    Task<int> TrimStatsAsync(TimeSpan retention, int limit, CancellationToken ct);
}
