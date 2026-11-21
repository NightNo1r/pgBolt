using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>Fetch strategy for a queue. <c>LimitReached</c> means the queue's cluster-wide limit is exhausted.</summary>
internal interface IFetchStrategy
{
    Task<LimitedDequeueResult> FetchAsync(string queue, int max, CancellationToken ct);
}

/// <summary>
/// Fetches through PgBoltStore.DequeueAsync. Queues in <see cref="QueueSettingsSnapshot.Limits"/> use
/// DequeueLimitedAsync, which serializes fetches with an advisory lock and enforces the cluster-wide limit. Each call
/// reads the snapshot so settings changes apply on the next fetch.
/// </summary>
internal sealed class DequeueFetchStrategy(IServerStore store, Guid serverId, TimeSpan lease, Func<QueueSettingsSnapshot> settings) : IFetchStrategy
{
    public async Task<LimitedDequeueResult> FetchAsync(string queue, int max, CancellationToken ct) =>
        settings().Limits.ContainsKey(queue)
            ? await store.DequeueLimitedAsync(queue, max, serverId, lease, ct)
            : new LimitedDequeueResult(await store.DequeueAsync(queue, max, serverId, lease, ct), LimitReached: false);
}
