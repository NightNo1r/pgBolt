using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>Стратегия выборки задач очереди; <c>LimitReached</c> — кластерный лимит очереди исчерпан.</summary>
internal interface IFetchStrategy
{
    Task<LimitedDequeueResult> FetchAsync(string queue, int max, CancellationToken ct);
}

/// <summary>
/// Выборка поверх PgBoltStore.DequeueAsync; очереди из <see cref="QueueSettingsSnapshot.Limits"/> —
/// поверх DequeueLimitedAsync, который сериализует выдачу advisory lock'ом и учитывает кластерный лимит.
/// Снимок читается при каждом вызове, поэтому смена настроек действует со следующей выборки.
/// </summary>
internal sealed class DequeueFetchStrategy(IServerStore store, Guid serverId, TimeSpan lease, Func<QueueSettingsSnapshot> settings) : IFetchStrategy
{
    public async Task<LimitedDequeueResult> FetchAsync(string queue, int max, CancellationToken ct) =>
        settings().Limits.ContainsKey(queue)
            ? await store.DequeueLimitedAsync(queue, max, serverId, lease, ct)
            : new LimitedDequeueResult(await store.DequeueAsync(queue, max, serverId, lease, ct), LimitReached: false);
}
