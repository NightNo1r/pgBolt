using Microsoft.Extensions.DependencyInjection;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Client;

/// <summary>
/// Manages queue limit and weight overrides. Validates before accessing the database. After any write attempt, clears this process's read cache
/// and refreshes local servers. A write that reported failure might still have committed.
/// </summary>
internal sealed class QueueSettingsManager(PgBoltStore store, QueueSettingsReadCache cache, IServiceProvider services) : IQueueSettingsManager
{
    internal const int MaxWeight = 1000;

    public ValueTask<IReadOnlyList<QueueSettings>> ListAsync(CancellationToken ct = default) =>
        cache.GetAsync(store.ListQueueSettingsAsync, ct);

    // Без кэша — одна строка, не вся таблица.
    public async ValueTask<QueueSettings?> GetAsync(string queue, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        if (!cache.Enabled)
            return await store.GetQueueSettingsAsync(queue, ct);
        return (await ListAsync(ct)).FirstOrDefault(s => s.Queue == queue);
    }

    public async ValueTask SetConcurrencyOverrideAsync(string queue, int? maxConcurrency, string? updatedBy = null, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        if (maxConcurrency < 0)
            throw new ArgumentOutOfRangeException(nameof(maxConcurrency), maxConcurrency, "maxConcurrency must be null (no limit), 0 (pause) or >= 1.");
        await WriteAsync(() => store.SetConcurrencyOverrideAsync(queue, maxConcurrency, updatedBy, ct));
    }

    public async ValueTask ResetConcurrencyOverrideAsync(string queue, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        await WriteAsync(() => store.ResetConcurrencyOverrideAsync(queue, ct));
    }

    public async ValueTask SetWeightOverrideAsync(string queue, int? weight, string? updatedBy = null, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        if (weight is < 1 or > MaxWeight)
            throw new ArgumentOutOfRangeException(nameof(weight), weight, $"weight must be null (no weight) or between 1 and {MaxWeight}.");
        await WriteAsync(() => store.SetWeightOverrideAsync(queue, weight, updatedBy, ct));
    }

    public async ValueTask ResetWeightOverrideAsync(string queue, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        await WriteAsync(() => store.ResetWeightOverrideAsync(queue, ct));
    }

    public void InvalidateCache() => cache.Invalidate();

    // Рефрешер берётся при вызове: AddPgBoltServer может быть вызван после AddPgBolt.
    private async Task WriteAsync(Func<Task> write)
    {
        try
        {
            await write();
        }
        finally
        {
            cache.Invalidate();
            services.GetService<IQueueSettingsRefresher>()?.RefreshNow();
        }
    }
}
