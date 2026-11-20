using System.Collections.Concurrent;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Client;

/// <summary>Умолчания динамического enqueue из job_types с TTL (§6.2).</summary>
internal sealed class JobTypesCache(PgBoltStore store, TimeSpan ttl, TimeProvider time)
{
    private readonly ConcurrentDictionary<string, (JobTypeRecord? Record, DateTimeOffset ExpiresAt)> _entries = new(StringComparer.Ordinal);

    public async ValueTask<JobTypeRecord?> GetAsync(string name, CancellationToken ct)
    {
        var now = time.GetUtcNow();
        if (_entries.TryGetValue(name, out var entry) && now < entry.ExpiresAt)
            return entry.Record;
        var record = await store.GetJobTypeAsync(name, ct);
        _entries[name] = (record, now + ttl);
        return record;
    }

    /// <summary>Умолчания динамического типа (enqueue по имени); нет типа — UnknownJobException.</summary>
    public async ValueTask<JobDescriptor> GetDescriptorAsync(string name, CancellationToken ct)
    {
        var type = await GetAsync(name, ct) ?? throw new UnknownJobException(name);
        return new JobDescriptor(type.Name, type.DefaultQueue, type.DefaultMaxAttempts, type.DefaultPriority,
            type.DefaultRetryDelaysSeconds);
    }
}
