using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Counts database-confirmed outcomes (part 6 §4.1). All virtual servers share one instance per process. Keys contain the
/// UTC minute from <see cref="TimeProvider"/> at confirmation, server_id and queue. <see cref="OutcomeStatsFlusher"/>
/// takes the dictionary with <see cref="Take"/> and writes it in one statement. On failure, <see cref="Restore"/> merges
/// its counts back with those accumulated since.
/// </summary>
internal sealed class OutcomeStats(TimeProvider time)
{
    private readonly Lock _lock = new();
    private Dictionary<(DateTime Bucket, Guid Server, string Queue), (int S, int F, int R)> _counts = [];

    public void Record(Guid serverId, string queue, int succeeded = 0, int failed = 0, int retried = 0)
    {
        var now = time.GetUtcNow().UtcDateTime;
        var bucket = new DateTime(now.Ticks - now.Ticks % TimeSpan.TicksPerMinute, DateTimeKind.Utc);
        lock (_lock)
            AddLocked((bucket, serverId, queue), succeeded, failed, retried);
    }

    /// <summary>Takes the accumulated rows in (bucket, server, queue) order for deterministic upsert lock acquisition.</summary>
    public IReadOnlyList<OutcomeStatsRow> Take()
    {
        Dictionary<(DateTime Bucket, Guid Server, string Queue), (int S, int F, int R)> taken;
        lock (_lock)
        {
            taken = _counts;
            _counts = [];
        }
        return
        [
            .. taken.Select(p => new OutcomeStatsRow(p.Key.Bucket, p.Key.Server, p.Key.Queue, p.Value.S, p.Value.F, p.Value.R))
                .OrderBy(r => r.BucketUtc).ThenBy(r => r.ServerId).ThenBy(r => r.Queue, StringComparer.Ordinal),
        ];
    }

    public void Restore(IReadOnlyList<OutcomeStatsRow> rows)
    {
        lock (_lock)
            foreach (var r in rows)
                AddLocked((r.BucketUtc, r.ServerId, r.Queue), r.Succeeded, r.Failed, r.Retried);
    }

    private void AddLocked((DateTime, Guid, string) key, int succeeded, int failed, int retried)
    {
        _counts.TryGetValue(key, out var v);
        _counts[key] = (v.S + succeeded, v.F + failed, v.R + retried);
    }
}
