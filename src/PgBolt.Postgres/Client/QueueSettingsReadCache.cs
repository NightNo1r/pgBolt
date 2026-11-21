namespace PgBolt.Postgres.Client;

/// <summary>
/// Caches queue settings for a TTL measured by <see cref="TimeProvider"/>. Without <c>duration</c>, every read starts a load. Concurrent misses
/// share a load independent of caller cancellation. Failures are never cached. Invalidation advances the generation so an earlier load cannot
/// cache stale data for the full TTL. The next caller starts a new load.
/// </summary>
internal sealed class QueueSettingsReadCache(TimeSpan? duration, TimeProvider time)
{
    private readonly Lock _lock = new();
    private long _generation;
    private Task<IReadOnlyList<QueueSettings>>? _inFlight;
    private (IReadOnlyList<QueueSettings> Value, long LoadedAt)? _snapshot;

    public bool Enabled => duration is not null;

    public ValueTask<IReadOnlyList<QueueSettings>> GetAsync(Func<CancellationToken, Task<IReadOnlyList<QueueSettings>>> load, CancellationToken ct)
    {
        if (duration is not { } ttl)
            return new(load(ct));
        Task<IReadOnlyList<QueueSettings>> task;
        lock (_lock)
        {
            if (_snapshot is { } s && time.GetElapsedTime(s.LoadedAt) < ttl)
                return new(s.Value);
            // Завершённая загрузка уже положила снимок или упала — её не переиспользуем.
            if (_inFlight is not { IsCompleted: false })
                _inFlight = LoadAsync(load, _generation);
            task = _inFlight;
        }
        return new(task.WaitAsync(ct));
    }

    public void Invalidate()
    {
        lock (_lock)
        {
            _generation++;
            _inFlight = null;
            _snapshot = null;
        }
    }

    private async Task<IReadOnlyList<QueueSettings>> LoadAsync(Func<CancellationToken, Task<IReadOnlyList<QueueSettings>>> load, long generation)
    {
        var startedAt = time.GetTimestamp();
        var value = await load(CancellationToken.None);
        lock (_lock)
        {
            if (generation == _generation)
                _snapshot = (value, startedAt);
        }
        return value;
    }
}
