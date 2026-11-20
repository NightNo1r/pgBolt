namespace PgBolt.Postgres.Client;

/// <summary>
/// Снимок списка настроек очередей с TTL на <see cref="TimeProvider"/>; без <c>duration</c> каждое чтение идёт в загрузку.
/// Параллельные промахи ждут одну загрузку; она идёт без токена вызывающего, чтобы отмена одного не обрывала её остальным.
/// Ошибка не кэшируется. Инвалидация увеличивает поколение: загрузка, начатая до неё, свой результат в кэш не кладёт —
/// иначе снимок, прочитанный до записи, жил бы до конца TTL, — а следующий вызывающий начинает новую загрузку.
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
