using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Последний прочитанный снимок <c>queue_stats</c> для кластерных gauges. Момент снимка считается по <see cref="MonoClock"/>
/// от возраста на часах PG, поэтому повторное чтение таблицы, которую перестали обновлять, снимок не освежает.
/// Снимок старше 4 × StatsSnapshotInterval устарел: проход владельца и чтение пода — раз в 0,8–1,2 интервала, в норме возраст
/// до ~2,4 интервала, одна неудачная попытка снимка ряд не скрывает, две подряд — скрывают. Потокобезопасен.
/// </summary>
internal sealed class ClusterQueueStats(MonoClock clock, PgBoltProcessingOptions options)
    : FreshRows<QueueStatsRow>(clock, options)
{
    /// <summary>Снимок без возраста (момент последнего успешного снимка неизвестен) свежим не считается.</summary>
    public void Publish(QueueStatsSnapshot snapshot) =>
        Set(snapshot.Rows, snapshot.AgeSeconds is { } age ? Clock.Add(Clock.Now, -TimeSpan.FromSeconds(age)) : null);
}

/// <summary>
/// Последние прочитанные строки для кластерных gauges. Строки старше 4 × StatsSnapshotInterval устарели и скрываются:
/// ряд пропадает, а не застывает. Потокобезопасен.
/// </summary>
internal abstract class FreshRows<T>(MonoClock clock, PgBoltProcessingOptions options)
{
    private const int StaleAfterIntervals = 4;

    private State? _state;

    protected MonoClock Clock => clock;

    /// <param name="readAt">Момент строк по <see cref="MonoClock"/>; <c>null</c> — неизвестен, строки не свежие.</param>
    protected void Set(IReadOnlyList<T> rows, long? readAt) => Volatile.Write(ref _state, new State(rows, readAt));

    /// <summary>Строки свежего чтения; <c>false</c> — чтения не было, его момент неизвестен или оно устарело.</summary>
    public bool TryGetFresh(out IReadOnlyList<T> rows)
    {
        if (Volatile.Read(ref _state) is { ReadAt: { } at } state
            && clock.Elapsed(at) <= options.StatsSnapshotInterval * StaleAfterIntervals)
        {
            rows = state.Rows;
            return true;
        }
        rows = [];
        return false;
    }

    private sealed record State(IReadOnlyList<T> Rows, long? ReadAt);
}
