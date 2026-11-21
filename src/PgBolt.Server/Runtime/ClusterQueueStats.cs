using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Latest <c>queue_stats</c> snapshot read for cluster gauges. Its timestamp is derived using <see cref="MonoClock"/> and
/// its age on the PostgreSQL clock, so rereading an unchanged table cannot refresh a stale snapshot. A snapshot older
/// than 4 × StatsSnapshotInterval is stale. The owner's update and each pod's read run every 0.8–1.2 intervals, giving a
/// normal age of up to about 2.4 intervals. One failed update leaves the series visible, while two consecutive failures
/// hide it. Thread-safe.
/// </summary>
internal sealed class ClusterQueueStats(MonoClock clock, PgBoltProcessingOptions options)
    : FreshRows<QueueStatsRow>(clock, options)
{
    /// <summary>A snapshot with unknown age has no known successful capture time and is treated as stale.</summary>
    public void Publish(QueueStatsSnapshot snapshot) =>
        Set(snapshot.Rows, snapshot.AgeSeconds is { } age ? Clock.Add(Clock.Now, -TimeSpan.FromSeconds(age)) : null);
}

/// <summary>
/// Latest rows read for cluster gauges. Rows older than 4 × StatsSnapshotInterval expire and their series disappear.
/// Thread-safe.
/// </summary>
internal abstract class FreshRows<T>(MonoClock clock, PgBoltProcessingOptions options)
{
    private const int StaleAfterIntervals = 4;

    private State? _state;

    protected MonoClock Clock => clock;

    /// <param name="rows">Rows from this read.</param>
    /// <param name="readAt">
    /// Timestamp of the rows on <see cref="MonoClock"/>. A <c>null</c> timestamp means their age is unknown and they are
    /// stale.
    /// </param>
    protected void Set(IReadOnlyList<T> rows, long? readAt) => Volatile.Write(ref _state, new State(rows, readAt));

    /// <summary>
    /// Gets the rows from a fresh read. Returns <c>false</c> if no read succeeded, its timestamp is unknown, or it is
    /// stale.
    /// </summary>
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
