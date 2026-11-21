using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Latest table statistics for cluster bloat gauges. Each pod reads the values from PostgreSQL. If no read succeeds for
/// more than 4 × StatsSnapshotInterval, the series disappear. Thread-safe.
/// </summary>
internal sealed class ClusterTableStats(MonoClock clock, PgBoltProcessingOptions options)
    : FreshRows<TableStatsRow>(clock, options)
{
    public void Update(IReadOnlyList<TableStatsRow> rows) => Set(rows, Clock.Now);
}
