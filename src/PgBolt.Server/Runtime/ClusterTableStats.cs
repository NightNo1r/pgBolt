using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Последняя прочитанная статистика таблиц для кластерных gauges bloat. Значения читаются из PostgreSQL на каждом поде и
/// скрываются, если успешного чтения не было дольше 4 × StatsSnapshotInterval: ряд пропадает, а не застывает. Потокобезопасен.
/// </summary>
internal sealed class ClusterTableStats(MonoClock clock, PgBoltProcessingOptions options)
    : FreshRows<TableStatsRow>(clock, options)
{
    public void Update(IReadOnlyList<TableStatsRow> rows) => Set(rows, Clock.Now);
}
