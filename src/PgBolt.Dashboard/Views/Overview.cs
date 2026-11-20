using System.ComponentModel;

namespace PgBolt.Dashboard.Views;

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: точка графика исходов (начало минуты или 15-минутки UTC).</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record ThroughputPoint(DateTime BucketUtc, long Succeeded, long Failed, long Retried);

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: суммы снимка queue_stats.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record QueueTotals(long Scheduled, long Enqueued, long Processing, long Awaiting);

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: строка stats в maintenance_tasks.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record StatsTaskRow(DateTime NextRunAt, DateTime? LastRunAt, string? LastError);

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: данные обзора.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record OverviewData(QueueTotals Totals, int LiveServers, StatsFreshness Freshness, IReadOnlyList<ThroughputPoint> LastHour);
