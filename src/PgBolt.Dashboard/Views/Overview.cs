using System.ComponentModel;

namespace PgBolt.Dashboard.Views;

/// <summary>Internal chart point at the start of a UTC minute or 15-minute bucket.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record ThroughputPoint(DateTime BucketUtc, long Succeeded, long Failed, long Retried);

/// <summary>Internal dashboard totals from the queue_stats snapshot.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record QueueTotals(long Scheduled, long Enqueued, long Processing, long Awaiting);

/// <summary>Internal dashboard representation of the stats row in maintenance_tasks.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record StatsTaskRow(DateTime NextRunAt, DateTime? LastRunAt, string? LastError);

/// <summary>Internal dashboard overview data.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record OverviewData(QueueTotals Totals, int LiveServers, StatsFreshness Freshness, IReadOnlyList<ThroughputPoint> LastHour);
