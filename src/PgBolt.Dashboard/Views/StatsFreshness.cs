using System.ComponentModel;

namespace PgBolt.Dashboard.Views;

/// <summary>Internal dashboard freshness state of a statistics snapshot (§6.4).</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public enum StatsFreshnessKind { Fresh, NotRunning, Failing }

/// <summary>Internal dashboard snapshot freshness and the error from the latest pass.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record StatsFreshness(StatsFreshnessKind Kind, string? Error)
{
    /// <summary>
    /// A missing row or a next_run_at overdue by more than staleAfter + 0.5 × (next_run_at − last_run_at) indicates an
    /// unclaimed task (§6.4). Claiming sets a deadline of 0.8 intervals and normal lateness can reach 0.4 intervals.
    /// Otherwise last_error indicates Failing.
    /// </summary>
    public static StatsFreshness Evaluate(StatsTaskRow? row, DateTime dbNowUtc, TimeSpan staleAfter)
    {
        if (row is null || dbNowUtc - row.NextRunAt > staleAfter + (row.NextRunAt - (row.LastRunAt ?? row.NextRunAt)) * 0.5)
            return new StatsFreshness(StatsFreshnessKind.NotRunning, null);
        return row.LastError is { } error ? new StatsFreshness(StatsFreshnessKind.Failing, error) : new StatsFreshness(StatsFreshnessKind.Fresh, null);
    }
}
