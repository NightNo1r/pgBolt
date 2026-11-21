namespace PgBolt.Postgres.Recurring;

internal readonly record struct PlanInput(CronSchedule Schedule, MisfirePolicy Misfire, TimeSpan CatchUpWindow, OverlapPolicy Overlap,
    DateTime? NextFireAt);

/// <summary>
/// Occurrences discarded by the misfire policy (warning in §7). CatchUp uses [FromUtc, ToUtc), where ToUtc starts the window. Skip uses
/// [FromUtc, ToUtc], where ToUtc is the last discarded occurrence.
/// </summary>
internal readonly record struct MissedRange(DateTime FromUtc, DateTime ToUtc);

/// <summary>Plan for one schedule row, including occurrences to enqueue and the new next_fire_at and wake_at.</summary>
internal sealed record FirePlan(IReadOnlyList<DateTime> Fires, DateTime? NextFireAt, DateTime? WakeAt, MissedRange? Missed);

/// <summary>Pure scheduling rules (Part 4 §5.3). All calculations use db_now. Due occurrences fall in [next_fire_at, db_now].</summary>
internal static class RecurringPlanner
{
    public const int MaxCatchUpFires = 100;
    public static readonly TimeSpan OverlapRecheckDelay = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan RowErrorDelay = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan SearchResolution = TimeSpan.FromSeconds(1);

    public static FirePlan Plan(PlanInput row, DateTime dbNow, TimeSpan misfireThreshold)
    {
        if (row.NextFireAt is not { } next)
            return new FirePlan([], null, null, null);
        if (next > dbNow)
            return new FirePlan([], next, next, null);

        var s = row.Schedule;
        switch (row.Misfire)
        {
            case MisfirePolicy.FireOnce:
                return Moved([LastDue(s, next, dbNow)], s.After(dbNow), dbNow, null);

            case MisfirePolicy.Skip:
            {
                var last = LastDue(s, next, dbNow);
                return last >= dbNow - misfireThreshold
                    ? Moved([last], s.After(dbNow), dbNow, null)
                    : Moved([], s.After(dbNow), dbNow, new MissedRange(next, last));
            }

            case MisfirePolicy.CatchUp:
            {
                var windowStart = dbNow - row.CatchUpWindow;
                var missed = next < windowStart ? new MissedRange(next, windowStart) : (MissedRange?)null;
                var take = row.Overlap == OverlapPolicy.Allow ? MaxCatchUpFires : 1;
                var fires = s.Between(next < windowStart ? windowStart : next, dbNow).Take(take).ToList();
                return fires.Count == 0
                    ? Moved([], s.After(dbNow), dbNow, missed)
                    : Moved(fires, s.After(fires[^1]), dbNow, missed);
            }

            default:
                throw new InvalidOperationException($"Unknown misfire policy {row.Misfire}.");
        }
    }

    /// <summary>
    /// For a key conflict under CatchUp with overlap Skip, keeps the cursor in place and checks again in 5 seconds. No skip is logged (§5.3).
    /// </summary>
    public static FirePlan OnConflict(PlanInput row, DateTime dbNow) =>
        new([], row.NextFireAt, dbNow + OverlapRecheckDelay, null);

    /// <summary>
    /// Finds the last occurrence in [next, dbNow], where next ≤ dbNow is the cursor occurrence. Uses After because CronSchedule does not expose
    /// Cronos GetPreviousOccurrence and its DST behavior is unverified. The monotonic predicate after(t) ≤ dbNow holds until the last due
    /// occurrence, then becomes false. Occurrences fall on whole seconds. Once hi − lo ≤ 1 second, (lo, hi] contains at most one occurrence, and
    /// after(lo) is the answer. Uses at most about 42 calls for any cron expression.
    /// </summary>
    public static DateTime LastDue(CronSchedule schedule, DateTime next, DateTime dbNow) => LastDue(schedule.After, next, dbNow);

    /// <summary>
    /// Variant of <see cref="LastDue(CronSchedule, DateTime, DateTime)"/> accepting an after(t) delegate so tests can count calls (§10: at most
    /// about 42 after a 100-year outage, one in the normal case).
    /// </summary>
    internal static DateTime LastDue(Func<DateTime, DateTime?> after, DateTime next, DateTime dbNow)
    {
        bool Due(DateTime t) => after(t) is { } a && a <= dbNow;

        if (!Due(next))
            return next;
        var lo = next;
        var hi = dbNow;
        while (hi - lo > SearchResolution)
        {
            var mid = lo + TimeSpan.FromTicks((hi - lo).Ticks / 2);
            if (Due(mid))
                lo = mid;
            else
                hi = mid;
        }
        return after(lo)!.Value;
    }

    private static FirePlan Moved(IReadOnlyList<DateTime> fires, DateTime? next, DateTime dbNow, MissedRange? missed) =>
        new(fires, next, next is { } n ? (n > dbNow ? n : dbNow) : null, missed);
}
