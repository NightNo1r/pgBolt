namespace PgBolt.Postgres.Recurring;

internal readonly record struct PlanInput(CronSchedule Schedule, MisfirePolicy Misfire, TimeSpan CatchUpWindow, OverlapPolicy Overlap,
    DateTime? NextFireAt);

/// <summary>Срабатывания, отброшенные misfire-политикой (Warning §7): CatchUp — [FromUtc, ToUtc), ToUtc — начало окна; Skip — [FromUtc, ToUtc], ToUtc — последнее отброшенное.</summary>
internal readonly record struct MissedRange(DateTime FromUtc, DateTime ToUtc);

/// <summary>Результат планирования строки: что ставить и новые next_fire_at / wake_at.</summary>
internal sealed record FirePlan(IReadOnlyList<DateTime> Fires, DateTime? NextFireAt, DateTime? WakeAt, MissedRange? Missed);

/// <summary>
/// Правила срабатывания (часть 4 §5.3). Чистая функция: все расчёты от db_now; due = срабатывания в [next_fire_at, db_now].
/// </summary>
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

    /// <summary>CatchUp + Skip, конфликт по ключу: курсор стоит, перепроверка через 5 с; пропуск не логируется (§5.3).</summary>
    public static FirePlan OnConflict(PlanInput row, DateTime dbNow) =>
        new([], row.NextFireAt, dbNow + OverlapRecheckDelay, null);

    /// <summary>
    /// Последнее срабатывание в [next, dbNow] (next ≤ dbNow — срабатывание курсора). Поиск опирается только на After
    /// (GetPreviousOccurrence Cronos в CronSchedule не выставлен, его поведение на DST не проверено), поэтому бинарный поиск
    /// по монотонному предикату after(t) ≤ dbNow: истинен до последнего due, ложен начиная с него. Срабатывания — на
    /// целых секундах, поэтому при hi − lo ≤ 1 с в (lo, hi] не больше одного, и ответ — after(lo). ≤ ~42 вызова при любом cron.
    /// </summary>
    public static DateTime LastDue(CronSchedule schedule, DateTime next, DateTime dbNow) => LastDue(schedule.After, next, dbNow);

    /// <summary>
    /// <see cref="LastDue(CronSchedule, DateTime, DateTime)"/> с after(t), переданным делегатом: шов для тестов, считающих
    /// вызовы after(t) (§10: ≤ ~42 при простое 100 лет, 1 в штатном случае).
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
