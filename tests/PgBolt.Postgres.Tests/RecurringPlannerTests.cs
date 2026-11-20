using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres.Tests;

public sealed class RecurringPlannerTests
{
    private static readonly DateTime Now = new(2026, 9, 30, 12, 0, 30, 500, DateTimeKind.Utc);
    private static readonly TimeSpan T = TimeSpan.FromMinutes(1);

    private static DateTime Utc(int y, int mo, int d, int h = 0, int mi = 0, int s = 0) => new(y, mo, d, h, mi, s, DateTimeKind.Utc);

    private static PlanInput Row(string cron, DateTime? next, MisfirePolicy misfire = MisfirePolicy.FireOnce,
        OverlapPolicy overlap = OverlapPolicy.Skip, TimeSpan? window = null) =>
        new(CronSchedule.Parse(cron, "UTC"), misfire, window ?? TimeSpan.FromDays(1), overlap, next);

    [Fact]
    public void Row_without_cursor_does_nothing()
    {
        var plan = RecurringPlanner.Plan(Row("* * * * *", null), Now, T);
        Assert.Empty(plan.Fires);
        Assert.Null(plan.NextFireAt);
        Assert.Null(plan.WakeAt);
        Assert.Null(plan.Missed);
    }

    [Fact]
    public void Future_cursor_is_kept()
    {
        var next = Utc(2026, 9, 30, 12, 5);
        var plan = RecurringPlanner.Plan(Row("* * * * *", next), Now, T);
        Assert.Empty(plan.Fires);
        Assert.Equal((next, next), (plan.NextFireAt, plan.WakeAt));
    }

    [Fact]
    public void Fire_once_fires_single_due_time()
    {
        var plan = RecurringPlanner.Plan(Row("* * * * *", Utc(2026, 9, 30, 12)), Now, T);
        Assert.Equal([Utc(2026, 9, 30, 12)], plan.Fires);
        Assert.Equal((Utc(2026, 9, 30, 12, 1), Utc(2026, 9, 30, 12, 1)), (plan.NextFireAt, plan.WakeAt));
        Assert.Null(plan.Missed);
    }

    [Fact]
    public void Fire_once_after_long_idle_fires_latest_due_time()
    {
        var plan = RecurringPlanner.Plan(Row("* * * * *", Utc(2026, 9, 27, 12)), Now, T);
        Assert.Equal([Utc(2026, 9, 30, 12)], plan.Fires);
        Assert.Equal(Utc(2026, 9, 30, 12, 1), plan.NextFireAt);
    }

    [Fact]
    public void Skip_fires_fresh_due_time()
    {
        var plan = RecurringPlanner.Plan(Row("* * * * *", Utc(2026, 9, 30, 11, 59), MisfirePolicy.Skip), Now, T);
        Assert.Equal([Utc(2026, 9, 30, 12)], plan.Fires);
        Assert.Null(plan.Missed);
    }

    [Fact]
    public void Skip_drops_stale_due_time_and_reports_it()
    {
        var plan = RecurringPlanner.Plan(Row("0 * * * *", Utc(2026, 9, 30, 10), MisfirePolicy.Skip), Now.AddMinutes(30), T);
        Assert.Empty(plan.Fires);
        Assert.Equal(new MissedRange(Utc(2026, 9, 30, 10), Utc(2026, 9, 30, 12)), plan.Missed);
        Assert.Equal(Utc(2026, 9, 30, 13), plan.NextFireAt);
    }

    [Fact]
    public void Catch_up_allow_fires_first_100_and_wakes_now()
    {
        var next = Utc(2026, 9, 30, 12).AddMinutes(-250);
        var plan = RecurringPlanner.Plan(Row("* * * * *", next, MisfirePolicy.CatchUp, OverlapPolicy.Allow), Now, T);
        Assert.Equal(100, plan.Fires.Count);
        Assert.Equal(next, plan.Fires[0]);
        Assert.Equal(next.AddMinutes(99), plan.Fires[^1]);
        Assert.Equal(next.AddMinutes(100), plan.NextFireAt);
        Assert.Equal(Now, plan.WakeAt);                                 // догон встаёт в общую очередь (§5.3)
    }

    [Fact]
    public void Catch_up_starts_at_window_and_reports_older_fires()
    {
        var next = Utc(2026, 9, 30, 9);
        var plan = RecurringPlanner.Plan(Row("* * * * *", next, MisfirePolicy.CatchUp, OverlapPolicy.Allow, TimeSpan.FromHours(1)), Now, T);
        var windowStart = Now - TimeSpan.FromHours(1);
        Assert.Equal(Utc(2026, 9, 30, 11, 1), plan.Fires[0]);
        Assert.Equal(new MissedRange(next, windowStart), plan.Missed);
    }

    [Fact]
    public void Catch_up_skip_fires_one_and_advances_by_one()
    {
        var next = Utc(2026, 9, 30, 11, 55);
        var plan = RecurringPlanner.Plan(Row("* * * * *", next, MisfirePolicy.CatchUp), Now, T);
        Assert.Equal([next], plan.Fires);
        Assert.Equal(next.AddMinutes(1), plan.NextFireAt);
        Assert.Equal(Now, plan.WakeAt);
    }

    [Fact]
    public void Catch_up_without_fires_in_window_moves_cursor_after_now()
    {
        var next = Utc(2025, 1, 1);
        foreach (var overlap in new[] { OverlapPolicy.Skip, OverlapPolicy.Allow })
        {
            var plan = RecurringPlanner.Plan(Row("0 0 1 1 *", next, MisfirePolicy.CatchUp, overlap), Now, T);
            Assert.Empty(plan.Fires);
            Assert.Equal(Utc(2027, 1, 1), plan.NextFireAt);
            Assert.Equal(next, plan.Missed!.Value.FromUtc);
        }
    }

    [Fact]
    public void Conflict_keeps_cursor_and_rechecks_in_5_seconds()
    {
        var row = Row("* * * * *", Utc(2026, 9, 30, 11, 55), MisfirePolicy.CatchUp);
        var plan = RecurringPlanner.OnConflict(row, Now);
        Assert.Empty(plan.Fires);
        Assert.Equal((row.NextFireAt, Now.AddSeconds(5)), (plan.NextFireAt, plan.WakeAt));
        Assert.Null(plan.Missed);
    }

    [Fact]
    public void Due_time_equal_to_now_is_included()
    {
        var now = Utc(2026, 9, 30, 12, 1);
        var plan = RecurringPlanner.Plan(Row("* * * * *", Utc(2026, 9, 30, 12)), now, T);
        Assert.Equal([now], plan.Fires);
        Assert.Equal(Utc(2026, 9, 30, 12, 2), plan.NextFireAt);
    }

    [Fact]
    public void Last_due_after_100_years_of_every_second()
    {
        var s = CronSchedule.Parse("* * * * * *", "UTC");
        Assert.Equal(Utc(2026, 9, 30, 12, 0, 30), RecurringPlanner.LastDue(s, Utc(1926, 9, 30), Now));
    }

    [Fact]
    public void Last_due_of_burst_cron_is_exact()                     // каждую секунду в январе; простой 10.01–01.03
    {
        var s = CronSchedule.Parse("* * * * 1 *", "UTC");
        Assert.Equal(Utc(2026, 1, 31, 23, 59, 59), RecurringPlanner.LastDue(s, Utc(2026, 1, 10), Utc(2026, 3, 1)));
        var plan = RecurringPlanner.Plan(new PlanInput(s, MisfirePolicy.FireOnce, TimeSpan.FromDays(1), OverlapPolicy.Skip, Utc(2026, 1, 10)),
            Utc(2026, 3, 1), T);
        Assert.Equal((Utc(2026, 1, 31, 23, 59, 59), Utc(2027, 1, 1)), (plan.Fires.Single(), plan.NextFireAt!.Value));
    }

    [Theory]                                                           // Europe/Moscow 2011-03-27: MSK+3 → MSK+4 в 22:00Z → 23:00Z
    [InlineData(22, 30, 22, 0)]
    [InlineData(23, 0, 23, 0)]
    [InlineData(23, 30, 23, 0)]
    [InlineData(0, 30, 0, 0)]
    public void Last_due_across_standard_offset_change(int nowHour, int nowMinute, int expectedHour, int expectedMinute)
    {
        var s = CronSchedule.Parse("0 * * * *", "Europe/Moscow");
        var now = Utc(2011, 3, 26, nowHour, nowMinute);
        if (nowHour == 0)
            now = now.AddDays(1);
        var expected = Utc(2011, 3, 26, expectedHour, expectedMinute);
        if (expectedHour == 0)
            expected = expected.AddDays(1);

        Assert.Equal(expected, RecurringPlanner.LastDue(s, Utc(2011, 3, 26, 20), now));
        var plan = RecurringPlanner.Plan(new PlanInput(s, MisfirePolicy.CatchUp, TimeSpan.FromDays(1), OverlapPolicy.Allow, Utc(2011, 3, 26, 20)),
            now, T);
        Assert.Equal(plan.Fires.Distinct().Count(), plan.Fires.Count);  // догон не повторяет одно срабатывание
        Assert.Equal(expected, plan.Fires[^1]);
        Assert.True(plan.NextFireAt > now);
    }

    // §5.3, §10: FireOnce после простоя больше суток для разреженных cron — одно срабатывание, последнее из due; курсор = after(db_now).
    // Now = среда 2026-09-30 12:00:30.5 UTC.
    [Theory]
    [InlineData("0 * * * *", "2026-09-27T12:00:00Z", "2026-09-30T12:00:00Z", "2026-09-30T13:00:00Z")]   // почасовой, 3 суток
    [InlineData("0 9 * * 1", "2026-08-03T09:00:00Z", "2026-09-28T09:00:00Z", "2026-10-05T09:00:00Z")]   // еженедельный (пн), 8 недель
    [InlineData("0 0 1 1 *", "2020-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "2027-01-01T00:00:00Z")]   // раз в год, 6 лет
    public void Fire_once_after_long_idle_fires_latest_due_of_sparse_cron(string cron, string next, string expectedFire, string expectedNext)
    {
        var plan = RecurringPlanner.Plan(Row(cron, At(next)), Now, T);

        Assert.Equal([At(expectedFire)], plan.Fires);
        Assert.Equal((At(expectedNext), At(expectedNext)), (plan.NextFireAt, plan.WakeAt));
        Assert.Null(plan.Missed);
    }

    private static DateTime At(string iso) =>
        DateTime.Parse(iso, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);

    // §5.3: Skip ставит последнее из due, если оно ≥ db_now − T (граница включена); на тик позже db_now — отбрасывает.
    [Fact]
    public void Skip_threshold_boundary_is_inclusive()
    {
        var row = Row("0 * * * *", Utc(2026, 9, 30, 10), MisfirePolicy.Skip);
        var last = Utc(2026, 9, 30, 12);
        var boundary = last + T;                                                   // last == db_now − T

        var atBoundary = RecurringPlanner.Plan(row, boundary, T);
        Assert.Equal([last], atBoundary.Fires);
        Assert.Null(atBoundary.Missed);
        Assert.Equal(Utc(2026, 9, 30, 13), atBoundary.NextFireAt);

        var past = RecurringPlanner.Plan(row, boundary.AddTicks(1), T);           // last на тик раньше db_now − T
        Assert.Empty(past.Fires);
        Assert.Equal(new MissedRange(Utc(2026, 9, 30, 10), last), past.Missed);
        Assert.Equal(Utc(2026, 9, 30, 13), past.NextFireAt);
    }

    // §5.3: CatchUp ставит срабатывания не раньше window_start = db_now − window (граница включена); раньше — пропуск.
    [Theory]
    [InlineData(OverlapPolicy.Allow)]
    [InlineData(OverlapPolicy.Skip)]
    public void Catch_up_window_boundary_is_inclusive(OverlapPolicy overlap)
    {
        var window = TimeSpan.FromHours(1);
        var dbNow = Utc(2026, 9, 30, 12, 30);
        var windowStart = dbNow - window;                                          // 11:30 — срабатывание "30 * * * *"

        var atStart = RecurringPlanner.Plan(Row("30 * * * *", windowStart, MisfirePolicy.CatchUp, overlap, window), dbNow, T);
        Assert.Equal(windowStart, atStart.Fires[0]);                               // ставится
        Assert.Null(atStart.Missed);                                               // и не считается пропущенным
        DateTime[] expected = overlap == OverlapPolicy.Allow ? [windowStart, dbNow] : [windowStart];
        Assert.Equal(expected, atStart.Fires);

        var older = RecurringPlanner.Plan(Row("30 * * * *", windowStart, MisfirePolicy.CatchUp, overlap, window), dbNow.AddTicks(1), T);
        Assert.Equal([dbNow], older.Fires);                                        // 11:30 на тик раньше окна — пропуск
        Assert.Equal(new MissedRange(windowStart, windowStart.AddTicks(1)), older.Missed);
        Assert.Equal(Utc(2026, 9, 30, 13, 30), older.NextFireAt);
    }

    /// <summary>LastDue с подсчётом вызовов after(t) (шов §10).</summary>
    private static (DateTime Last, int Calls) CountedLastDue(string cron, DateTime next, DateTime dbNow)
    {
        var s = CronSchedule.Parse(cron, "UTC");
        var calls = 0;
        var last = RecurringPlanner.LastDue(t =>
        {
            calls++;
            return s.After(t);
        }, next, dbNow);
        return (last, calls);
    }

    // §5.3, §10: ⌈log₂((db_now − next_fire_at) / 1 с)⌉ + 2 вызова, не больше ~42 при любом простое и cron.
    [Theory]
    [InlineData("* * * * * *", "2026-09-30T12:00:30Z")]
    [InlineData("0 * * * *", "2026-09-30T12:00:00Z")]
    [InlineData("0 0 1 1 *", "2026-01-01T00:00:00Z")]
    public void Last_due_after_100_years_needs_at_most_42_after_calls(string cron, string expected)
    {
        var next = Utc(1926, 9, 30);
        var (last, calls) = CountedLastDue(cron, next, Now);

        Assert.Equal(At(expected), last);
        var bound = (int)Math.Ceiling(Math.Log2((Now - next).TotalSeconds)) + 2;
        Assert.InRange(calls, 3, Math.Min(bound, 42));
    }

    [Fact]
    public void Last_due_in_regular_case_needs_one_after_call()                // курсор — единственное due: 12:00, после него 12:01 > db_now
    {
        Assert.Equal((Utc(2026, 9, 30, 12), 1), CountedLastDue("* * * * *", Utc(2026, 9, 30, 12), Now));
    }
}
