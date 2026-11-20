using Cronos;
using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres.Tests;

public sealed class CronScheduleTests
{
    private static DateTime Utc(int y, int mo, int d, int h = 0, int mi = 0, int s = 0, int ms = 0) =>
        new(y, mo, d, h, mi, s, ms, DateTimeKind.Utc);

    [Theory]
    [InlineData("0 3 * * *", CronFormat.Standard)]
    [InlineData("  */15 * * * *  ", CronFormat.Standard)]
    [InlineData("0 0 3 * * *", CronFormat.IncludeSeconds)]
    [InlineData("@daily", CronFormat.Standard)]
    [InlineData("@every_second", CronFormat.Standard)]
    public void Format_is_chosen_by_field_count_or_macro(string cron, CronFormat expected) =>
        Assert.Equal(expected, CronSchedule.FormatOf(cron));

    [Theory]
    [InlineData("* * * *")]
    [InlineData("0 0 0 * * * *")]
    public void Wrong_field_count_is_format_error(string cron) =>
        Assert.ThrowsAny<FormatException>(() => CronSchedule.Parse(cron, "UTC"));

    [Fact]
    public void Macros_parse_and_fire()
    {
        Assert.Equal(Utc(2026, 10, 1), CronSchedule.Parse("@daily", "UTC").After(Utc(2026, 9, 30, 12)));
        Assert.Equal(Utc(2026, 9, 30, 12, 0, 1), CronSchedule.Parse("@every_second", "UTC").After(Utc(2026, 9, 30, 12)));
    }

    [Fact]
    public void Unknown_time_zone_throws() =>
        Assert.Throws<TimeZoneNotFoundException>(() => CronSchedule.Parse("0 3 * * *", "Mars/Olympus"));

    [Fact]
    public void After_is_strict_and_between_includes_both_bounds()
    {
        var s = CronSchedule.Parse("* * * * *", "UTC");
        Assert.Equal(Utc(2026, 9, 30, 12, 1), s.After(Utc(2026, 9, 30, 12)));
        Assert.Equal([Utc(2026, 9, 30, 12), Utc(2026, 9, 30, 12, 1), Utc(2026, 9, 30, 12, 2)],
            s.Between(Utc(2026, 9, 30, 12), Utc(2026, 9, 30, 12, 2)).ToList());
    }

    [Fact]
    public void Time_zone_is_applied()
    {
        var s = CronSchedule.Parse("0 3 * * *", "Europe/Moscow");   // UTC+3
        Assert.Equal(Utc(2026, 10, 1, 0), s.After(Utc(2026, 9, 30, 12)));
    }

    [Fact]
    public void Dst_spring_forward_fires_at_transition()                // America/New_York, 2026-03-08 02:00 → 03:00
    {
        var s = CronSchedule.Parse("30 2 * * *", "America/New_York");
        Assert.Equal(Utc(2026, 3, 8, 7), s.After(Utc(2026, 3, 8, 5)));  // 03:00 EDT
    }

    [Fact]
    public void Dst_fall_back_fires_fixed_once_and_interval_twice()    // 2026-11-01 02:00 EDT → 01:00 EST
    {
        var from = Utc(2026, 11, 1, 4);
        var to = Utc(2026, 11, 1, 8);
        Assert.Single(CronSchedule.Parse("0 1 * * *", "America/New_York").Between(from, to));
        Assert.Equal(4, CronSchedule.Parse("*/30 1 * * *", "America/New_York").Between(from, to).Count());
    }

    /// <summary>
    /// Смена стандартного смещения (не DST): Europe/Moscow 2011-03-27 02:00 MSK+3 → 03:00 MSK+4 (22:00Z → 23:00Z по UTC
    /// 2011-03-26). Cronos 0.13 здесь отвечает after(t) ≤ t (для «0 * * * *» — 22:00Z при t из [22:00Z, 23:00Z)).
    /// </summary>
    [Theory]
    [InlineData("0 * * * *", 60)]
    [InlineData("* * * * *", 1)]
    public void After_strictly_increases_across_standard_offset_change(string cron, int stepMinutes)
    {
        var s = CronSchedule.Parse(cron, "Europe/Moscow");

        // Цепочка срабатываний: несуществующее 02:00 MSK срабатывает в момент перехода (23:00Z), дальше — MSK+4.
        var chain = new List<DateTime>();
        for (var t = s.After(Utc(2011, 3, 26, 23).AddMinutes(-2 * stepMinutes - 1)); chain.Count < 4; t = s.After(t!.Value))
            chain.Add(t!.Value);
        Assert.Equal(Enumerable.Range(-2, 4).Select(i => Utc(2011, 3, 26, 23).AddMinutes(i * stepMinutes)), chain);

        // after(t) > t и не убывает по t (на этом стоит бинарный поиск LastDue) — посекундно вокруг перехода.
        DateTime? previous = null;
        for (var t = Utc(2011, 3, 26, 21, 55); t <= Utc(2011, 3, 26, 23, 5); t = t.AddSeconds(1))
        {
            var after = s.After(t)!.Value;
            Assert.True(after > t, $"after({t:O}) = {after:O}");
            Assert.True(previous is null || after >= previous, $"after({t:O}) = {after:O} < {previous:O}");
            previous = after;
        }
        Assert.Equal(Utc(2011, 3, 26, 23), s.After(Utc(2011, 3, 26, 22, 59, 59, 999)));

        // Between — та же цепочка: без повторов одного срабатывания.
        Assert.Equal(chain, s.Between(chain[0], chain[^1]).ToList());
    }

    [Fact]
    public void After_of_every_second_cron_does_not_skip_transition_second()
    {
        var s = CronSchedule.Parse("* * * * * *", "Europe/Moscow");
        Assert.Equal(Utc(2011, 3, 26, 23), s.After(Utc(2011, 3, 26, 22, 59, 59)));
        Assert.Equal(Utc(2011, 3, 26, 23, 0, 1), s.After(Utc(2011, 3, 26, 23)));
    }

    /// <summary>
    /// §5.4: на смене базового смещения семантика DST не гарантируется. Europe/Moscow 2014-10-26 02:00 MSK+4 → 01:00 MSK+3
    /// (22:00Z по UTC 2014-10-25): час 01:00–02:00 повторяется, но срабатывает один раз; Europe/Moscow 2011-03-27:
    /// несуществующее 02:30 срабатывает в 22:30Z, за полчаса до перехода.
    /// </summary>
    [Fact]
    public void Standard_offset_change_does_not_follow_dst_semantics()
    {
        var quarter = CronSchedule.Parse("*/15 1 * * *", "Europe/Moscow");
        Assert.Equal(Enumerable.Range(0, 4).Select(i => Utc(2014, 10, 25, 21, i * 15)),
            quarter.Between(Utc(2014, 10, 25, 19), Utc(2014, 10, 26, 0)));
        Assert.Equal([Utc(2014, 10, 25, 21), Utc(2014, 10, 25, 23)],
            CronSchedule.Parse("0 * * * *", "Europe/Moscow").Between(Utc(2014, 10, 25, 20, 30), Utc(2014, 10, 25, 23)));

        Assert.Equal(Utc(2011, 3, 26, 22, 30), CronSchedule.Parse("30 2 * * *", "Europe/Moscow").After(Utc(2011, 3, 26, 20)));
    }

    [Fact]
    public void Iana_id_is_kept_and_windows_id_is_converted()
    {
        Assert.Equal("Europe/Moscow", CronSchedule.IanaId(TimeZoneInfo.FindSystemTimeZoneById("Europe/Moscow")));
        Assert.Equal("Europe/Moscow", CronSchedule.IanaId(TimeZoneInfo.FindSystemTimeZoneById("Russian Standard Time")));
    }
}
