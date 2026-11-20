using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Tests.Measure;

public sealed class SamplerScheduleTests
{
    private static TimeSpan S(double seconds) => TimeSpan.FromSeconds(seconds);

    [Fact]
    public void Slow_sample_skips_ticks_without_accumulating()
    {
        var schedule = new SamplerSchedule(S(1));

        Assert.NotEqual(TickDecision.Skip, schedule.OnTick(S(1)));
        Assert.Equal(TickDecision.Skip, schedule.OnTick(S(2)));
        Assert.Equal(TickDecision.Skip, schedule.OnTick(S(3)));
        schedule.OnCompleted(anyQueryOk: true);

        // Пропущенные тики не догоняются: после завершения — один срез на следующем тике.
        Assert.NotEqual(TickDecision.Skip, schedule.OnTick(S(4)));
        schedule.OnCompleted(anyQueryOk: true);
        Assert.NotEqual(TickDecision.Skip, schedule.OnTick(S(5)));
        Assert.Equal(2, schedule.SkippedTicks);
    }

    [Fact]
    public void Failed_query_is_a_skip()
    {
        var schedule = new SamplerSchedule(S(1));

        schedule.OnTick(S(1));
        schedule.OnCompleted(anyQueryOk: false);

        Assert.Equal(1, schedule.FailedAttempts);
        Assert.Equal(0, schedule.SkippedTicks);
        Assert.False(schedule.Exhausted);
        Assert.NotEqual(TickDecision.Skip, schedule.OnTick(S(2)));
    }

    [Fact]
    public void Exhausted_on_30th_consecutive_failure_not_before()
    {
        var schedule = new SamplerSchedule(S(1));

        for (var i = 1; i < 30; i++)
        {
            schedule.OnTick(S(i));
            schedule.OnCompleted(anyQueryOk: false);
        }
        Assert.False(schedule.Exhausted);

        schedule.OnTick(S(30));
        schedule.OnCompleted(anyQueryOk: false);
        Assert.True(schedule.Exhausted);
        Assert.Equal(30, schedule.FailedAttempts);
    }

    [Fact]
    public void Success_resets_failure_streak()
    {
        var schedule = new SamplerSchedule(S(1), maxConsecutiveFailures: 3);

        foreach (var ok in new[] { false, false, true, false, false })
        {
            schedule.OnTick(S(1));
            schedule.OnCompleted(ok);
        }

        Assert.False(schedule.Exhausted);
        Assert.Equal(4, schedule.FailedAttempts);
    }

    [Fact]
    public void Counts_run_every_ten_seconds()
    {
        var schedule = new SamplerSchedule(S(1));
        var withCounts = new List<double>();

        for (var t = 1; t <= 25; t++)
        {
            if (schedule.OnTick(S(t)) == TickDecision.SampleWithCounts)
                withCounts.Add(t);
            schedule.OnCompleted(anyQueryOk: true);
        }

        Assert.Equal([1, 11, 21], withCounts);
    }
}
