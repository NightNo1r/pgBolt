using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Slo;
using static PgBolt.LoadTest.Tests.Slo.SloTestData;

namespace PgBolt.LoadTest.Tests.Slo;

public sealed class RampEvaluatorTests
{
    // Оцениваемые окна ступени — последние 40 с ступени 60 с.
    private static RampStep Step(int rate, Func<int, WindowStats, WindowStats>? change = null,
        LatencySummary? immediate = null) =>
        new(rate,
            Enumerable.Range(0, 4)
                .Select(i => Window(i * 10, completed: rate * 10L, enqueued: rate * 10L))
                .Select((w, i) => change?.Invoke(i, w) ?? w).ToList(),
            immediate ?? GoodImmediate);

    [Fact]
    public void All_steps_pass()
    {
        var result = RampEvaluator.Evaluate([Step(2_000), Step(4_000), Step(6_000)]);

        Assert.Equal(new RampResult(6_000, null, false), result);
    }

    [Fact]
    public void Stops_on_first_slo1_violation()
    {
        var result = RampEvaluator.Evaluate([
            Step(2_000), Step(4_000),
            Step(6_000, (i, w) => i == 2 ? w with { Completed = 56_999 } : w),
            Step(8_000, (_, w) => w with { Enqueued = 0 }),
        ]);

        Assert.Equal(new RampResult(4_000, "SLO 1", false), result);
    }

    [Fact]
    public void Stops_on_slo2_merged_p99()
    {
        var result = RampEvaluator.Evaluate([
            Step(2_000), Step(4_000, immediate: GoodImmediate with { P99Ms = 501 }), Step(6_000),
        ]);

        Assert.Equal(new RampResult(2_000, "SLO 2", false), result);
    }

    [Fact]
    public void Slo2_uses_merged_p99_not_per_window()
    {
        var slowWindow = GoodImmediate with { P99Ms = 600 };

        var result = RampEvaluator.Evaluate([
            Step(2_000, (i, w) => i == 1 ? w with { Immediate = slowWindow } : w, GoodImmediate with { P99Ms = 500 }),
        ]);

        Assert.Equal(new RampResult(2_000, null, false), result);
    }

    [Fact]
    public void Underloaded_window_stops_with_enqueue()
    {
        var result = RampEvaluator.Evaluate([Step(2_000), Step(4_000, (i, w) => i == 3 ? w with { Enqueued = 39_100 } : w)]);

        Assert.Equal(new RampResult(2_000, "enqueue", false), result);
    }

    [Fact]
    public void Cpu_bound_step_stops_with_stand()
    {
        // Одно окно из четырёх (25 %) — упор больше чем в 10 % окон ступени.
        var result = RampEvaluator.Evaluate([Step(2_000), Step(4_000, (i, w) => i == 0 ? w with { CpuFraction = 0.9 } : w)]);

        Assert.Equal(new RampResult(2_000, "стенд", true), result);
    }

    [Fact]
    public void Violation_on_first_step_has_no_sustained_rate()
    {
        var result = RampEvaluator.Evaluate([Step(2_000, immediate: GoodImmediate with { P99Ms = 900 })]);

        Assert.Equal(new RampResult(null, "SLO 2", false), result);
    }
}
