using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Slo;
using static PgBolt.LoadTest.Tests.Slo.SloTestData;

namespace PgBolt.LoadTest.Tests.Slo;

public sealed class CpuBoundTests
{
    private static List<WindowStats> WindowsWithBusy(int busy) =>
        Enumerable.Range(0, 100).Select(i => Window(i * 10, cpu: i < busy ? 0.81 : 0.8)).ToList();

    [Fact]
    public void Ten_percent_of_busy_windows_is_not_cpu_bound() =>
        Assert.False(SloEvaluator.IsCpuBound(WindowsWithBusy(10)));

    [Fact]
    public void Eleven_percent_of_busy_windows_is_cpu_bound() =>
        Assert.True(SloEvaluator.IsCpuBound(WindowsWithBusy(11)));

    [Fact]
    public void No_windows_is_not_cpu_bound() => Assert.False(SloEvaluator.IsCpuBound([]));

    private static SloVerdict V(Verdict result) => new(1, "x", "x", "x", result, null);

    [Fact]
    public void Exit_code_cpu_bound_or_stand_failure_is_2()
    {
        Assert.Equal(2, ExitCode.For([V(Verdict.Pass)], standFailure: false, cpuBound: true));
        Assert.Equal(2, ExitCode.For([V(Verdict.Fail)], standFailure: true, cpuBound: false));
    }

    [Fact]
    public void Exit_code_fail_is_1() =>
        Assert.Equal(1, ExitCode.For([V(Verdict.Pass), V(Verdict.Fail), V(Verdict.NoData)], false, false));

    [Fact]
    public void Exit_code_pass_and_no_data_is_0() =>
        Assert.Equal(0, ExitCode.For([V(Verdict.Pass), V(Verdict.NoData)], false, false));
}
