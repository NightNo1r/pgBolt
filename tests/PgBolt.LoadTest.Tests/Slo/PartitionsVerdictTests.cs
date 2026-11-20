using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Slo;

namespace PgBolt.LoadTest.Tests.Slo;

public sealed class PartitionsVerdictTests
{
    private static CombinationResult R(int p, IndexSet indexes = IndexSet.Current, LoadMode mode = LoadMode.Direct,
        bool slo1 = true, bool slo2 = true, bool cpuBound = false) =>
        new(p, indexes, mode, slo1 ? Verdict.Pass : Verdict.Fail, slo2 ? Verdict.Pass : Verdict.Fail, cpuBound);

    [Fact]
    public void Exit_code_1_only_from_p15_current()
    {
        List<CombinationResult> others =
        [
            R(15), R(15, mode: LoadMode.Bouncer),
            R(60, slo1: false), R(15, IndexSet.Extra, slo2: false), R(400, IndexSet.Extra, LoadMode.BouncerPrepared, false, false),
        ];

        Assert.Equal(0, PartitionsVerdict.ExitCode(others));
        Assert.Equal(1, PartitionsVerdict.ExitCode([.. others, R(15, mode: LoadMode.DirectPrepared, slo2: false)]));
        Assert.Equal(1, PartitionsVerdict.ExitCode([.. others, R(15, mode: LoadMode.BouncerPrepared, slo1: false)]));
    }

    [Fact]
    public void Exit_code_2_only_on_cpu_bound_p15_current()
    {
        List<CombinationResult> others = [R(15), R(60, cpuBound: true), R(15, IndexSet.Extra, cpuBound: true)];

        Assert.Equal(0, PartitionsVerdict.ExitCode(others));
        Assert.Equal(2, PartitionsVerdict.ExitCode([.. others, R(15, mode: LoadMode.Bouncer, slo1: false, cpuBound: true)]));
    }

    [Fact]
    public void Threshold_is_smallest_p_with_violation()
    {
        List<CombinationResult> results =
        [
            R(15), R(60), R(400, slo1: false), R(200, slo2: false),
            R(60, mode: LoadMode.Bouncer, slo1: false), R(15, IndexSet.Extra, slo1: false),
        ];

        Assert.Equal(200, PartitionsVerdict.Threshold(results, LoadMode.Direct, IndexSet.Current));
        Assert.Equal(60, PartitionsVerdict.Threshold(results, LoadMode.Bouncer, IndexSet.Current));
        Assert.Equal(15, PartitionsVerdict.Threshold(results, LoadMode.Direct, IndexSet.Extra));
        Assert.Null(PartitionsVerdict.Threshold(results, LoadMode.DirectPrepared, IndexSet.Current));
    }

    [Fact]
    public void Missing_p15_gives_0() =>
        Assert.Equal(0, PartitionsVerdict.ExitCode([R(60, slo1: false, cpuBound: true), R(200, slo2: false)]));

    [Fact]
    public void No_data_is_not_a_violation()
    {
        List<CombinationResult> results =
            [R(15) with { Slo1 = Verdict.NoData }, R(60) with { Slo2 = Verdict.NoData }];

        Assert.Equal(0, PartitionsVerdict.ExitCode(results));
        Assert.Null(PartitionsVerdict.Threshold(results, LoadMode.Direct, IndexSet.Current));
    }

    [Fact]
    public void Failed_combination_is_stand_failure()
    {
        List<CombinationResult> results = [R(15), R(400, IndexSet.Extra) with { Failure = "TimeoutException: x" }];

        Assert.Equal(2, PartitionsVerdict.ExitCode(results));
    }
}
