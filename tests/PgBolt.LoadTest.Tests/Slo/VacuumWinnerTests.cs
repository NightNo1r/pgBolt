using PgBolt.LoadTest.Slo;

namespace PgBolt.LoadTest.Tests.Slo;

public sealed class VacuumWinnerTests
{
    private static readonly ModeOutcome Met = new(Verdict.Pass, 1_000, 100);
    private static readonly ModeOutcome NotMet = new(Verdict.Fail, 1_000, 100);

    private static VacuumCandidate Current(ModeOutcome bouncer, ModeOutcome direct) => new("current", bouncer, direct);
    private static VacuumCandidate Baseline(ModeOutcome bouncer, ModeOutcome direct) => new("cost_delay=0", bouncer, direct);

    [Fact]
    public void IncompleteRunCannotWin()
    {
        var incomplete = Met with { Slo7To9 = Verdict.NoData };
        Assert.Throws<InvalidOperationException>(() => VacuumWinner.Choose(Current(Met, Met), Baseline(Met, incomplete)));
    }

    [Fact]
    public void Meeting_slo_wins_over_not_meeting()
    {
        var baseline = Baseline(Met with { PeakSizeBytes = 5_000 }, Met);

        Assert.Same(baseline, VacuumWinner.Choose(Current(NotMet, Met), baseline).Winner);

        var current = Current(Met with { PeakSizeBytes = 5_000 }, Met);
        Assert.Same(current, VacuumWinner.Choose(current, Baseline(NotMet with { PeakSizeBytes = 10 }, Met)).Winner);
    }

    [Fact]
    public void Smaller_size_with_not_worse_p99_wins()
    {
        var baseline = Baseline(Met with { PeakSizeBytes = 899, P99Ms = 109 }, Met);

        Assert.Same(baseline, VacuumWinner.Choose(Current(Met, Met), baseline).Winner);
    }

    [Theory]
    [InlineData(901, 100)] // размер меньше, но разница меньше 10 %
    [InlineData(500, 112)] // размер меньше, а p99 хуже больше чем на 10 %
    [InlineData(1_000, 50)] // размер тот же, p99 лучше — тоже равенство
    public void Tie_keeps_current_meeting_slo_in_direct(double size, double p99)
    {
        var current = Current(Met, Met);

        Assert.Same(current, VacuumWinner.Choose(current, Baseline(Met with { PeakSizeBytes = size, P99Ms = p99 }, Met)).Winner);
    }

    [Fact]
    public void Tie_prefers_variant_meeting_slo_in_both_modes()
    {
        var baseline = Baseline(Met with { PeakSizeBytes = 950 }, Met);

        Assert.Same(baseline, VacuumWinner.Choose(Current(Met, NotMet), baseline).Winner);
    }

    [Fact]
    public void Modes_divergence_keeps_current()
    {
        var current = Current(Met, Met);
        var failsDirect = Baseline(Met with { PeakSizeBytes = 500 }, NotMet);
        var worseInDirect = Baseline(Met with { PeakSizeBytes = 500 }, Met with { PeakSizeBytes = 1_200 });

        var choice = VacuumWinner.Choose(current, failsDirect);
        Assert.Same(current, choice.Winner);
        Assert.Contains("modes disagree", choice.Reason);
        Assert.Same(current, VacuumWinner.Choose(current, worseInDirect).Winner);
    }

    [Fact]
    public void Candidate_accepted_when_current_fails_direct()
    {
        // Текущие в direct не выполняют SLO 7–9 — сравнения с ними в direct нет.
        var baseline = Baseline(Met with { PeakSizeBytes = 500 }, Met with { PeakSizeBytes = 5_000 });

        Assert.Same(baseline, VacuumWinner.Choose(Current(Met, NotMet with { PeakSizeBytes = 10 }), baseline).Winner);
    }

    [Fact]
    public void Bouncer_slo_not_met_by_any_table_variant_keeps_current()
    {
        var current = Current(NotMet, Met);

        Assert.Same(current, VacuumWinner.Choose(current, Baseline(NotMet with { PeakSizeBytes = 10 }, Met)).Winner);
    }

    [Fact]
    public void Global_vs_table_winner_tie_keeps_table_winner()
    {
        var table = Current(Met, NotMet);
        var global = new VacuumCandidate("naptime=10s", Met with { PeakSizeBytes = 950 }, Met);

        Assert.Same(table, VacuumWinner.ChooseGlobal(table, global).Winner);
    }

    [Fact]
    public void Global_wins_when_table_winner_fails_slo()
    {
        var table = Current(NotMet, NotMet);
        var global = new VacuumCandidate("naptime=10s", Met, Met);

        Assert.Same(global, VacuumWinner.ChooseGlobal(table, global).Winner);
    }
}
