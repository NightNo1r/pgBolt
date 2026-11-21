namespace PgBolt.LoadTest.Slo;

/// <summary>One mode's <c>vacuum</c> result: SLOs 7–9, peak <c>queue</c> size, and phase 1 SLO 2 p99.</summary>
internal sealed record ModeOutcome(Verdict Slo7To9, double PeakSizeBytes, double P99Ms)
{
    public bool Passed => Slo7To9 == Verdict.Pass;
}

internal sealed record VacuumCandidate(string Name, ModeOutcome Bouncer, ModeOutcome Direct);

internal sealed record VacuumChoice(VacuumCandidate Winner, string Reason);

/// <summary>
/// Compares incumbent and candidate <c>queue</c> autovacuum settings. The worst-case <c>bouncer</c> mode decides. A candidate must also meet
/// SLOs 7–9 in <c>direct</c> and perform at least as well as a passing incumbent there. Differences below 10% count as ties because single runs
/// are noisy.
/// </summary>
internal static class VacuumWinner
{
    private const double Tolerance = 0.1;

    /// <summary>
    /// Compares current reloptions to the base candidate. On a tie, keep the current settings if they also pass SLOs 7–9 in <c>direct</c>. Otherwise
    /// use a candidate that passes in both modes.
    /// </summary>
    public static VacuumChoice Choose(VacuumCandidate current, VacuumCandidate baseline) =>
        Pick(current, baseline, tieKeepsIncumbent: current.Direct.Passed);

    /// <summary>Compares the table-level winner to the global candidate, keeping the table-level winner on a tie.</summary>
    public static VacuumChoice ChooseGlobal(VacuumCandidate tableWinner, VacuumCandidate global) =>
        Pick(tableWinner, global, tieKeepsIncumbent: true);

    private static VacuumChoice Pick(VacuumCandidate incumbent, VacuumCandidate challenger, bool tieKeepsIncumbent)
    {
        if (new[] { incumbent.Direct, incumbent.Bouncer, challenger.Direct, challenger.Bouncer }
            .Any(result => result.Slo7To9 == Verdict.NoData))
        {
            throw new InvalidOperationException("Selecting autovacuum settings requires SLO 7–9 results for both modes.");
        }
        var (mine, theirs) = (incumbent.Bouncer, challenger.Bouncer);
        if (!mine.Passed && !theirs.Passed) return new(incumbent, "both variants fail SLOs 7–9 in bouncer mode");

        string reason;
        if (!theirs.Passed) return new(incumbent, $"{challenger.Name} fails SLOs 7–9 in bouncer mode");
        if (!mine.Passed) reason = $"{incumbent.Name} fails SLOs 7–9 in bouncer mode";
        else if (Better(mine, theirs)) return new(incumbent, $"{incumbent.Name} performs better in bouncer mode");
        else if (Better(theirs, mine)) reason = $"{challenger.Name} performs better in bouncer mode";
        else if (tieKeepsIncumbent || !challenger.Direct.Passed) return new(incumbent, "tie in bouncer mode");
        else reason = $"tie in bouncer mode, {challenger.Name} meets SLOs 7–9 in both modes";

        if (!challenger.Direct.Passed)
            return new(incumbent, $"modes disagree: {challenger.Name} fails SLOs 7–9 in direct mode");
        if (incumbent.Direct.Passed && !NotWorse(challenger.Direct, incumbent.Direct))
            return new(incumbent, $"modes disagree: {challenger.Name} performs worse than {incumbent.Name} in direct mode");
        return new(challenger, reason);
    }

    /// <summary>Size is smaller and p99 is no worse.</summary>
    private static bool Better(ModeOutcome a, ModeOutcome b) =>
        Exceeds(b.PeakSizeBytes, a.PeakSizeBytes) && !Exceeds(a.P99Ms, b.P99Ms);

    private static bool NotWorse(ModeOutcome a, ModeOutcome b) =>
        !Exceeds(a.PeakSizeBytes, b.PeakSizeBytes) && !Exceeds(a.P99Ms, b.P99Ms);

    /// <summary><paramref name="a"/> exceeds <paramref name="b"/> by at least 10% of the larger value.</summary>
    private static bool Exceeds(double a, double b) => a > b && a - b >= Tolerance * a;
}
