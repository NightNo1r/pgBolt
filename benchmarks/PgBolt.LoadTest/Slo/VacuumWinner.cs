namespace PgBolt.LoadTest.Slo;

/// <summary>Итог прогона <c>vacuum</c> в одном режиме: SLO 7–9, пиковый размер <c>queue</c> и p99 SLO 2 фазы 1.</summary>
internal sealed record ModeOutcome(bool Slo7To9, double PeakSizeBytes, double P99Ms);

internal sealed record VacuumCandidate(string Name, ModeOutcome Bouncer, ModeOutcome Direct);

internal sealed record VacuumChoice(VacuumCandidate Winner, string Reason);

/// <summary>
/// Выбор параметров autovacuum <c>queue</c> между действующим вариантом и претендентом. Решает режим
/// <c>bouncer</c> как худший; претендент принимается, только если и в <c>direct</c> выполняет SLO 7–9 и не хуже
/// действующего, когда тот их выполняет. Разница показателей меньше 10 % — равенство: однократные прогоны шумные.
/// </summary>
internal static class VacuumWinner
{
    private const double Tolerance = 0.1;

    /// <summary>
    /// Текущие reloptions против базового кандидата; при равенстве остаются текущие, если они выполняют SLO 7–9
    /// и в <c>direct</c>, иначе — кандидат, выполняющий их в обоих режимах.
    /// </summary>
    public static VacuumChoice Choose(VacuumCandidate current, VacuumCandidate baseline) =>
        Pick(current, baseline, tieKeepsIncumbent: current.Direct.Slo7To9);

    /// <summary>Табличный победитель против глобального кандидата; при равенстве остаётся табличный.</summary>
    public static VacuumChoice ChooseGlobal(VacuumCandidate tableWinner, VacuumCandidate global) =>
        Pick(tableWinner, global, tieKeepsIncumbent: true);

    private static VacuumChoice Pick(VacuumCandidate incumbent, VacuumCandidate challenger, bool tieKeepsIncumbent)
    {
        var (mine, theirs) = (incumbent.Bouncer, challenger.Bouncer);
        if (!mine.Slo7To9 && !theirs.Slo7To9) return new(incumbent, "SLO 7–9 в bouncer не выполняет ни один вариант");

        string reason;
        if (!theirs.Slo7To9) return new(incumbent, $"{challenger.Name} не выполняет SLO 7–9 в bouncer");
        if (!mine.Slo7To9) reason = $"{incumbent.Name} не выполняет SLO 7–9 в bouncer";
        else if (Better(mine, theirs)) return new(incumbent, $"{incumbent.Name} лучше в bouncer");
        else if (Better(theirs, mine)) reason = $"{challenger.Name} лучше в bouncer";
        else if (tieKeepsIncumbent || !challenger.Direct.Slo7To9) return new(incumbent, "равенство в bouncer");
        else reason = $"равенство в bouncer, {challenger.Name} выполняет SLO 7–9 в обоих режимах";

        if (!challenger.Direct.Slo7To9)
            return new(incumbent, $"расхождение режимов: {challenger.Name} не выполняет SLO 7–9 в direct");
        if (incumbent.Direct.Slo7To9 && !NotWorse(challenger.Direct, incumbent.Direct))
            return new(incumbent, $"расхождение режимов: {challenger.Name} хуже {incumbent.Name} в direct");
        return new(challenger, reason);
    }

    /// <summary>Размер меньше, а p99 не хуже.</summary>
    private static bool Better(ModeOutcome a, ModeOutcome b) =>
        Exceeds(b.PeakSizeBytes, a.PeakSizeBytes) && !Exceeds(a.P99Ms, b.P99Ms);

    private static bool NotWorse(ModeOutcome a, ModeOutcome b) =>
        !Exceeds(a.PeakSizeBytes, b.PeakSizeBytes) && !Exceeds(a.P99Ms, b.P99Ms);

    /// <summary><paramref name="a"/> больше <paramref name="b"/> на 10 % большего значения и более.</summary>
    private static bool Exceeds(double a, double b) => a > b && a - b >= Tolerance * a;
}
