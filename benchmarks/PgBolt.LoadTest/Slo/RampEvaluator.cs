using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Slo;

/// <summary>
/// Ступень <c>ramp</c>: оцениваемые окна (с начала второй трети ступени) и сводка задержки немедленных
/// за эти окна вместе.
/// </summary>
internal sealed record RampStep(int Rate, IReadOnlyList<WindowStats> Evaluated, LatencySummary Immediate);

/// <summary>
/// <see cref="MaxSustainedRate"/> — поток последней ступени без нарушения (<c>null</c> — нарушена первая);
/// <see cref="FirstViolation"/> — «SLO 1», «SLO 2», «enqueue» или «стенд»; <see cref="LimitedByStand"/> —
/// рост остановлен упором в CPU стенда.
/// </summary>
internal sealed record RampResult(int? MaxSustainedRate, string? FirstViolation, bool LimitedByStand);

/// <summary>Поиск максимального устойчивого потока: ступени по порядку до первой с нарушением.</summary>
internal static class RampEvaluator
{
    private const string Stand = "стенд";

    public static RampResult Evaluate(IReadOnlyList<RampStep> steps)
    {
        int? sustained = null;
        foreach (var step in steps)
        {
            var violation = Violation(step);
            if (violation is not null) return new(sustained, violation, violation == Stand);
            sustained = step.Rate;
        }

        return new(sustained, null, false);
    }

    // Упор в CPU проверяется первым: при нём остальные нарушения говорят о стенде, а не о PgBolt.
    private static string? Violation(RampStep step) =>
        SloEvaluator.IsCpuBound(step.Evaluated) ? Stand
        : step.Evaluated.Any(w => SloEvaluator.IsUnderloaded(w, step.Rate)) ? "enqueue"
        : step.Evaluated.Any(w => !SloEvaluator.MeetsThroughput(w, step.Rate)) ? "SLO 1"
        : step.Immediate.P99Ms > SloEvaluator.MaxImmediateP99Ms ? "SLO 2"
        : null;
}
