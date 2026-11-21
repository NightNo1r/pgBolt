using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Slo;

/// <summary>
/// A <c>ramp</c> step with its evaluated windows, starting at the second third of the step, and immediate-job latency aggregated across those
/// windows.
/// </summary>
internal sealed record RampStep(int Rate, IReadOnlyList<WindowStats> Evaluated, LatencySummary Immediate);

/// <summary>
/// <see cref="MaxSustainedRate"/> is the last step's rate without a violation (<c>null</c> if the first step failed).
/// <see cref="FirstViolation"/> is “SLO 1”, “SLO 2”, “enqueue”, or “harness”. <see cref="LimitedByStand"/> indicates that harness CPU saturation
/// stopped the ramp.
/// </summary>
internal sealed record RampResult(int? MaxSustainedRate, string? FirstViolation, bool LimitedByStand);

/// <summary>Finds the maximum sustainable rate by evaluating steps in order until the first violation.</summary>
internal static class RampEvaluator
{
    private const string Stand = "harness";

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
