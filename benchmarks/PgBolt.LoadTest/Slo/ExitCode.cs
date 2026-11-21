namespace PgBolt.LoadTest.Slo;

/// <summary>
/// Scenario exit code: 2 for harness failure or harness process CPU saturation, 1 for an SLO violation, otherwise 0. Missing data is not a
/// violation.
/// </summary>
internal static class ExitCode
{
    public static int For(IReadOnlyList<SloVerdict> verdicts, bool standFailure, bool cpuBound) =>
        standFailure || cpuBound ? 2 : verdicts.Any(v => v.Result == Verdict.Fail) ? 1 : 0;
}
