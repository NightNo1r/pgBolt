namespace PgBolt.LoadTest.Slo;

/// <summary>
/// Код возврата сценария: 2 — сбой стенда или упор в CPU процесса стенда, 1 — нарушен хотя бы один SLO,
/// иначе 0. «Нет данных» нарушением не считается.
/// </summary>
internal static class ExitCode
{
    public static int For(IReadOnlyList<SloVerdict> verdicts, bool standFailure, bool cpuBound) =>
        standFailure || cpuBound ? 2 : verdicts.Any(v => v.Result == Verdict.Fail) ? 1 : 0;
}
