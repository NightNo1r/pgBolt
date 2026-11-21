using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Slo;

/// <summary>
/// Hot-path statement for one combination, with measurement-window <c>pg_stat_statements</c> totals (<c>null</c> if unavailable) and pre-load
/// locks. <see cref="LowerBound"/> marks estimated generic-plan locks. <see cref="Plan"/> identifies the plan in <c>*-prepared</c> modes and is
/// <c>null</c> for an unprepared statement.
/// </summary>
internal sealed record StatementResult(StatementClassStats? Stats, LockCount? Locks, bool LowerBound, PlanKind? Plan);

/// <summary>
/// One <c>partitions</c> matrix result. Includes SLO 1 and 2 verdicts, harness CPU saturation, immediate-job p99 start latency, hot-path
/// statements by class, backends waiting on <c>LockManager</c> during measurement, <c>pgbolt.storage.errors</c> (including “out of shared
/// memory” when the lock table is exhausted), and any <see cref="Failure"/> that interrupted the combination. Optional fields are <c>null</c>
/// when unavailable.
/// </summary>
internal sealed record CombinationResult(int P, IndexSet Indexes, LoadMode Mode, Verdict Slo1, Verdict Slo2,
    bool CpuBound, double? ImmediateP99Ms = null, IReadOnlyDictionary<string, StatementResult>? Statements = null,
    int? LockManagerWaitsMax = null, double? LockManagerWaitsMean = null, long? StorageErrors = null,
    string? Failure = null, FetchSummary? Fetches = null)
{
    /// <summary>True if SLO 1 or 2 failed. Missing data is not a violation.</summary>
    public bool Violated => Slo1 == Verdict.Fail || Slo2 == Verdict.Fail;
}

/// <summary>
/// Verdict for <c>partitions</c>. Only the default configuration (P = 15, current indexes) determines exit code 1 and CPU saturation. Other
/// combinations establish P thresholds for the report. A harness failure in any combination returns 2.
/// </summary>
internal static class PartitionsVerdict
{
    private const int DefaultPartitions = 15;

    public static int ExitCode(IReadOnlyList<CombinationResult> results)
    {
        var defaults = results.Where(r => r.P == DefaultPartitions && r.Indexes == IndexSet.Current).ToList();
        return results.Any(r => r.Failure is not null) || defaults.Any(r => r.CpuBound) ? 2
            : defaults.Any(r => r.Violated) ? 1 : 0;
    }

    /// <summary>Smallest P at which a mode and index set violate SLO 1 or 2. Returns <c>null</c> if none do.</summary>
    public static int? Threshold(IReadOnlyList<CombinationResult> results, LoadMode mode, IndexSet indexes) =>
        results.Where(r => r.Mode == mode && r.Indexes == indexes && r.Violated)
            .Select(r => (int?)r.P).Min();
}
