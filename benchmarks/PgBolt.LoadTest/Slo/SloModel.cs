using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Slo;

/// <summary>SLO verdict. <see cref="NoData"/> does not count as a violation.</summary>
internal enum Verdict { Pass, Fail, NoData }

/// <summary>SLO table row. <see cref="Mark"/> can be “enqueue”, “harness-limited”, both separated by a comma, or absent.</summary>
internal sealed record SloVerdict(int Number, string Name, string Threshold, string Value, Verdict Result, string? Mark);

/// <summary><c>vacuum</c> phase boundaries relative to run start.</summary>
internal sealed record PhaseMarks(TimeSpan Phase1Start, TimeSpan Phase2Start, TimeSpan Phase3Start, TimeSpan End);

/// <summary>
/// Post-drain correctness result including execution reconciliation, <c>jobs</c> rows outside Succeeded, remaining <c>queue</c> rows,
/// <c>jobs</c> rows with an incorrect <c>attempt</c>, and PgBolt counters.
/// </summary>
internal sealed record CorrectnessData(
    long MissingExecutions, long DuplicatedExecutions, long ExcludedFailedCalls, long JobsNotSucceeded,
    long QueueRowsAfterDrain, bool DrainedInTime, long AttemptMismatches, long OutcomeDropped, long StorageErrors);

/// <summary>
/// Measurements for one mode. <see cref="Windows"/> and <see cref="Samples"/> include warmup and drain. Evaluation uses only windows fully
/// inside [MeasureStart, MeasureEnd) and samples within that interval. <see cref="Immediate"/> and <see cref="Delayed"/> are measurement-window
/// histograms.
/// </summary>
internal sealed record RunData(
    int Rate, IReadOnlyList<WindowStats> Windows, IReadOnlyList<Sample> Samples,
    LatencySummary Immediate, LatencySummary Delayed, CorrectnessData? Correctness, LoadMode Mode,
    PhaseMarks? Phases, TimeSpan MeasureStart, TimeSpan MeasureEnd);
