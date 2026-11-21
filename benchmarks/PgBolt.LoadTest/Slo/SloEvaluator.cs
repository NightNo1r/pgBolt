using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Measure;
using static System.FormattableString;

namespace PgBolt.LoadTest.Slo;

/// <summary>
/// Evaluates SLOs from 10-second windows, per-second samples, and run histograms. Uses only windows fully inside [MeasureStart, MeasureEnd) and
/// samples in that interval. Warmup and drain appear in reports but do not affect verdicts or the CPU saturation flag.
/// </summary>
internal static class SloEvaluator
{
    public const double MaxImmediateP99Ms = 500;

    private const double MaxImmediateP50Ms = 50;
    private const double MaxImmediateP999Ms = 2_000;
    private const double MaxDelayedP99Ms = 1_500;
    private const long MaxBouncerWaitMicros = 100_000;
    // Сумма Maximum Pool Size двух хостов (по 20) и продюсера (8).
    private const int MaxDirectConnections = 48;
    private const double CpuBusyFraction = 0.8;
    private const string EnqueueMark = "enqueue";
    private const string StandMark = "harness-limited";

    private static readonly TimeSpan WindowLength = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan RecoveryLimit = TimeSpan.FromMinutes(5);

    /// <summary>Evaluates profile B SLOs 1–6 over the measurement window.</summary>
    public static IReadOnlyList<SloVerdict> Sustained(RunData data)
    {
        var windows = WindowsIn(data.Windows, data.MeasureStart, data.MeasureEnd);
        var samples = SamplesIn(data.Samples, data.MeasureStart, data.MeasureEnd);
        var (growth, growthValue) = BacklogGrowth(samples, data.MeasureStart, data.MeasureEnd, data.Rate);

        List<SloVerdict> verdicts =
        [
            Throughput(windows, data.Rate),
            StartLatency(data.Immediate),
            Lateness(data.Delayed),
            BacklogEachSample(samples, data.Rate),
            new(4, "Ready backlog growth over the measurement window", "last-minute mean ≤ first-minute mean + rate × 1 s",
                growthValue, growth, null),
            Correctness(data.Correctness),
            Connections(samples, data.Mode),
        ];
        return MarkStand(verdicts, IsCpuBound(windows));
    }

    /// <summary>Evaluates SLOs 1 and 2 for a <c>partitions</c> combination over the measurement window.</summary>
    public static IReadOnlyList<SloVerdict> Partitions(RunData data) =>
        MarkStand([Throughput(WindowsIn(data.Windows, data.MeasureStart, data.MeasureEnd), data.Rate),
            StartLatency(data.Immediate)], IsCpuBound(data));

    /// <summary>Evaluates SLOs 6–9 for <c>vacuum</c> across measurement phases 1–3.</summary>
    public static IReadOnlyList<SloVerdict> Vacuum(RunData data)
    {
        var phases = data.Phases ?? throw new ArgumentException("The vacuum run has no phase boundaries.", nameof(data));
        List<SloVerdict> verdicts =
        [
            Connections(SamplesIn(data.Samples, data.MeasureStart, data.MeasureEnd), data.Mode),
            QueueSize(data.Samples, phases),
            Stability(data, phases),
            .. Recovery(data, phases),
        ];
        return MarkStand(verdicts, IsCpuBound(data));
    }

    /// <summary>Detects an underfed window: the producer enqueued fewer than 98% of rate × 10 seconds, indicating enqueue cannot keep up.</summary>
    public static bool IsUnderloaded(WindowStats w, int rate) => w.Enqueued * 100 < 98L * rate * 10;

    /// <summary>At least 95% of rate × 10 seconds completed within the window.</summary>
    public static bool MeetsThroughput(WindowStats w, int rate) => w.Completed * 100 >= 95L * rate * 10;

    /// <summary>Harness CPU saturation means CPU usage exceeds 80% of available cores in more than 10% of windows.</summary>
    public static bool IsCpuBound(IReadOnlyList<WindowStats> windows) =>
        windows.Count(w => w.CpuFraction > CpuBusyFraction) * 10 > windows.Count;

    public static bool IsCpuBound(RunData data) =>
        IsCpuBound(WindowsIn(data.Windows, data.MeasureStart, data.MeasureEnd));

    private static SloVerdict Throughput(List<WindowStats> windows, int rate)
    {
        const string name = "Throughput", threshold = "each 10 s window: completed ≥ 95% of target rate";
        if (windows.Count == 0) return NoData(1, name, threshold);

        var underloaded = windows.Count(w => IsUnderloaded(w, rate));
        var slow = windows.Count(w => !MeetsThroughput(w, rate));
        var minShare = windows.Min(w => w.Completed * 100.0 / (rate * 10L));
        return new(1, name, threshold,
            Invariant($"minimum {minShare:F1}% of target rate, {slow} windows below threshold, {underloaded} underloaded"),
            Result(slow == 0 && underloaded == 0), underloaded > 0 ? EnqueueMark : null);
    }

    private static SloVerdict StartLatency(LatencySummary s)
    {
        const string name = "Immediate job start latency", threshold = "p50 ≤ 50 ms, p99 ≤ 500 ms, p99.9 ≤ 2 s";
        return s.Count == 0
            ? NoData(2, name, threshold)
            : new(2, name, threshold, Invariant($"p50 {s.P50Ms:F1} ms, p99 {s.P99Ms:F1} ms, p99.9 {s.P999Ms:F1} ms"),
                Result(StartLatencyHolds(s)), null);
    }

    private static SloVerdict Lateness(LatencySummary s)
    {
        const string name = "Delayed job lateness", threshold = "p99 ≤ 1.5 s";
        return s.Count == 0
            ? NoData(3, name, threshold)
            : new(3, name, threshold, Invariant($"p99 {s.P99Ms:F1} ms"), Result(s.P99Ms <= MaxDelayedP99Ms), null);
    }

    private static SloVerdict BacklogEachSample(List<Sample> samples, int rate)
    {
        const string name = "Ready backlog per sample", threshold = "≤ rate × 1 s";
        return samples.Max(s => s.ReadyBacklog) is not { } max
            ? NoData(4, name, threshold)
            : new(4, name, threshold, Invariant($"max {max}"), Result(max <= rate), null);
    }

    /// <summary>
    /// Second part of SLO 4: mean backlog over the last minute must not exceed the first-minute mean plus rate × 1 second. Returns no data for
    /// intervals shorter than two minutes because the windows overlap.
    /// </summary>
    private static (Verdict Result, string Value) BacklogGrowth(IReadOnlyList<Sample> samples, TimeSpan from, TimeSpan to, int rate)
    {
        if (to - from < 2 * Minute) return (Verdict.NoData, "interval shorter than 2 min");
        var first = Mean(samples, from, from + Minute, s => s.ReadyBacklog);
        var last = Mean(samples, to - Minute, to, s => s.ReadyBacklog);
        if (first is null || last is null) return (Verdict.NoData, "no samples");
        return (Result(last <= first + rate), Invariant($"first minute {first:F0}, last minute {last:F0}"));
    }

    private static SloVerdict Correctness(CorrectnessData? c)
    {
        const string name = "Correctness", threshold = "all jobs Succeeded, queue empty, execution counts and attempt match each Seq, no loss or errors";
        if (c is null) return NoData(5, name, threshold);

        var holds = c is
        {
            MissingExecutions: 0, DuplicatedExecutions: 0, JobsNotSucceeded: 0, QueueRowsAfterDrain: 0,
            DrainedInTime: true, AttemptMismatches: 0, OutcomeDropped: 0, StorageErrors: 0,
        };
        var value =
            $"missing {c.MissingExecutions}, extra executions {c.DuplicatedExecutions}, " +
            $"Seq values excluded due to failed calls {c.ExcludedFailedCalls}, jobs outside Succeeded {c.JobsNotSucceeded}, " +
            $"queue rows after drain {c.QueueRowsAfterDrain}{(c.DrainedInTime ? "" : " (drain exceeded 2 min)")}, " +
            $"incorrect attempt {c.AttemptMismatches}, outcome.dropped {c.OutcomeDropped}, storage.errors {c.StorageErrors}";
        return new(5, name, threshold, value, Result(holds), null);
    }

    private static SloVerdict Connections(List<Sample> samples, LoadMode mode)
    {
        const string name = "Connections";
        if (mode is LoadMode.Bouncer or LoadMode.BouncerPrepared)
        {
            const string bouncerThreshold = "maxwait ≤ 100 ms in every sample";
            if (samples.Max(s => s.MaxWaitMicros) is not { } maxWait) return NoData(6, name, bouncerThreshold);

            var value = Invariant($"max maxwait {maxWait / 1_000.0:F1} ms") + (samples.Max(s => s.ClWaiting) is not { } clWaiting
                ? ""
                : Invariant($", max cl_waiting {clWaiting}, mean {samples.Average(s => s.ClWaiting):F1}"));
            return new(6, name, bouncerThreshold, value, Result(maxWait <= MaxBouncerWaitMicros), null);
        }

        const string directThreshold = "host and producer connections ≤ 48";
        return samples.Max(s => s.Connections) is not { } connections
            ? NoData(6, name, directThreshold)
            : new(6, name, directThreshold, Invariant($"max {connections}"), Result(connections <= MaxDirectConnections), null);
    }

    private static SloVerdict QueueSize(IReadOnlyList<Sample> samples, PhaseMarks p)
    {
        const string name = "Phase 1 queue size", threshold = "last-minute mean ≤ 2 × mean over 60–120 s";
        // Последняя минута не должна перекрываться с отрезком 60–120 с.
        if (p.Phase2Start - p.Phase1Start < 3 * Minute) return NoData(7, name, threshold);

        var early = Mean(samples, p.Phase1Start + Minute, p.Phase1Start + 2 * Minute, s => s.QueueTotalBytes);
        var last = Mean(samples, p.Phase2Start - Minute, p.Phase2Start, s => s.QueueTotalBytes);
        if (early is null || last is null) return NoData(7, name, threshold);
        return new(7, name, threshold, Invariant($"60–120 s {early / 1_048_576:F1} MiB, last minute {last / 1_048_576:F1} MiB"),
            Result(last <= 2 * early), null);
    }

    /// <summary>SLO 8 requires SLOs 1–4 in each phase 1 window, using per-window percentiles for SLOs 2 and 3.</summary>
    private static SloVerdict Stability(RunData data, PhaseMarks p)
    {
        const string name = "Phase 1 stability", threshold = "SLOs 1–4 in every 10 s window of phase 1";
        var windows = WindowsIn(data.Windows, p.Phase1Start, p.Phase2Start);
        if (windows.Count == 0) return NoData(8, name, threshold);

        var rate = data.Rate;
        var samples = SamplesIn(data.Samples, p.Phase1Start, p.Phase2Start);
        var underloaded = windows.Count(w => IsUnderloaded(w, rate));
        var violated = windows.Count(w =>
            IsUnderloaded(w, rate) || !MeetsThroughput(w, rate)
            || w.Immediate.Count > 0 && !StartLatencyHolds(w.Immediate)
            || w.Delayed.Count > 0 && w.Delayed.P99Ms > MaxDelayedP99Ms);
        var backlogOver = samples.Count(s => s.ReadyBacklog > rate);
        var (growth, growthValue) = BacklogGrowth(samples, p.Phase1Start, p.Phase2Start, rate);

        var result = violated > 0 || backlogOver > 0 || growth == Verdict.Fail ? Verdict.Fail : growth;
        return new(8, name, threshold,
            Invariant($"windows violating SLOs 1–3: {violated} of {windows.Count}, samples with backlog above rate: {backlogOver}, backlog growth: {growthValue}"),
            result, underloaded > 0 ? EnqueueMark : null);
    }

    /// <summary>
    /// For SLO 9, recovery time T starts at the first phase 3 window after which all remaining windows satisfy SLO 1, per-window p99 for SLO 2, and
    /// the first part of SLO 4. Then evaluate the second part of SLO 4 on [T, end). Underfed windows before T delay recovery and add the “enqueue”
    /// mark.
    /// </summary>
    private static SloVerdict[] Recovery(RunData data, PhaseMarks p)
    {
        const string name = "Recovery", threshold = "T ≤ start of phase 3 + 5 min";
        const string growthName = "Recovery: backlog growth after T", growthThreshold = "last-minute mean ≤ mean of first minute after T + rate × 1 s";
        var windows = WindowsIn(data.Windows, p.Phase3Start, p.End);
        if (windows.Count == 0) return [NoData(9, name, threshold), NoData(9, growthName, growthThreshold)];

        var rate = data.Rate;
        var lastBad = windows.FindLastIndex(w =>
            IsUnderloaded(w, rate) || !MeetsThroughput(w, rate)
            || w.Immediate.Count > 0 && w.Immediate.P99Ms > MaxImmediateP99Ms
            || data.Samples.Any(s => s.At >= w.Start && s.At < w.Start + WindowLength && s.ReadyBacklog > rate));
        var mark = windows.Take(lastBad + 1).Any(w => IsUnderloaded(w, rate)) ? EnqueueMark : null;
        if (lastBad == windows.Count - 1)
            return [new(9, name, threshold, "no recovery by the end of phase 3", Verdict.Fail, mark), NoData(9, growthName, growthThreshold)];

        var t = windows[lastBad + 1].Start;
        var (growth, growthValue) = BacklogGrowth(data.Samples, t, p.End, rate);
        return
        [
            new(9, name, threshold, Invariant($"T = start of phase 3 + {(t - p.Phase3Start).TotalSeconds:F0} s"),
                Result(t <= p.Phase3Start + RecoveryLimit), mark),
            new(9, growthName, growthThreshold, growthValue, growth, null),
        ];
    }

    private static bool StartLatencyHolds(LatencySummary s) =>
        s.P50Ms <= MaxImmediateP50Ms && s.P99Ms <= MaxImmediateP99Ms && s.P999Ms <= MaxImmediateP999Ms;

    internal static List<WindowStats> WindowsIn(IReadOnlyList<WindowStats> windows, TimeSpan from, TimeSpan to) =>
        windows.Where(w => w.Start >= from && w.Start + WindowLength <= to).ToList();

    internal static List<Sample> SamplesIn(IReadOnlyList<Sample> samples, TimeSpan from, TimeSpan to) =>
        samples.Where(s => s.At >= from && s.At < to).ToList();

    private static double? Mean(IReadOnlyList<Sample> samples, TimeSpan from, TimeSpan to, Func<Sample, long?> value) =>
        SamplesIn(samples, from, to).Average(value);

    private static IReadOnlyList<SloVerdict> MarkStand(List<SloVerdict> verdicts, bool cpuBound) =>
        cpuBound
            ? verdicts.Select(v => v with { Mark = v.Mark is null ? StandMark : $"{v.Mark}, {StandMark}" }).ToList()
            : verdicts;

    // Один измеренный провал достаточен для отказа. Для успеха нужны все показатели.
    internal static Verdict VacuumStatus(IEnumerable<Verdict> verdicts)
    {
        var result = Verdict.Pass;
        foreach (var value in verdicts)
        {
            if (value == Verdict.Fail) { return Verdict.Fail; }
            if (value == Verdict.NoData) { result = Verdict.NoData; }
        }
        return result;
    }

    private static Verdict Result(bool holds) => holds ? Verdict.Pass : Verdict.Fail;

    private static SloVerdict NoData(int number, string name, string threshold) =>
        new(number, name, threshold, "no data", Verdict.NoData, null);
}
