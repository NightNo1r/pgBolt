using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Measure;
using static System.FormattableString;

namespace PgBolt.LoadTest.Slo;

/// <summary>
/// Вычисление SLO по окнам 10 с, посекундным срезам и гистограммам прогона. Оцениваются только окна, целиком
/// лежащие в [MeasureStart, MeasureEnd), и срезы из этого отрезка: прогрев и дренаж пишутся в отчёт, но на
/// вердикты и на признак упора в CPU не влияют.
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
    private const string StandMark = "ограничено стендом";

    private static readonly TimeSpan WindowLength = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan RecoveryLimit = TimeSpan.FromMinutes(5);

    /// <summary>SLO 1–6 профиля B на окне замера.</summary>
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
            new(4, "Backlog готовых: рост за окно замера", "среднее последней минуты ≤ среднее первой + rate × 1 с",
                growthValue, growth, null),
            Correctness(data.Correctness),
            Connections(samples, data.Mode),
        ];
        return MarkStand(verdicts, IsCpuBound(windows));
    }

    /// <summary>SLO 1 и 2 комбинации <c>partitions</c> на окне замера.</summary>
    public static IReadOnlyList<SloVerdict> Partitions(RunData data) =>
        MarkStand([Throughput(WindowsIn(data.Windows, data.MeasureStart, data.MeasureEnd), data.Rate),
            StartLatency(data.Immediate)], IsCpuBound(data));

    /// <summary>SLO 6–9 сценария <c>vacuum</c>; окно замера — фазы 1–3.</summary>
    public static IReadOnlyList<SloVerdict> Vacuum(RunData data)
    {
        var phases = data.Phases ?? throw new ArgumentException("У прогона vacuum нет границ фаз.", nameof(data));
        List<SloVerdict> verdicts =
        [
            Connections(SamplesIn(data.Samples, data.MeasureStart, data.MeasureEnd), data.Mode),
            QueueSize(data.Samples, phases),
            Stability(data, phases),
            .. Recovery(data, phases),
        ];
        return MarkStand(verdicts, IsCpuBound(data));
    }

    /// <summary>Недогруз: продюсер поставил в окне меньше 98 % от rate × 10 с — не справляется enqueue.</summary>
    public static bool IsUnderloaded(WindowStats w, int rate) => w.Enqueued * 100 < 98L * rate * 10;

    /// <summary>Выполнено в окне не меньше 95 % от rate × 10 с.</summary>
    public static bool MeetsThroughput(WindowStats w, int rate) => w.Completed * 100 >= 95L * rate * 10;

    /// <summary>Упор в CPU стенда: загрузка выше 80 % ядер больше чем в 10 % окон.</summary>
    public static bool IsCpuBound(IReadOnlyList<WindowStats> windows) =>
        windows.Count(w => w.CpuFraction > CpuBusyFraction) * 10 > windows.Count;

    public static bool IsCpuBound(RunData data) =>
        IsCpuBound(WindowsIn(data.Windows, data.MeasureStart, data.MeasureEnd));

    private static SloVerdict Throughput(List<WindowStats> windows, int rate)
    {
        const string name = "Пропускная способность", threshold = "каждое окно 10 с: выполнено ≥ 95 % rate";
        if (windows.Count == 0) return NoData(1, name, threshold);

        var underloaded = windows.Count(w => IsUnderloaded(w, rate));
        var slow = windows.Count(w => !MeetsThroughput(w, rate));
        var minShare = windows.Min(w => w.Completed * 100.0 / (rate * 10L));
        return new(1, name, threshold,
            Invariant($"мин. {minShare:F1} % rate; окон ниже порога: {slow}, недогруз: {underloaded}"),
            Result(slow == 0 && underloaded == 0), underloaded > 0 ? EnqueueMark : null);
    }

    private static SloVerdict StartLatency(LatencySummary s)
    {
        const string name = "Задержка старта немедленных", threshold = "p50 ≤ 50 мс, p99 ≤ 500 мс, p99.9 ≤ 2 с";
        return s.Count == 0
            ? NoData(2, name, threshold)
            : new(2, name, threshold, Invariant($"p50 {s.P50Ms:F1} мс, p99 {s.P99Ms:F1} мс, p99.9 {s.P999Ms:F1} мс"),
                Result(StartLatencyHolds(s)), null);
    }

    private static SloVerdict Lateness(LatencySummary s)
    {
        const string name = "Опоздание отложенных", threshold = "p99 ≤ 1,5 с";
        return s.Count == 0
            ? NoData(3, name, threshold)
            : new(3, name, threshold, Invariant($"p99 {s.P99Ms:F1} мс"), Result(s.P99Ms <= MaxDelayedP99Ms), null);
    }

    private static SloVerdict BacklogEachSample(List<Sample> samples, int rate)
    {
        const string name = "Backlog готовых: каждый срез", threshold = "≤ rate × 1 с";
        return samples.Max(s => s.ReadyBacklog) is not { } max
            ? NoData(4, name, threshold)
            : new(4, name, threshold, Invariant($"макс. {max}"), Result(max <= rate), null);
    }

    /// <summary>
    /// Вторая часть SLO 4: среднее backlog за последнюю минуту отрезка не больше среднего за первую + rate × 1 с.
    /// На отрезке короче 2 мин минуты перекрываются — «нет данных».
    /// </summary>
    private static (Verdict Result, string Value) BacklogGrowth(IReadOnlyList<Sample> samples, TimeSpan from, TimeSpan to, int rate)
    {
        if (to - from < 2 * Minute) return (Verdict.NoData, "отрезок короче 2 мин");
        var first = Mean(samples, from, from + Minute, s => s.ReadyBacklog);
        var last = Mean(samples, to - Minute, to, s => s.ReadyBacklog);
        if (first is null || last is null) return (Verdict.NoData, "нет срезов");
        return (Result(last <= first + rate), Invariant($"первая минута {first:F0}, последняя {last:F0}"));
    }

    private static SloVerdict Correctness(CorrectnessData? c)
    {
        const string name = "Корректность", threshold = "все jobs Succeeded, queue пуста, выполнения и attempt по Seq, без потерь и ошибок";
        if (c is null) return NoData(5, name, threshold);

        var holds = c is
        {
            MissingExecutions: 0, DuplicatedExecutions: 0, JobsNotSucceeded: 0, QueueRowsAfterDrain: 0,
            DrainedInTime: true, AttemptMismatches: 0, OutcomeDropped: 0, StorageErrors: 0,
        };
        var value =
            $"пропуски {c.MissingExecutions}, лишние выполнения {c.DuplicatedExecutions}, " +
            $"исключено Seq неуспешных вызовов {c.ExcludedFailedCalls}, jobs не Succeeded {c.JobsNotSucceeded}, " +
            $"queue после дренажа {c.QueueRowsAfterDrain}{(c.DrainedInTime ? "" : " (дренаж дольше 2 мин)")}, " +
            $"неверный attempt {c.AttemptMismatches}, outcome.dropped {c.OutcomeDropped}, storage.errors {c.StorageErrors}";
        return new(5, name, threshold, value, Result(holds), null);
    }

    private static SloVerdict Connections(List<Sample> samples, LoadMode mode)
    {
        const string name = "Соединения";
        if (mode is LoadMode.Bouncer or LoadMode.BouncerPrepared)
        {
            const string bouncerThreshold = "maxwait ≤ 100 мс в каждом срезе";
            if (samples.Max(s => s.MaxWaitMicros) is not { } maxWait) return NoData(6, name, bouncerThreshold);

            var value = Invariant($"maxwait макс. {maxWait / 1_000.0:F1} мс") + (samples.Max(s => s.ClWaiting) is not { } clWaiting
                ? ""
                : Invariant($"; cl_waiting макс. {clWaiting}, ср. {samples.Average(s => s.ClWaiting):F1}"));
            return new(6, name, bouncerThreshold, value, Result(maxWait <= MaxBouncerWaitMicros), null);
        }

        const string directThreshold = "соединений хостов и продюсера ≤ 48";
        return samples.Max(s => s.Connections) is not { } connections
            ? NoData(6, name, directThreshold)
            : new(6, name, directThreshold, Invariant($"макс. {connections}"), Result(connections <= MaxDirectConnections), null);
    }

    private static SloVerdict QueueSize(IReadOnlyList<Sample> samples, PhaseMarks p)
    {
        const string name = "Размер queue в фазе 1", threshold = "последняя минута ≤ 2 × среднее за 60–120 с";
        // Последняя минута не должна перекрываться с отрезком 60–120 с.
        if (p.Phase2Start - p.Phase1Start < 3 * Minute) return NoData(7, name, threshold);

        var early = Mean(samples, p.Phase1Start + Minute, p.Phase1Start + 2 * Minute, s => s.QueueTotalBytes);
        var last = Mean(samples, p.Phase2Start - Minute, p.Phase2Start, s => s.QueueTotalBytes);
        if (early is null || last is null) return NoData(7, name, threshold);
        return new(7, name, threshold, Invariant($"60–120 с {early / 1_048_576:F1} МБ, последняя минута {last / 1_048_576:F1} МБ"),
            Result(last <= 2 * early), null);
    }

    /// <summary>SLO 8: SLO 1–4 в каждом окне фазы 1, перцентили SLO 2 и 3 — по окну.</summary>
    private static SloVerdict Stability(RunData data, PhaseMarks p)
    {
        const string name = "Стабильность в фазе 1", threshold = "SLO 1–4 в каждом окне 10 с фазы 1";
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
            Invariant($"окон с нарушением SLO 1–3: {violated} из {windows.Count}; срезов с backlog выше rate: {backlogOver}; рост backlog: {growthValue}"),
            result, underloaded > 0 ? EnqueueMark : null);
    }

    /// <summary>
    /// SLO 9: момент восстановления T — начало первого окна фазы 3, с которого во всех окнах до её конца
    /// выполняются SLO 1, p99 SLO 2 по окну и первая часть SLO 4; затем вторая часть SLO 4 на отрезке [T, конец).
    /// Недогруз в окнах до T отодвигает T и помечает вердикт «enqueue».
    /// </summary>
    private static SloVerdict[] Recovery(RunData data, PhaseMarks p)
    {
        const string name = "Восстановление", threshold = "T ≤ начало фазы 3 + 5 мин";
        const string growthName = "Восстановление: рост backlog после T", growthThreshold = "среднее последней минуты ≤ среднее первой минуты после T + rate × 1 с";
        var windows = WindowsIn(data.Windows, p.Phase3Start, p.End);
        if (windows.Count == 0) return [NoData(9, name, threshold), NoData(9, growthName, growthThreshold)];

        var rate = data.Rate;
        var lastBad = windows.FindLastIndex(w =>
            IsUnderloaded(w, rate) || !MeetsThroughput(w, rate)
            || w.Immediate.Count > 0 && w.Immediate.P99Ms > MaxImmediateP99Ms
            || data.Samples.Any(s => s.At >= w.Start && s.At < w.Start + WindowLength && s.ReadyBacklog > rate));
        var mark = windows.Take(lastBad + 1).Any(w => IsUnderloaded(w, rate)) ? EnqueueMark : null;
        if (lastBad == windows.Count - 1)
            return [new(9, name, threshold, "не восстановилось до конца фазы 3", Verdict.Fail, mark), NoData(9, growthName, growthThreshold)];

        var t = windows[lastBad + 1].Start;
        var (growth, growthValue) = BacklogGrowth(data.Samples, t, p.End, rate);
        return
        [
            new(9, name, threshold, Invariant($"T = начало фазы 3 + {(t - p.Phase3Start).TotalSeconds:F0} с"),
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
        new(number, name, threshold, "нет данных", Verdict.NoData, null);
}
