using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Slo;
using static System.FormattableString;

namespace PgBolt.LoadTest.Report;

/// <summary>Пропуски среза: пропущенные тики, попытки без единого удачного запроса и неудачные запросы.</summary>
internal sealed record SamplerSummary(long SkippedTicks, long FailedAttempts, long FailedQueries);

/// <summary>Счётчики <c>Meter</c> PgBolt хостов и продюсера; <see cref="Enqueued"/> — справочно.</summary>
internal sealed record PgBoltCounters(long OutcomeDropped, IReadOnlyDictionary<string, long> StorageErrors, long Enqueued)
{
    public long StorageErrorsTotal => StorageErrors.Values.Sum();
}

/// <summary>
/// Всё, что пишется в отчёт одного прогона; <see cref="Aborted"/> — прогон прерван, значения неполные;
/// <see cref="Ramp"/> — итог сценария <c>ramp</c>; <see cref="WindowRamp"/> — для каждого окна <c>Data.Windows</c> rate
/// его ступени и признак «оценивается» (окно не из первой трети ступени); <see cref="Combination"/> — комбинация
/// сценария <c>partitions</c>: из неё в отчёт идут P, набор индексов и блокировки statement'ов;
/// <see cref="VacuumOutcome"/> — входы выбора параметров autovacuum прогона <c>vacuum</c>, <c>null</c> — нет данных;
/// <see cref="DuplicatedExecutions"/> — лишние выполнения прогона без дренажа (при проверке корректности берутся из неё).
/// </summary>
internal sealed record ReportModel(
    LoadTestOptions Options, LoadMode Mode, DateTime StartedAt, string Schema,
    IReadOnlyList<(string Key, string Value)> Environment, IReadOnlyList<SloVerdict> Verdicts, RunData Data,
    StatementsSummary Statements, SamplerSummary Sampler, PgBoltCounters Counters,
    long ImmediateOverflows, long DelayedOverflows, IReadOnlyList<string> Warnings, bool Aborted,
    RampResult? Ramp = null, IReadOnlyList<(int Rate, bool Evaluated)>? WindowRamp = null,
    CombinationResult? Combination = null, ModeOutcome? VacuumOutcome = null, long? DuplicatedExecutions = null);

/// <summary>
/// Отчёт прогона: <c>&lt;yyyyMMdd-HHmm&gt;-&lt;сценарий&gt;-&lt;режим&gt;.md</c> (у комбинации <c>partitions</c> — с
/// суффиксом <c>-p&lt;P&gt;-&lt;индексы&gt;</c>) и рядом <c>.csv</c> с посекундными срезами; сводка <c>partitions</c> —
/// <c>&lt;yyyyMMdd-HHmm&gt;-partitions-summary.md</c>. Существующие файлы не перезаписываются: к имени добавляется
/// <c>-2</c>, <c>-3</c>…
/// </summary>
internal static class ReportWriter
{
    private const string NoData = "нет данных";

    public static async Task<string> WriteAsync(string outDir, ReportModel model)
    {
        var name = $"{model.StartedAt:yyyyMMdd-HHmm}-{Kebab(model.Options.Scenario)}-{Kebab(model.Mode)}"
                   + (model.Combination is { } c ? $"-p{c.P}-{Kebab(c.Indexes)}" : "");
        var path = FreePath(outDir, name);
        await File.WriteAllTextAsync(path, Markdown(model));
        await File.WriteAllTextAsync(Path.ChangeExtension(path, ".csv"), Csv(model.Data.Samples));
        return path;
    }

    /// <summary>
    /// Сводка <c>partitions</c>: SLO 1 и 2 и измерения каждой комбинации, statement'ы горячего пути, порог P по режимам и
    /// наборам индексов и ёмкость общей таблицы блокировок.
    /// </summary>
    public static async Task<string> WriteSummaryAsync(string outDir, DateTime startedAt, LoadTestOptions options,
        IReadOnlyList<CombinationResult> results, long lockTableCapacity, IReadOnlyList<string> warnings)
    {
        var md = new StringBuilder();
        md.AppendLine("# Нагрузочный прогон: partitions, сводка").AppendLine();

        Section(md, "Параметры прогона");
        Table(md, ["Параметр", "Значение"],
        [
            ["Старт", startedAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)],
            ["rate", Invariant($"{options.Rate}/с")],
            ["Прогрев", Invariant($"{options.Warmup.TotalSeconds} с")],
            ["Окно замера", Invariant($"{options.Duration.TotalSeconds} с на комбинацию")],
            ["Ёмкость таблицы блокировок", Invariant($"{lockTableCapacity}")],
        ]);

        Section(md, "Комбинации");
        Table(md,
            ["Режим", "Индексы", "P", "SLO 1", "SLO 2", "p99 немедл., мс", "Ограничено стендом", "LockManager, макс.",
             "LockManager, среднее", "storage.errors", "Сбой"],
            results.Select(r => new[]
            {
                Kebab(r.Mode), Kebab(r.Indexes), Invariant($"{r.P}"), Result(r.Slo1), Result(r.Slo2),
                r.ImmediateP99Ms is { } p99 ? Invariant($"{p99:F1}") : NoData, r.CpuBound ? "да" : "",
                r.LockManagerWaitsMax is { } max ? Invariant($"{max}") : NoData,
                r.LockManagerWaitsMean is { } mean ? Invariant($"{mean:F2}") : NoData,
                r.StorageErrors is { } errors ? Invariant($"{errors}") : NoData, r.Failure ?? "",
            }));

        Section(md, "Statement'ы");
        Table(md,
            ["Режим", "Индексы", "P", "Statement", "План", "Блокировок вне fast-path", "Блокировок всего", "Оценка",
             "mean plan, мс", "mean exec, мс"],
            results.SelectMany(r => r.Statements?.Select(s => (string[])
            [
                Kebab(r.Mode), Kebab(r.Indexes), Invariant($"{r.P}"), .. StatementCells(s.Key, s.Value),
                Ms(s.Value.Stats?.MeanPlanTime), Ms(s.Value.Stats?.MeanExecTime),
            ]) ?? []));

        Section(md, "Порог P");
        md.AppendLine("Наименьшее P матрицы, с которого режим и набор индексов нарушают SLO 1 или 2.").AppendLine();
        Table(md, ["Режим", "Индексы", "Порог P"],
            results.Select(r => (r.Mode, r.Indexes)).Distinct().Select(k => new[]
            {
                Kebab(k.Mode), Kebab(k.Indexes),
                PartitionsVerdict.Threshold(results, k.Mode, k.Indexes) is { } p ? Invariant($"{p}") : "нет",
            }));

        Section(md, "Предупреждения");
        md.AppendLine(warnings.Count == 0 ? "нет" : string.Join(Environment.NewLine, warnings.Select(w => "- " + w)));

        var path = FreePath(outDir, $"{startedAt:yyyyMMdd-HHmm}-partitions-summary");
        await File.WriteAllTextAsync(path, md.ToString());
        return path;
    }

    /// <summary>Путь <c>.md</c>, у которого нет ни отчёта, ни <c>.csv</c> рядом.</summary>
    private static string FreePath(string outDir, string name)
    {
        Directory.CreateDirectory(outDir);
        var path = Path.Combine(outDir, name + ".md");
        for (var n = 2; File.Exists(path) || File.Exists(Path.ChangeExtension(path, ".csv")); n++)
            path = Path.Combine(outDir, $"{name}-{n}.md");
        return path;
    }

    /// <summary>Statement, план, блокировки вне fast-path и всего, измерены они или оценены.</summary>
    private static string[] StatementCells(string statement, StatementResult s) =>
    [
        statement, s.Plan == PlanKind.Generic ? "generic" : "custom", Invariant($"{s.Locks.NonFastPath}"),
        Invariant($"{s.Locks.Total}"), s.LowerBound ? "нижняя оценка" : "измерение",
    ];

    private static string Kebab<T>(T value) where T : Enum =>
        Regex.Replace(value.ToString(), "(?<!^)([A-Z])", "-$1").ToLowerInvariant();

    private static string Markdown(ReportModel m)
    {
        var d = m.Data;
        var measured = m.WindowRamp is { } ramped
            ? d.Windows.Where((_, i) => ramped[i].Evaluated).ToList()
            : SloEvaluator.WindowsIn(d.Windows, d.MeasureStart, d.MeasureEnd);
        // Недогруз окна считается от rate его ступени.
        int RateOf(int windowIndex) => m.WindowRamp?[windowIndex].Rate ?? d.Rate;
        var md = new StringBuilder();
        md.AppendLine($"# Нагрузочный прогон: {Kebab(m.Options.Scenario)}, {Kebab(m.Mode)}").AppendLine();
        if (m.Aborted)
            md.AppendLine("**Прогон прерван**: значения неполные, вердикты SLO не вычислялись.").AppendLine();

        Section(md, "Параметры прогона");
        var o = m.Options;
        string[][] load = m.Ramp is null
            ?
            [
                ["rate", Invariant($"{d.Rate}/с")],
                ["Прогрев", Invariant($"{o.Warmup.TotalSeconds} с")],
            ]
            :
            [
                ["Прогрев", "нет"],
                ["ramp-start", Invariant($"{o.RampStart}/с")],
                ["ramp-step", Invariant($"{o.RampStep}/с")],
                ["ramp-step-duration", Invariant($"{o.RampStepDuration.TotalSeconds} с")],
                ["ramp-max", Invariant($"{o.RampMax}/с")],
            ];
        Table(md, ["Параметр", "Значение"],
        [
            ["Старт", m.StartedAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)],
            ["Схема", m.Schema],
            .. load,
            ["Окно замера", Invariant($"{d.MeasureStart.TotalSeconds:F0}–{d.MeasureEnd.TotalSeconds:F0} с от начала прогона")],
            .. d.Phases is { } p
                ? new[] { new[] { "Фазы", Invariant($"1: {p.Phase1Start.TotalSeconds:F0}–{p.Phase2Start.TotalSeconds:F0} с, 2 (удержание xmin): до {p.Phase3Start.TotalSeconds:F0} с, 3: до {p.End.TotalSeconds:F0} с") } }
                : [],
        ]);

        if (m.Ramp is { } ramp)
        {
            Section(md, "Результат ramp");
            Table(md, ["Показатель", "Значение"],
            [
                ["Максимальный устойчивый rate", ramp.MaxSustainedRate is { } max
                    ? Invariant($"{max}/с") + (ramp.LimitedByStand ? " (ограничено стендом)" : "")
                    : "нет: нарушена первая ступень"],
                ["Нарушено первым", ramp.FirstViolation ?? "ничего до ramp-max"],
            ]);
        }

        Section(md, "Окружение");
        Table(md, ["Параметр", "Значение"], m.Environment.Select(e => new[] { e.Key, e.Value }));

        Section(md, "SLO");
        Table(md, ["#", "SLO", "Порог", "Значение", "Результат", "Пометка"], m.Verdicts.Select(v => new[]
        {
            v.Number.ToString(CultureInfo.InvariantCulture), v.Name, v.Threshold, v.Value,
            Result(v.Result), v.Mark ?? "",
        }));

        if (m.Options.Scenario == Scenario.Vacuum && !m.Aborted)
        {
            Section(md, "Входы выбора параметров autovacuum");
            if (m.VacuumOutcome is { } v)
                Table(md, ["SLO 7–9", "Пик размера queue в фазе 1, МБ", "p99 немедл. фазы 1, мс"],
                    [[v.Slo7To9 ? "да" : "нет", Invariant($"{v.PeakSizeBytes / 1_048_576:F1}"), Invariant($"{v.P99Ms:F1}")]]);
            else
                md.AppendLine(NoData).AppendLine();
        }

        Section(md, "Задержка старта");
        Table(md, ["Задачи", "Число", "p50, мс", "p90, мс", "p99, мс", "p99.9, мс", "max, мс", "Переполнений"],
        [
            Latency("немедленные", d.Immediate, m.ImmediateOverflows),
            Latency("отложенные", d.Delayed, m.DelayedOverflows),
        ]);

        Section(md, "Пропускная способность по окнам 10 с");
        Table(md,
            ["#", "Старт, с", "Замер", "Выполнено/с", "Поставлено/с", "Вызовов/с", "Ср. пачка", "Ошибок enqueue",
             "p99 enqueue, мс", "CPU, %", "Ожидание генератора, мс", "Отброшено токенов", "p99 немедл., мс",
             "p99 отлож., мс", "Недогруз"],
            d.Windows.Select((w, i) =>
            {
                // Окно короче 10 с (неполное окно прогрева) кончается там, где начинается следующее.
                var seconds = i + 1 < d.Windows.Count ? Math.Min((d.Windows[i + 1].Start - w.Start).TotalSeconds, 10) : 10;
                return new[]
                {
                    w.Index.ToString(CultureInfo.InvariantCulture), Invariant($"{w.Start.TotalSeconds:F0}"),
                    measured.Contains(w) ? "да" : "", Invariant($"{w.Completed / seconds:F0}"),
                    Invariant($"{w.Enqueued / seconds:F0}"), Invariant($"{w.EnqueueCalls / seconds:F1}"), BatchSize(w.Enqueued, w.EnqueueCalls),
                    Invariant($"{w.EnqueueErrors}"), Invariant($"{w.EnqueueP99Ms:F1}"), Invariant($"{w.CpuFraction * 100:F0}"),
                    Invariant($"{w.MaxGeneratorWait.TotalMilliseconds:F0}"), Invariant($"{w.DroppedTokens}"),
                    Invariant($"{w.Immediate.P99Ms:F1}"), Invariant($"{w.Delayed.P99Ms:F1}"),
                    measured.Contains(w) && SloEvaluator.IsUnderloaded(w, RateOf(i)) ? "да" : "",
                };
            }));

        var calls = measured.Sum(w => w.EnqueueCalls);
        Section(md, "Enqueue");
        Table(md, ["Показатель (окно замера)", "Значение"],
        [
            ["Вызовов EnqueueManyAsync/с", measured.Count == 0 ? NoData : Invariant($"{calls / (measured.Count * 10.0):F1}")],
            ["Средний размер пачки", BatchSize(measured.Sum(w => w.Enqueued), calls)],
            ["Ошибок EnqueueManyAsync", Invariant($"{measured.Sum(w => w.EnqueueErrors)}")],
            ["Seq неуспешных вызовов", d.Correctness is { } c ? Invariant($"{c.ExcludedFailedCalls}") : NoData],
            ["Окон недогруза", Invariant($"{d.Windows.Where((w, i) => measured.Contains(w) && SloEvaluator.IsUnderloaded(w, RateOf(i))).Count()} из {measured.Count}")],
        ]);

        Section(md, "Загрузка CPU стенда");
        Table(md, ["Показатель (окно замера)", "Значение"],
        [
            ["Среднее, % ядер", Stat(measured.Average(w => (double?)w.CpuFraction * 100), "F0")],
            ["Максимум, % ядер", Stat(measured.Max(w => (double?)w.CpuFraction * 100), "F0")],
            ["Окон выше 80 %", Invariant($"{measured.Count(w => w.CpuFraction > 0.8)} из {measured.Count}")],
        ]);

        Section(md, "pg_stat_statements");
        if (m.Statements.Classes is { } classes)
            Table(md, ["Класс", "calls", "mean plan, мс", "mean exec, мс", "total plan, мс", "total exec, мс"],
                classes.OrderBy(c => c.Key, StringComparer.Ordinal).Select(c => new[]
                {
                    c.Key, Invariant($"{c.Value.Calls}"), Ms(c.Value.MeanPlanTime), Ms(c.Value.MeanExecTime),
                    Ms(c.Value.TotalPlanTime), Ms(c.Value.TotalExecTime),
                }));
        else
            md.AppendLine(NoData).AppendLine();

        if (m.Combination?.Statements is { } probed)
        {
            Section(md, "Блокировки statement'ов");
            Table(md, ["Statement", "План", "Вне fast-path", "Всего", "Оценка"],
                probed.Select(p => StatementCells(p.Key, p.Value)));
        }

        var s = d.Samples;
        Section(md, "Bloat и autovacuum");
        Table(md, ["Показатель (за прогон)", "Значение"],
        [
            ["Размер queue, макс. МБ", Stat(s.Max(x => x.QueueTotalBytes / 1_048_576.0), "F1")],
            ["n_dead_tup queue, макс.", Stat(s.Max(x => x.Queue?.DeadTuples), "F0")],
            ["n_dead_tup completions, макс.", Stat(s.Max(x => x.Completions?.DeadTuples), "F0")],
            ["autovacuum queue, раз", Delta(s, x => x.Queue?.AutovacuumCount)],
            ["autovacuum completions, раз", Delta(s, x => x.Completions?.AutovacuumCount)],
            ["Активных autovacuum workers, макс.", Stat(s.Max(x => x.AutovacuumWorkers), "F0")],
            ["Размер БД в конце прогона, МБ", Stat(s.LastOrDefault(x => x.DatabaseBytes is not null)?.DatabaseBytes / 1_048_576.0, "F1")],
        ]);

        Section(md, "Checkpoint'ы");
        Table(md, ["Показатель (за прогон)", "Значение"],
        [
            ["num_timed", Delta(s, x => x.Checkpointer?.NumTimed)],
            ["num_requested", Delta(s, x => x.Checkpointer?.NumRequested)],
            ["write_time, мс", Delta(s, x => (long?)x.Checkpointer?.WriteTimeMs)],
            ["sync_time, мс", Delta(s, x => (long?)x.Checkpointer?.SyncTimeMs)],
            ["buffers_written", Delta(s, x => x.Checkpointer?.BuffersWritten)],
            ["WAL, байт", Delta(s, x => x.WalBytes)],
        ]);

        Section(md, "Ожидания LockManager");
        Table(md, ["Показатель (за прогон)", "Значение"],
        [
            ["Backend'ов в ожидании, макс.", Stat(s.Max(x => x.LockManagerWaits), "F0")],
            ["Backend'ов в ожидании, среднее", Stat(s.Average(x => x.LockManagerWaits), "F2")],
        ]);

        Section(md, "Корректность и счётчики PgBolt");
        var r = d.Correctness;
        Table(md, ["Показатель", "Значение"],
        [
            ["Пропущенные выполнения", r is null ? NoData : Invariant($"{r.MissingExecutions}")],
            ["Повторные (лишние) выполнения", (r?.DuplicatedExecutions ?? m.DuplicatedExecutions) is { } dup ? Invariant($"{dup}") : NoData],
            ["Seq неуспешных вызовов вне сверки", r is null ? NoData : Invariant($"{r.ExcludedFailedCalls}")],
            ["jobs не в Succeeded", r is null ? NoData : Invariant($"{r.JobsNotSucceeded}")],
            ["queue после дренажа", r is null ? NoData : Invariant($"{r.QueueRowsAfterDrain}")],
            ["Дренаж уложился в 2 мин", r is null ? NoData : r.DrainedInTime ? "да" : "нет"],
            ["jobs с неверным attempt", r is null ? NoData : Invariant($"{r.AttemptMismatches}")],
            ["pgbolt.job.outcome.dropped", Invariant($"{m.Counters.OutcomeDropped}")],
            ["pgbolt.storage.errors", m.Counters.StorageErrors.Count == 0
                ? "0"
                : string.Join(", ", m.Counters.StorageErrors.OrderBy(e => e.Key, StringComparer.Ordinal).Select(e => Invariant($"{e.Key} {e.Value}")))],
            ["pgbolt.job.enqueued (справочно)", Invariant($"{m.Counters.Enqueued}")],
        ]);

        Section(md, "Срез");
        Table(md, ["Показатель (за прогон)", "Значение"],
        [
            ["Ежесекундные запросы, среднее мс", Stat(s.Average(x => x.QueryTime?.TotalMilliseconds), "F1")],
            ["Ежесекундные запросы, макс. мс", Stat(s.Max(x => x.QueryTime?.TotalMilliseconds), "F1")],
            ["Счёт по queue раз в 10 с, среднее мс", Stat(s.Average(x => x.CountsTime?.TotalMilliseconds), "F1")],
            ["Счёт по queue раз в 10 с, макс. мс", Stat(s.Max(x => x.CountsTime?.TotalMilliseconds), "F1")],
            ["Пропущенных тиков", Invariant($"{m.Sampler.SkippedTicks}")],
            ["Неудачных запросов", Invariant($"{m.Sampler.FailedQueries}")],
            ["Неудачных попыток", Invariant($"{m.Sampler.FailedAttempts}")],
        ]);

        Section(md, "Предупреждения");
        md.AppendLine(m.Warnings.Count == 0 ? "нет" : string.Join(Environment.NewLine, m.Warnings.Select(w => "- " + w)));
        return md.ToString();
    }

    private static string Csv(IReadOnlyList<Sample> samples)
    {
        var csv = new StringBuilder(
            "at_s,ready_backlog,queue_total_bytes,max_wait_us,cl_waiting,sv_active,sv_idle,connections,completed,delayed," +
            "leased,lock_manager_waits,autovacuum_workers,queue_live_tup,queue_dead_tup,completions_live_tup," +
            "completions_dead_tup,wal_bytes,vacuums,query_ms,counts_ms\n");
        foreach (var s in samples)
        {
            object?[] values =
            [
                s.At.TotalSeconds, s.ReadyBacklog, s.QueueTotalBytes, s.MaxWaitMicros, s.ClWaiting, s.SvActive, s.SvIdle,
                s.Connections, s.Completed, s.Delayed, s.Leased, s.LockManagerWaits, s.AutovacuumWorkers,
                s.Queue?.LiveTuples, s.Queue?.DeadTuples, s.Completions?.LiveTuples, s.Completions?.DeadTuples, s.WalBytes,
                s.Vacuums is null ? null : string.Join(';', s.Vacuums.Select(v => $"{v.Relation}:{v.Phase}")),
                s.QueryTime?.TotalMilliseconds, s.CountsTime?.TotalMilliseconds,
            ];
            csv.AppendJoin(',', values.Select(v => Convert.ToString(v, CultureInfo.InvariantCulture))).Append('\n');
        }

        return csv.ToString();
    }

    private static string Result(Verdict v) => v switch { Verdict.Pass => "PASS", Verdict.Fail => "FAIL", _ => NoData };

    private static void Section(StringBuilder md, string title) => md.AppendLine($"## {title}").AppendLine();

    private static void Table(StringBuilder md, string[] header, IEnumerable<string[]> rows)
    {
        md.AppendLine(Row(header)).AppendLine(Row(header.Select(_ => "---")));
        foreach (var row in rows) md.AppendLine(Row(row));
        md.AppendLine();
    }

    private static string Row(IEnumerable<string> cells) => "| " + string.Join(" | ", cells) + " |";

    private static string[] Latency(string name, LatencySummary l, long overflows) =>
        l.Count == 0
            ? [name, "0", NoData, NoData, NoData, NoData, NoData, Invariant($"{overflows}")]
            : [name, Invariant($"{l.Count}"), Invariant($"{l.P50Ms:F1}"), Invariant($"{l.P90Ms:F1}"), Invariant($"{l.P99Ms:F1}"),
               Invariant($"{l.P999Ms:F1}"), Invariant($"{l.MaxMs:F1}"), Invariant($"{overflows}")];

    private static string BatchSize(long enqueued, long calls) => calls == 0 ? NoData : Invariant($"{(double)enqueued / calls:F1}");

    private static string Ms(double? value) => value is { } v ? Invariant($"{v:F3}") : NoData;

    /// <summary>Значение агрегата; <c>null</c> — значений не было.</summary>
    private static string Stat(double? value, string format) => value?.ToString(format, CultureInfo.InvariantCulture) ?? NoData;

    /// <summary>Разница между последним и первым срезом, где значение есть.</summary>
    private static string Delta(IReadOnlyList<Sample> samples, Func<Sample, long?> value)
    {
        var values = samples.Select(value).OfType<long>().ToList();
        return values.Count == 0 ? NoData : Invariant($"{values[^1] - values[0]}");
    }
}
