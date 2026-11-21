using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Slo;
using static System.FormattableString;

namespace PgBolt.LoadTest.Report;

/// <summary>Sampling gaps: skipped ticks, attempts with no successful query, and failed queries.</summary>
internal sealed record SamplerSummary(long SkippedTicks, long FailedAttempts, long FailedQueries);

/// <summary>PgBolt <c>Meter</c> counters for hosts and producer. <see cref="Enqueued"/> is informational.</summary>
internal sealed record PgBoltCounters(long OutcomeDropped, IReadOnlyDictionary<string, long> StorageErrors, long Enqueued)
{
    public long StorageErrorsTotal => StorageErrors.Values.Sum();
}

/// <summary>
/// Data written to one run report. <see cref="Aborted"/> marks an interrupted run with incomplete measurements. <see cref="Ramp"/> holds the
/// <c>ramp</c> result. For each <c>Data.Windows</c> window, <see cref="WindowRamp"/> records the stage rate and evaluation flag (outside the
/// first third of the stage). <see cref="Combination"/> identifies the <c>partitions</c> combination, supplying P, the index set, and statement
/// locks. <see cref="VacuumOutcome"/> supplies autovacuum selection inputs for <c>vacuum</c>, or <c>null</c> for missing data.
/// <see cref="DuplicatedExecutions"/> counts excess executions for runs without a drain. Runs with correctness verification use the count from
/// that check.
/// </summary>
internal sealed record ReportModel(
    LoadTestOptions Options, LoadMode Mode, DateTime StartedAt, string Schema,
    IReadOnlyList<(string Key, string Value)> Environment, IReadOnlyList<SloVerdict> Verdicts, RunData Data,
    StatementsSummary Statements, SamplerSummary Sampler, PgBoltCounters Counters,
    long ImmediateOverflows, long DelayedOverflows, IReadOnlyList<string> Warnings, bool Aborted,
    RampResult? Ramp = null, IReadOnlyList<(int Rate, bool Evaluated)>? WindowRamp = null,
    CombinationResult? Combination = null, ModeOutcome? VacuumOutcome = null, long? DuplicatedExecutions = null);

/// <summary>
/// Run report <c>&lt;yyyyMMdd-HHmm&gt;-&lt;scenario&gt;-&lt;mode&gt;.md</c>, with suffix <c>-p&lt;P&gt;-&lt;indexes&gt;</c> for a
/// <c>partitions</c> combination and an adjacent <c>.csv</c> of per-second samples. The <c>partitions</c> summary is
/// <c>&lt;yyyyMMdd-HHmm&gt;-partitions-summary.md</c>. Appends <c>-2</c>, <c>-3</c>, etc. to avoid overwriting existing files.
/// </summary>
internal static class ReportWriter
{
    private const string NoData = "no data";

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
    /// Summarizes <c>partitions</c> SLOs 1 and 2, measurements per combination, hot-path statements, P thresholds by mode and index set, and shared
    /// lock table capacity.
    /// </summary>
    public static async Task<string> WriteSummaryAsync(string outDir, DateTime startedAt, LoadTestOptions options,
        IReadOnlyList<CombinationResult> results, long lockTableCapacity, IReadOnlyList<string> warnings)
    {
        var md = new StringBuilder();
        md.AppendLine("# Load test: partitions summary").AppendLine();

        Section(md, "Run parameters");
        Table(md, ["Parameter", "Value"],
        [
            ["Start", startedAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)],
            ["rate", Invariant($"{options.Rate}/s")],
            ["Warmup", Invariant($"{options.Warmup.TotalSeconds} s")],
            ["Measurement window", Invariant($"{options.Duration.TotalSeconds} s per combination")],
            ["--queue-profile", Kebab(options.Queues)],
            ["Lock table capacity", Invariant($"{lockTableCapacity}")],
        ]);

        Section(md, "Combinations");
        Table(md,
            ["Mode", "Indexes", "P", "SLO 1", "SLO 2", "Immediate p99, ms", "Harness-limited", "LockManager, max",
             "LockManager, mean", "storage.errors", "Failure"],
            results.Select(r => new[]
            {
                Kebab(r.Mode), Kebab(r.Indexes), Invariant($"{r.P}"), Result(r.Slo1), Result(r.Slo2),
                r.ImmediateP99Ms is { } p99 ? Invariant($"{p99:F1}") : NoData, r.CpuBound ? "yes" : "",
                r.LockManagerWaitsMax is { } max ? Invariant($"{max}") : NoData,
                r.LockManagerWaitsMean is { } mean ? Invariant($"{mean:F2}") : NoData,
                r.StorageErrors is { } errors ? Invariant($"{errors}") : NoData, r.Failure ?? "",
            }));

        Section(md, "Statements");
        Table(md,
            ["Mode", "Indexes", "P", "Statement", "Plan", "Non-fast-path locks", "Total locks", "Measurement type",
             "mean plan, ms", "mean exec, ms"],
            results.SelectMany(r => r.Statements?.Select(s => (string[])
            [
                Kebab(r.Mode), Kebab(r.Indexes), Invariant($"{r.P}"), .. StatementCells(s.Key, s.Value),
                Ms(s.Value.Stats?.MeanPlanTime), Ms(s.Value.Stats?.MeanExecTime),
            ]) ?? []));

        FetchTable(md, results);

        Section(md, "P threshold");
        md.AppendLine("Smallest P in the matrix at which a mode and index set violate SLO 1 or 2.").AppendLine();
        Table(md, ["Mode", "Indexes", "P threshold"],
            results.Select(r => (r.Mode, r.Indexes)).Distinct().Select(k => new[]
            {
                Kebab(k.Mode), Kebab(k.Indexes),
                PartitionsVerdict.Threshold(results, k.Mode, k.Indexes) is { } p ? Invariant($"{p}") : "none",
            }));

        Section(md, "Warnings");
        md.AppendLine(warnings.Count == 0 ? "none" : string.Join(Environment.NewLine, warnings.Select(w => "- " + w)));

        var path = FreePath(outDir, $"{startedAt:yyyyMMdd-HHmm}-partitions-summary");
        await File.WriteAllTextAsync(path, md.ToString());
        return path;
    }

    /// <summary>Returns a <c>.md</c> path with neither an existing report nor an adjacent <c>.csv</c>.</summary>
    private static string FreePath(string outDir, string name)
    {
        Directory.CreateDirectory(outDir);
        var path = Path.Combine(outDir, name + ".md");
        for (var n = 2; File.Exists(path) || File.Exists(Path.ChangeExtension(path, ".csv")); n++)
            path = Path.Combine(outDir, $"{name}-{n}.md");
        return path;
    }

    /// <summary>Statement, plan, non-fast-path and total locks, and whether those counts were measured or estimated.</summary>
    private static string[] StatementCells(string statement, StatementResult s) =>
    [
        statement, s.Locks is null ? NoData : s.Plan == PlanKind.Generic ? "generic" : "custom",
        s.Locks?.NonFastPath.ToString() ?? NoData, s.Locks?.Total.ToString() ?? NoData,
        s.Locks is null ? NoData : s.LowerBound ? "lower bound" : "measured",
    ];

    private static void FetchTable(StringBuilder md, IEnumerable<CombinationResult> results)
    {
        Section(md, "Job fetches");
        Table(md, ["Mode", "P", "Indexes", "Calls", "Empty", "With jobs", "Errors", "Call p99, ms",
            "Advisory lock samples", "Lock p99, ms", "Database sends", "Sends/call"], results.Select(r =>
        {
            var f = r.Fetches;
            return new[] { Kebab(r.Mode), Invariant($"{r.P}"), Kebab(r.Indexes),
                f?.Duration.Count.ToString() ?? NoData, f?.Empty.ToString() ?? NoData,
                (f?.Duration.Count - f?.Empty - f?.Failed)?.ToString() ?? NoData, f?.Failed.ToString() ?? NoData,
                Ms(f?.Duration.Count > 0 ? f.Duration.P99Ms : null), f?.QueueLock.Count.ToString() ?? NoData,
                Ms(f?.QueueLock.Count > 0 ? f.QueueLock.P99Ms : null), f?.Sent.ToString() ?? NoData,
                f?.Duration.Count > 0 ? Invariant($"{(double)f.Sent / f.Duration.Count:F3}") : NoData };
        }));
    }

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
        md.AppendLine($"# Load test: {Kebab(m.Options.Scenario)}, {Kebab(m.Mode)}").AppendLine();
        if (m.Aborted)
            md.AppendLine("**Run aborted**. Measurements are incomplete. SLOs were not evaluated.").AppendLine();

        Section(md, "Run parameters");
        var o = m.Options;
        string[][] load = m.Ramp is null
            ?
            [
                ["rate", Invariant($"{d.Rate}/s")],
                ["Warmup", Invariant($"{o.Warmup.TotalSeconds} s")],
            ]
            :
            [
                ["Warmup", "none"],
                ["ramp-start", Invariant($"{o.RampStart}/s")],
                ["ramp-step", Invariant($"{o.RampStep}/s")],
                ["ramp-step-duration", Invariant($"{o.RampStepDuration.TotalSeconds} s")],
                ["ramp-max", Invariant($"{o.RampMax}/s")],
            ];
        Table(md, ["Parameter", "Value"],
        [
            ["Start", m.StartedAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)],
            ["Schema", m.Schema],
            .. load,
            ["Measurement window", Invariant($"{d.MeasureStart.TotalSeconds:F0}–{d.MeasureEnd.TotalSeconds:F0} s from the start of the run")],
            .. d.Phases is { } p
                ? new[] { new[] { "Phases", Invariant($"1: {p.Phase1Start.TotalSeconds:F0}–{p.Phase2Start.TotalSeconds:F0} s, 2 (holding xmin): until {p.Phase3Start.TotalSeconds:F0} s, 3: until {p.End.TotalSeconds:F0} s") } }
                : [],
        ]);

        if (m.Ramp is { } ramp)
        {
            Section(md, "Ramp result");
            Table(md, ["Metric", "Value"],
            [
                ["Maximum sustained rate", ramp.MaxSustainedRate is { } max
                    ? Invariant($"{max}/s") + (ramp.LimitedByStand ? " (harness-limited)" : "")
                    : "none (first step failed)"],
                ["First violation", ramp.FirstViolation ?? "none through ramp-max"],
            ]);
        }

        Section(md, "Environment");
        Table(md, ["Parameter", "Value"], m.Environment.Select(e => new[] { e.Key, e.Value }));

        Section(md, "SLO");
        Table(md, ["#", "SLO", "Threshold", "Value", "Result", "Note"], m.Verdicts.Select(v => new[]
        {
            v.Number.ToString(CultureInfo.InvariantCulture), v.Name, v.Threshold, v.Value,
            Result(v.Result), v.Mark ?? "",
        }));

        if (m.Options.Scenario == Scenario.Vacuum && !m.Aborted)
        {
            Section(md, "Autovacuum selection inputs");
            if (m.VacuumOutcome is { } v)
                Table(md, ["SLO 7–9", "Peak phase 1 queue size, MiB", "Phase 1 immediate p99, ms"],
                    [[v.Slo7To9 switch { Verdict.Pass => "yes", Verdict.Fail => "no", _ => NoData }, Invariant($"{v.PeakSizeBytes / 1_048_576:F1}"), Invariant($"{v.P99Ms:F1}")]]);
            else
                md.AppendLine(NoData).AppendLine();
        }

        Section(md, "Start latency");
        Table(md, ["Jobs", "Count", "p50, ms", "p90, ms", "p99, ms", "p99.9, ms", "max, ms", "Overflows"],
        [
            Latency("immediate", d.Immediate, m.ImmediateOverflows),
            Latency("delayed", d.Delayed, m.DelayedOverflows),
        ]);

        Section(md, "Throughput per 10 s window");
        Table(md,
            ["#", "Start, s", "Measured", "Completed/s", "Enqueued/s", "Calls/s", "Mean batch size", "Enqueue errors",
             "Enqueue p99, ms", "CPU, %", "Generator wait, ms", "Dropped tokens", "Immediate p99, ms",
             "Delayed p99, ms", "Underloaded"],
            d.Windows.Select((w, i) =>
            {
                // Окно короче 10 с (неполное окно прогрева) кончается там, где начинается следующее.
                var seconds = i + 1 < d.Windows.Count ? Math.Min((d.Windows[i + 1].Start - w.Start).TotalSeconds, 10) : 10;
                return new[]
                {
                    w.Index.ToString(CultureInfo.InvariantCulture), Invariant($"{w.Start.TotalSeconds:F0}"),
                    measured.Contains(w) ? "yes" : "", Invariant($"{w.Completed / seconds:F0}"),
                    Invariant($"{w.Enqueued / seconds:F0}"), Invariant($"{w.EnqueueCalls / seconds:F1}"), BatchSize(w.Enqueued, w.EnqueueCalls),
                    Invariant($"{w.EnqueueErrors}"), Invariant($"{w.EnqueueP99Ms:F1}"), Invariant($"{w.CpuFraction * 100:F0}"),
                    Invariant($"{w.MaxGeneratorWait.TotalMilliseconds:F0}"), Invariant($"{w.DroppedTokens}"),
                    Invariant($"{w.Immediate.P99Ms:F1}"), Invariant($"{w.Delayed.P99Ms:F1}"),
                    measured.Contains(w) && SloEvaluator.IsUnderloaded(w, RateOf(i)) ? "yes" : "",
                };
            }));

        var calls = measured.Sum(w => w.EnqueueCalls);
        Section(md, "Enqueue");
        Table(md, ["Metric (measurement window)", "Value"],
        [
            ["EnqueueManyAsync calls/s", measured.Count == 0 ? NoData : Invariant($"{calls / (measured.Count * 10.0):F1}")],
            ["Mean batch size", BatchSize(measured.Sum(w => w.Enqueued), calls)],
            ["EnqueueManyAsync errors", Invariant($"{measured.Sum(w => w.EnqueueErrors)}")],
            ["Seq values from failed calls", d.Correctness is { } c ? Invariant($"{c.ExcludedFailedCalls}") : NoData],
            ["Underloaded windows", Invariant($"{d.Windows.Where((w, i) => measured.Contains(w) && SloEvaluator.IsUnderloaded(w, RateOf(i))).Count()} of {measured.Count}")],
        ]);

        Section(md, "Harness CPU usage");
        Table(md, ["Metric (measurement window)", "Value"],
        [
            ["Mean, % of cores", Stat(measured.Average(w => (double?)w.CpuFraction * 100), "F0")],
            ["Maximum, % of cores", Stat(measured.Max(w => (double?)w.CpuFraction * 100), "F0")],
            ["Windows above 80%", Invariant($"{measured.Count(w => w.CpuFraction > 0.8)} of {measured.Count}")],
        ]);

        Section(md, "pg_stat_statements");
        if (m.Statements.Classes is { } classes)
            Table(md, ["Class", "calls", "mean plan, ms", "mean exec, ms", "total plan, ms", "total exec, ms"],
                classes.OrderBy(c => c.Key, StringComparer.Ordinal).Select(c => new[]
                {
                    c.Key, Invariant($"{c.Value.Calls}"), Ms(c.Value.MeanPlanTime), Ms(c.Value.MeanExecTime),
                    Ms(c.Value.TotalPlanTime), Ms(c.Value.TotalExecTime),
                }));
        else
            md.AppendLine(NoData).AppendLine();

        if (m.Combination?.Statements is { } probed)
        {
            FetchTable(md, [m.Combination]);
            if (m.Combination.Failure is { } failure) { md.AppendLine(failure).AppendLine(); }
            md.AppendLine("The outer dequeue time includes read_jobs. The send count includes ExecuteReaderAsync retries and excludes connection setup. The server executes queue_lock and dequeue_limited in a single batch.").AppendLine();
            Section(md, "Statement locks");
            Table(md, ["Statement", "Plan", "Non-fast-path", "Total", "Measurement type"],
                probed.Where(p => p.Value.Locks is not null).Select(p => StatementCells(p.Key, p.Value)));
        }

        var s = d.Samples;
        Section(md, "Bloat and autovacuum");
        Table(md, ["Metric (whole run)", "Value"],
        [
            ["Maximum queue size, MiB", Stat(s.Max(x => x.QueueTotalBytes / 1_048_576.0), "F1")],
            ["Maximum queue n_dead_tup", Stat(s.Max(x => x.Queue?.DeadTuples), "F0")],
            ["Maximum completions n_dead_tup", Stat(s.Max(x => x.Completions?.DeadTuples), "F0")],
            ["Queue autovacuum runs", Delta(s, x => x.Queue?.AutovacuumCount)],
            ["Completions autovacuum runs", Delta(s, x => x.Completions?.AutovacuumCount)],
            ["Maximum active autovacuum workers", Stat(s.Max(x => x.AutovacuumWorkers), "F0")],
            ["Database size at end of run, MiB", Stat(s.LastOrDefault(x => x.DatabaseBytes is not null)?.DatabaseBytes / 1_048_576.0, "F1")],
        ]);

        Section(md, "Checkpoints");
        Table(md, ["Metric (whole run)", "Value"],
        [
            ["num_timed", Delta(s, x => x.Checkpointer?.NumTimed)],
            ["num_requested", Delta(s, x => x.Checkpointer?.NumRequested)],
            ["write_time, ms", Delta(s, x => (long?)x.Checkpointer?.WriteTimeMs)],
            ["sync_time, ms", Delta(s, x => (long?)x.Checkpointer?.SyncTimeMs)],
            ["buffers_written", Delta(s, x => x.Checkpointer?.BuffersWritten)],
            ["WAL, bytes", Delta(s, x => x.WalBytes)],
        ]);

        Section(md, "LockManager waits");
        Table(md, ["Metric (whole run)", "Value"],
        [
            ["Maximum waiting backends", Stat(s.Max(x => x.LockManagerWaits), "F0")],
            ["Mean waiting backends", Stat(s.Average(x => x.LockManagerWaits), "F2")],
        ]);

        Section(md, "Correctness and PgBolt counters");
        var r = d.Correctness;
        Table(md, ["Metric", "Value"],
        [
            ["Missing executions", r is null ? NoData : Invariant($"{r.MissingExecutions}")],
            ["Duplicate (extra) executions", (r?.DuplicatedExecutions ?? m.DuplicatedExecutions) is { } dup ? Invariant($"{dup}") : NoData],
            ["Seq values excluded due to failed calls", r is null ? NoData : Invariant($"{r.ExcludedFailedCalls}")],
            ["Jobs outside Succeeded", r is null ? NoData : Invariant($"{r.JobsNotSucceeded}")],
            ["Queue rows after drain", r is null ? NoData : Invariant($"{r.QueueRowsAfterDrain}")],
            ["Drained within 2 min", r is null ? NoData : r.DrainedInTime ? "yes" : "no"],
            ["Jobs with incorrect attempt", r is null ? NoData : Invariant($"{r.AttemptMismatches}")],
            ["pgbolt.job.outcome.dropped", Invariant($"{m.Counters.OutcomeDropped}")],
            ["pgbolt.storage.errors", m.Counters.StorageErrors.Count == 0
                ? "0"
                : string.Join(", ", m.Counters.StorageErrors.OrderBy(e => e.Key, StringComparer.Ordinal).Select(e => Invariant($"{e.Key} {e.Value}")))],
            ["pgbolt.job.enqueued (for reference)", Invariant($"{m.Counters.Enqueued}")],
        ]);

        Section(md, "Sampler");
        Table(md, ["Metric (whole run)", "Value"],
        [
            ["Per-second queries, mean ms", Stat(s.Average(x => x.QueryTime?.TotalMilliseconds), "F1")],
            ["Per-second queries, max ms", Stat(s.Max(x => x.QueryTime?.TotalMilliseconds), "F1")],
            ["Queue counts every 10 s, mean ms", Stat(s.Average(x => x.CountsTime?.TotalMilliseconds), "F1")],
            ["Queue counts every 10 s, max ms", Stat(s.Max(x => x.CountsTime?.TotalMilliseconds), "F1")],
            ["Skipped ticks", Invariant($"{m.Sampler.SkippedTicks}")],
            ["Failed queries", Invariant($"{m.Sampler.FailedQueries}")],
            ["Failed attempts", Invariant($"{m.Sampler.FailedAttempts}")],
        ]);

        Section(md, "Warnings");
        md.AppendLine(m.Warnings.Count == 0 ? "none" : string.Join(Environment.NewLine, m.Warnings.Select(w => "- " + w)));
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

    /// <summary>Aggregate value, or <c>null</c> when there were no readings.</summary>
    private static string Stat(double? value, string format) => value?.ToString(format, CultureInfo.InvariantCulture) ?? NoData;

    /// <summary>Difference between the last and first samples containing the value.</summary>
    private static string Delta(IReadOnlyList<Sample> samples, Func<Sample, long?> value)
    {
        var values = samples.Select(value).OfType<long>().ToList();
        return values.Count == 0 ? NoData : Invariant($"{values[^1] - values[0]}");
    }
}
