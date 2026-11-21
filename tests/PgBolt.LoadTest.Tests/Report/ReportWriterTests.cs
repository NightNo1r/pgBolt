using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Report;
using PgBolt.LoadTest.Slo;
using PgBolt.LoadTest.Tests.Slo;

namespace PgBolt.LoadTest.Tests.Report;

public sealed class ReportWriterTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pgbolt-report-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    private static ReportModel Model() => new(
        OptionsParser.Parse(["sustained", "--mode", "direct"]).Options!, LoadMode.Direct,
        new DateTime(2026, 10, 17, 12, 30, 0), "load_0123456789ab",
        [("PostgreSQL", "17.6"), ("CPU", "Apple M3, 8 cores")],
        [
            new(1, "Throughput", "≥ 95%", "min 99%", Verdict.Pass, null),
            new(2, "Immediate job start latency", "p99 ≤ 500 ms", "p99 900 ms", Verdict.Fail, "harness-limited"),
            new(4, "Ready backlog per sample", "≤ rate", "max 10", Verdict.Pass, null),
            new(4, "Ready backlog growth over the measurement window", "≤ first + rate", "interval shorter than 2 min", Verdict.NoData, null),
        ],
        SloTestData.Sustained(), StatementsSummary.NoData, new SamplerSummary(1, 2, 3),
        new PgBoltCounters(0, new Dictionary<string, long> { ["ack"] = 2 }, 6_000_000), 0, 0, [], Aborted: false);

    [Fact]
    public async Task Report_writer_creates_output_directory()
    {
        var outDir = Path.Combine(_dir, "nested", "results");

        var path = await ReportWriter.WriteAsync(outDir, Model());

        Assert.Equal(Path.Combine(outDir, "20261017-1230-sustained-direct.md"), path);
        Assert.True(File.Exists(path));
        Assert.True(File.Exists(Path.ChangeExtension(path, ".csv")));
    }

    [Fact]
    public async Task Report_contains_slo_table_and_environment()
    {
        var text = await File.ReadAllTextAsync(await ReportWriter.WriteAsync(_dir, Model()), TestContext.Current.CancellationToken);

        foreach (var section in new[]
                 {
                     "## Run parameters", "## Environment", "## SLO", "## Start latency",
                     "## Throughput per 10 s window", "## Enqueue", "## Harness CPU usage",
                     "## pg_stat_statements", "## Bloat and autovacuum", "## Checkpoints", "## LockManager waits",
                     "## Correctness and PgBolt counters", "## Sampler", "## Warnings",
                 })
            Assert.Contains(section, text);
        Assert.Contains("| PostgreSQL | 17.6 |", text);
        Assert.Contains("| 1 | Throughput | ≥ 95% | min 99% | PASS |  |", text);
        Assert.Contains("| 2 | Immediate job start latency | p99 ≤ 500 ms | p99 900 ms | FAIL | harness-limited |", text);
        Assert.Contains("| 4 | Ready backlog per sample |", text);
        Assert.Contains("| 4 | Ready backlog growth over the measurement window | ≤ first + rate | interval shorter than 2 min | no data |  |", text);
        Assert.DoesNotContain("aborted", text);
    }

    [Fact]
    public async Task Same_name_does_not_overwrite()
    {
        var first = await ReportWriter.WriteAsync(_dir, Model());
        var second = await ReportWriter.WriteAsync(_dir, Model() with { Aborted = true });

        Assert.NotEqual(first, second);
        Assert.EndsWith("-sustained-direct-2.md", second);
        Assert.DoesNotContain("aborted", await File.ReadAllTextAsync(first, TestContext.Current.CancellationToken));
        Assert.Contains("aborted", await File.ReadAllTextAsync(second, TestContext.Current.CancellationToken));
        Assert.True(File.Exists(Path.ChangeExtension(second, ".csv")));
    }

    [Fact]
    public async Task Short_window_rates_use_its_length_and_only_measured_windows_are_underloaded()
    {
        // Неполное окно прогрева 55–60 с, окна замера 60–660 с, окно дренажа 660–670 с; вне замера всё «недогруз».
        var data = SloTestData.Sustained();
        data = data with
        {
            Windows = [SloTestData.Window(55, completed: 2_500, enqueued: 0), .. data.Windows,
                SloTestData.Window(660, completed: 1_000, enqueued: 0)],
        };

        var text = await File.ReadAllTextAsync(await ReportWriter.WriteAsync(_dir, Model() with { Data = data }),
            TestContext.Current.CancellationToken);

        Assert.Contains("| 5 | 55 |  | 500 | 0 |", text);
        Assert.Contains("| 66 | 660 |  | 100 | 0 |", text);
        Assert.DoesNotMatch(@"(?m)^\| (5|66) \|.*\| yes \|$", text);
    }

    [Fact]
    public async Task Ramp_report_checks_underload_against_own_step_rate_and_marks_only_evaluated_windows()
    {
        // Ступень 500/с: окно 0 — переходное, окно 10 оценивается; ступень 1000/с: окно 20 — переходное, окно 30 оценивается.
        // Окно 10 поставило 5000 — для своих 500/с это норма, для 1000/с (rate последней ступени) был бы недогруз.
        var data = SloTestData.Sustained() with
        {
            Rate = 1_000,
            Windows =
            [
                SloTestData.Window(0, completed: 5_000, enqueued: 5_000), SloTestData.Window(10, completed: 5_000, enqueued: 5_000),
                SloTestData.Window(20, completed: 10_000, enqueued: 10_000), SloTestData.Window(30, completed: 10_000, enqueued: 9_000),
            ],
            MeasureStart = TimeSpan.Zero,
            MeasureEnd = TimeSpan.FromSeconds(40),
        };
        var model = Model() with
        {
            Data = data,
            Ramp = new RampResult(500, "enqueue", false),
            WindowRamp = [(500, false), (500, true), (1_000, false), (1_000, true)],
        };

        var text = await File.ReadAllTextAsync(await ReportWriter.WriteAsync(_dir, model), TestContext.Current.CancellationToken);

        Assert.Matches(@"(?m)^\| 0 \| 0 \|  \| ", text);
        Assert.Matches(@"(?m)^\| 1 \| 10 \| yes \| .*\|  \|$", text);
        Assert.Matches(@"(?m)^\| 2 \| 20 \|  \| ", text);
        Assert.Matches(@"(?m)^\| 3 \| 30 \| yes \| .*\| yes \|$", text);
        Assert.Contains("| Underloaded windows | 1 of 2 |", text);
        Assert.Contains("| Warmup | none |", text);
        Assert.Contains("| ramp-step-duration |", text);
    }

    private static CombinationResult Combination(int p, LoadMode mode, bool slo1 = true) =>
        new(p, IndexSet.Extra, mode, slo1 ? Verdict.Pass : Verdict.Fail, Verdict.Pass, false, 120.5,
            new Dictionary<string, StatementResult>
            {
                [StatementClasses.Dequeue] = new(new StatementClassStats(10, 0.25, 1.5, 2.5, 15), new LockCount(9, 25), false, PlanKind.Generic),
                [StatementClasses.Ack] = new(null, new LockCount(0, 6), false, PlanKind.Custom),
            },
            3, 0.5, 7);

    [Fact]
    public async Task Partitions_combination_report_is_named_by_p_and_indexes_and_shows_statement_locks()
    {
        var model = Model() with
        {
            Options = OptionsParser.Parse(["partitions", "--mode", "direct-prepared"]).Options!,
            Mode = LoadMode.DirectPrepared,
            Combination = Combination(60, LoadMode.DirectPrepared),
        };

        var path = await ReportWriter.WriteAsync(_dir, model);
        var text = await File.ReadAllTextAsync(path, TestContext.Current.CancellationToken);

        Assert.Equal(Path.Combine(_dir, "20261017-1230-partitions-direct-prepared-p60-extra.md"), path);
        Assert.Contains("## Statement locks", text);
        Assert.Contains("| dequeue | generic | 9 | 25 | measured |", text);
        Assert.Contains("| ack | custom | 0 | 6 | measured |", text);
    }

    [Fact]
    public async Task Run_without_drain_reports_extra_executions_and_no_drain_checks()
    {
        var model = Model() with { Data = SloTestData.Sustained() with { Correctness = null }, DuplicatedExecutions = 3 };

        var text = await File.ReadAllTextAsync(await ReportWriter.WriteAsync(_dir, model), TestContext.Current.CancellationToken);

        Assert.Contains("| Duplicate (extra) executions | 3 |", text);
        Assert.Contains("| Missing executions | no data |", text);
    }

    [Fact]
    public async Task Database_size_is_taken_from_last_sample_with_value()
    {
        var model = Model() with
        {
            Data = SloTestData.Sustained() with
            {
                Samples =
                [
                    SloTestData.Sample(1) with { DatabaseBytes = 5 * 1_048_576 },
                    SloTestData.Sample(2) with { DatabaseBytes = 12 * 1_048_576 },
                    SloTestData.Sample(3),
                ],
            },
        };

        var text = await File.ReadAllTextAsync(await ReportWriter.WriteAsync(_dir, model), TestContext.Current.CancellationToken);

        Assert.Contains("| Database size at end of run, MiB | 12.0 |", text);
    }

    [Fact]
    public async Task Partitions_summary_lists_combinations_statements_capacity_and_threshold()
    {
        var options = OptionsParser.Parse(["partitions", "--mode", "both", "--rate", "500"]).Options!;
        List<CombinationResult> results =
        [
            Combination(15, LoadMode.Direct), Combination(60, LoadMode.Direct, slo1: false), Combination(15, LoadMode.Bouncer),
            new(200, IndexSet.Extra, LoadMode.Bouncer, Verdict.NoData, Verdict.NoData, false, Failure: "TimeoutException: x"),
        ];

        var path = await ReportWriter.WriteSummaryAsync(_dir, new DateTime(2026, 10, 17, 12, 30, 0), options, results,
            7_808, ["warning"]);
        var text = await File.ReadAllTextAsync(path, TestContext.Current.CancellationToken);

        Assert.Equal(Path.Combine(_dir, "20261017-1230-partitions-summary.md"), path);
        Assert.Contains("| Lock table capacity | 7808 |", text);
        Assert.Contains("| direct | extra | 60 | FAIL | PASS | 120.5 |  | 3 | 0.50 | 7 |  |", text);
        Assert.Contains("| bouncer | extra | 200 | no data | no data | no data |  | no data | no data | no data | TimeoutException: x |", text);
        Assert.Contains("| direct | extra | 60 | dequeue | generic | 9 | 25 | measured | 0.250 | 1.500 |", text);
        Assert.Contains("| direct | extra | 15 | ack | custom | 0 | 6 | measured | no data | no data |", text);
        // Порог P: наименьшее P, с которого режим и набор индексов нарушают SLO 1 или 2.
        Assert.Matches(@"(?m)^\| direct \| extra \| 60 \|\r?$", text);
        Assert.Matches(@"(?m)^\| bouncer \| extra \| none \|\r?$", text);
        Assert.Contains("- warning", text);
    }
}
