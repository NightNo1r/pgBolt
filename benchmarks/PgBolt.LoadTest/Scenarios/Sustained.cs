using System.Diagnostics;
using Npgsql;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Env;
using PgBolt.LoadTest.Load;
using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Report;
using PgBolt.LoadTest.Run;
using PgBolt.LoadTest.Slo;

namespace PgBolt.LoadTest.Scenarios;

/// <summary>
/// Runs profile B at a constant rate. Each mode gets a fresh schema, warmup, measurement window, producer shutdown, drain, correctness check,
/// and report. Returns the highest exit code across modes.
/// </summary>
internal sealed class Sustained : IScenario
{
    private const int Partitions = 15;
    private static readonly TimeSpan WindowLength = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan DrainLimit = TimeSpan.FromMinutes(2);

    public Task<int> RunAsync(LoadTestOptions options, CancellationToken ct) =>
        IScenario.RunModesAsync(options, (env, mode) => RunModeAsync(env, options, mode, ct), ct);

    private static async Task<int> RunModeAsync(LoadEnvironment env, LoadTestOptions options, LoadMode mode,
        CancellationToken ct)
    {
        var startedAt = DateTime.Now;
        var schema = await env.CreateSchemaNameAsync();
        using var run = new SteadyRun(env, options, mode, schema, startedAt);
        try
        {
            await run.PrepareAsync(Partitions, IndexSet.Current, ct);
            CorrectnessData? correctness = null;
            var (immediate, delayed, summary) = await run.WarmupAndMeasureAsync(null, async (collector, producer) =>
            {
                await producer.StopAsync();
                var drained = await DrainAsync(collector, run.Windows, run.MeasureEnd, env.DirectConnectionString, schema, ct);
                correctness = await CorrectnessCheck.RunAsync(env.DirectConnectionString, schema, run.Ctx,
                    producer.Issued, run.Counters, ct) with { DrainedInTime = drained };
            }, ct);

            var data = run.Data(immediate, delayed, correctness);
            var verdicts = SloEvaluator.Sustained(data);
            await IScenario.ReportAsync(options.Out, run.Model(data, verdicts, summary, [], aborted: false));
            return ExitCode.For(verdicts, run.Sampler.Exhausted, SloEvaluator.IsCpuBound(data));
        }
        catch (Exception)
        {
            await IScenario.ReportAsync(options.Out,
                run.Model(run.Partial(), [], StatementsSummary.NoData, [], aborted: true));
            throw;
        }
        finally
        {
            await env.DropSchemaAsync(schema);
        }
    }

    /// <summary>
    /// Waits up to two minutes after producer shutdown for <c>queue</c> to empty. Continues collecting full 10-second drain windows from
    /// <paramref name="from"/>, the end of measurement. Returns <c>false</c> on timeout.
    /// </summary>
    private static async Task<bool> DrainAsync(WindowCollector collector, List<WindowStats> windows, TimeSpan from,
        string connectionString, string schema, CancellationToken ct)
    {
        var drained = WaitQueueEmptyAsync(connectionString, schema, RunClock.Now + DrainLimit, ct);
        var start = from;
        do
        {
            await RunClock.DelayUntilAsync(start + WindowLength, ct);
            windows.Add(collector.CloseWindow(windows.Count, start));
            start += WindowLength;
        } while (!drained.IsCompleted);

        return await drained;
    }

    private static async Task<bool> WaitQueueEmptyAsync(string connectionString, string schema, TimeSpan deadline,
        CancellationToken ct)
    {
        await using var conn = await StandConnection.OpenAsync(connectionString, "drain", ct);
        await using var cmd = new NpgsqlCommand($"SELECT NOT EXISTS (SELECT 1 FROM {schema}.queue)", conn);
        while (!(bool)(await cmd.ExecuteScalarAsync(ct))!)
        {
            if (RunClock.Now >= deadline) return false;
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
        }

        return true;
    }
}

/// <summary>
/// Runs profile B at a fixed rate on one schema with warmup and measurement. Collects windows, samples, histograms, and measurement bounds for
/// both complete and partial reports.
/// </summary>
internal sealed class SteadyRun : IDisposable
{
    /// <summary>The sampler lost its connection to the harness database.</summary>
    internal const string SamplerLost = "30 consecutive attempts without a successful query, connection lost";

    internal const string SamplerLostWarning = $"Sampler: {SamplerLost}.";

    private readonly LoadEnvironment _env;
    private readonly LoadTestOptions _options;
    private readonly LoadMode _mode;
    private readonly string _schema;
    private readonly DateTime _startedAt;
    private readonly string _connection;
    private readonly StatementsReader _statements;
    private IReadOnlyList<(string Key, string Value)> _environment = [];

    public SteadyRun(LoadEnvironment env, LoadTestOptions options, LoadMode mode, string schema, DateTime startedAt)
    {
        (_env, _options, _mode, _schema, _startedAt) = (env, options, mode, schema, startedAt);
        var bouncer = mode is LoadMode.Bouncer or LoadMode.BouncerPrepared;
        _connection = bouncer ? env.BouncerConnectionString! : env.DirectConnectionString;
        // Генерация идёт прогрев и окно замера; запас 10 % и секунда потока — потолок накопленных токенов.
        Ctx = new RunContext((long)((options.Rate * (options.Warmup + options.Duration).TotalSeconds + options.Rate) * 1.1), options.Queues)
        {
            Fetches = options.Scenario == Scenario.Partitions ? new FetchRecorder() : null,
        };
        Counters = new MeterCounters();
        _statements = new StatementsReader(env.DirectConnectionString);
        Sampler = new Sampler(env.DirectConnectionString, schema, bouncer ? env.Console : null,
            [.. HostCluster.ApplicationNames, Producer.ApplicationName], () => options.Rate, () => Ctx.Executed);
    }

    public FetchSummary? Fetches { get; private set; }
    public RunContext Ctx { get; }
    public MeterCounters Counters { get; }
    public Sampler Sampler { get; }
    public List<WindowStats> Windows { get; } = [];
    public TimeSpan MeasureStart { get; private set; }
    public TimeSpan MeasureEnd { get; private set; }

    /// <summary>Jobs issued by the producer through the end of measurement and subsequent actions.</summary>
    public long Issued { get; private set; }

    /// <summary>
    /// Prepares a schema with <paramref name="partitions"/> partitions and the chosen index set, applies <c>queue</c> reloptions from
    /// <c>--queue-reloptions</c>, then captures environment settings.
    /// </summary>
    public async Task PrepareAsync(int partitions, IndexSet indexes, CancellationToken ct)
    {
        await SchemaPreparer.PrepareAsync(_env.DirectConnectionString, _schema, partitions, indexes, ct);
        if (_options.QueueReloptions.Count > 0)
        {
            await using var conn = await StandConnection.OpenAsync(_env.DirectConnectionString, "prepare", ct);
            await using var cmd = new NpgsqlCommand(
                $"ALTER TABLE {_schema}.queue SET ({string.Join(", ", _options.QueueReloptions.Select(p => $"{p.Key} = {p.Value}"))})",
                conn);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        _environment = await EnvironmentInfo.CollectAsync(_env.DirectConnectionString, _env.Console, _options, _schema, ct);
    }

    /// <summary>
    /// Starts the run clock from zero, starts hosts, producer, and sampling, then warms up. Measurement starts with reset <c>pg_stat_statements</c>
    /// and overall histograms. <paramref name="measure"/> can close windows by phase instead of using a continuous 10-second grid.
    /// <paramref name="afterMeasure"/> runs after measurement while hosts and sampling are still active.
    /// </summary>
    public async Task<(LatencySummary Immediate, LatencySummary Delayed, StatementsSummary Summary)> WarmupAndMeasureAsync(
        Func<WindowCollector, Task>? measure, Func<WindowCollector, Producer, Task>? afterMeasure, CancellationToken ct)
    {
        RunClock.Reset();
        await using var hosts = await HostCluster.StartAsync(_connection, _schema, _mode, Ctx, ct);
        await using var producer = await Producer.StartAsync(_connection, _schema, _mode, Ctx, _options.Rate, ct);
        using var samplerStop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var sampling = Sampler.RunAsync(samplerStop.Token);
        try
        {
            using var process = Process.GetCurrentProcess();
            var collector = new WindowCollector(() => Ctx.Executed, Ctx.Immediate.CloseWindow,
                Ctx.Delayed.CloseWindow, producer.TakeWindow, () => process.TotalProcessorTime,
                Environment.ProcessorCount);
            var start = RunClock.Now;
            MeasureStart = start + _options.Warmup;
            MeasureEnd = MeasureStart + _options.Duration;
            await collector.CloseWindowsAsync(Windows, start, MeasureStart, ct);

            // Прогрев не входит ни в pg_stat_statements, ни в общие гистограммы окна замера.
            await _statements.ResetAsync();
            Ctx.Immediate.ResetTotal();
            Ctx.Delayed.ResetTotal();
            Ctx.Fetches?.Begin();
            await (measure?.Invoke(collector) ?? collector.CloseWindowsAsync(Windows, MeasureStart, MeasureEnd, ct));
            Fetches = Ctx.Fetches?.End();
            var result = (Ctx.Immediate.Total(), Ctx.Delayed.Total(), await _statements.ReadAsync());
            if (afterMeasure is not null) await afterMeasure(collector, producer);
            return result;
        }
        finally
        {
            Issued = producer.Issued;
            await samplerStop.CancelAsync();
            await sampling;
        }
    }

    public RunData Data(LatencySummary immediate, LatencySummary delayed, CorrectnessData? correctness) =>
        new(_options.Rate, Windows, Sampler.Samples, immediate, delayed, correctness, _mode, null, MeasureStart, MeasureEnd);

    /// <summary>Partial run data with histograms at failure and no correctness check.</summary>
    public RunData Partial() => Data(Ctx.Immediate.Total(), Ctx.Delayed.Total(), null);

    /// <summary>Builds the report model and adds to <paramref name="warnings"/> if the sampler lost its connection.</summary>
    public ReportModel Model(RunData data, IReadOnlyList<SloVerdict> verdicts, StatementsSummary summary,
        IReadOnlyList<string> warnings, bool aborted, CombinationResult? combination = null) =>
        new(_options, _mode, _startedAt, _schema, _environment, verdicts, data, summary,
            new SamplerSummary(Sampler.SkippedTicks, Sampler.FailedAttempts, Sampler.FailedQueries),
            Counters.Snapshot(), Ctx.Immediate.Overflows, Ctx.Delayed.Overflows,
            Sampler.Exhausted ? [.. warnings, SamplerLostWarning] : warnings, aborted, Combination: combination);

    public void Dispose() => Counters.Dispose();
}
