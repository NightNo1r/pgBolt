using System.Diagnostics;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Env;
using PgBolt.LoadTest.Load;
using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Report;
using PgBolt.LoadTest.Run;
using PgBolt.LoadTest.Slo;

namespace PgBolt.LoadTest.Scenarios;

/// <summary>
/// Поиск максимального устойчивого потока: поток растёт ступенями без прогрева и дренажа до первой ступени с
/// нарушением. Порогов нет: код 0, если стенд отработал без сбоев, в том числе при остановке «стенд» (это результат
/// прогона); 2 — сбой стенда.
/// </summary>
internal sealed class Ramp : IScenario
{
    private const int Partitions = 15;
    private static readonly TimeSpan WindowLength = TimeSpan.FromSeconds(10);

    public Task<int> RunAsync(LoadTestOptions options, CancellationToken ct) =>
        IScenario.RunModesAsync(options, (env, mode) => RunModeAsync(env, options, mode, ct), ct);

    private static async Task<int> RunModeAsync(LoadEnvironment env, LoadTestOptions options, LoadMode mode,
        CancellationToken ct)
    {
        var startedAt = DateTime.Now;
        var schema = await env.CreateSchemaNameAsync();
        var bouncer = mode is LoadMode.Bouncer or LoadMode.BouncerPrepared;
        var connection = bouncer ? env.BouncerConnectionString! : env.DirectConnectionString;
        var stepWindows = (int)(options.RampStepDuration / WindowLength);
        // Задач за все ступени плюс секунда потока — потолок накопленных токенов; запас 10 %.
        var ctx = new RunContext((long)((Rates(options).Sum(r => (long)r * stepWindows * WindowLength.TotalSeconds)
            + options.RampMax) * 1.1));
        using var counters = new MeterCounters();
        var statements = new StatementsReader(env.DirectConnectionString);
        var rate = options.RampStart;
        var sampler = new Sampler(env.DirectConnectionString, schema, bouncer ? env.Console : null,
            [.. HostCluster.ApplicationNames, Producer.ApplicationName], () => rate, () => ctx.Executed);
        IReadOnlyList<(string Key, string Value)> environment = [];
        var windows = new List<WindowStats>();
        var steps = new List<RampStep>();
        var windowRamp = new List<(int Rate, bool Evaluated)>();
        TimeSpan measureStart = default, measureEnd = default;
        long issued = 0;

        RunData Data() => new(rate, windows, sampler.Samples, ctx.Immediate.Total(), ctx.Delayed.Total(), null, mode, null,
            measureStart, measureEnd);

        ReportModel Model(RunData data, StatementsSummary summary, bool aborted) =>
            new(options, mode, startedAt, schema, environment, [], data, summary,
                new SamplerSummary(sampler.SkippedTicks, sampler.FailedAttempts, sampler.FailedQueries),
                counters.Snapshot(), ctx.Immediate.Overflows, ctx.Delayed.Overflows,
                sampler.Exhausted ? [SteadyRun.SamplerLostWarning] : [],
                aborted, aborted ? null : RampEvaluator.Evaluate(steps), windowRamp);

        try
        {
            await SchemaPreparer.PrepareAsync(env.DirectConnectionString, schema, Partitions, IndexSet.Current, ct);
            environment = await EnvironmentInfo.CollectAsync(env.DirectConnectionString, env.Console, options, schema, ct);
            RunClock.Reset();

            StatementsSummary summary;
            await using (var hosts = await HostCluster.StartAsync(connection, schema, mode, ctx, ct))
            await using (var producer = await Producer.StartAsync(connection, schema, mode, ctx, rate, ct))
            {
                using var samplerStop = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var sampling = sampler.RunAsync(samplerStop.Token);
                try
                {
                    using var process = Process.GetCurrentProcess();
                    var collector = new WindowCollector(() => ctx.Executed, ctx.Immediate.CloseWindow,
                        ctx.Delayed.CloseWindow, producer.TakeWindow, () => process.TotalProcessorTime,
                        Environment.ProcessorCount);
                    await statements.ResetAsync();
                    measureStart = measureEnd = RunClock.Now;
                    var stepStart = measureStart;
                    foreach (var stepRate in Rates(options))
                    {
                        rate = stepRate;
                        producer.SetRate(stepRate);
                        var evaluated = new List<WindowStats>();
                        for (var i = 0; i < stepWindows; i++)
                        {
                            // Первая треть ступени — переходный процесс: оцениваются окна, начавшиеся не раньше трети.
                            var isEvaluated = i * WindowLength >= options.RampStepDuration / 3;
                            if (isEvaluated && evaluated.Count == 0) ctx.Immediate.BeginSegment();
                            await RunClock.DelayUntilAsync(stepStart + (i + 1) * WindowLength, ct);
                            var window = collector.CloseWindow(windows.Count, stepStart + i * WindowLength);
                            windows.Add(window);
                            windowRamp.Add((stepRate, isEvaluated));
                            measureEnd = window.Start + WindowLength;
                            if (isEvaluated) evaluated.Add(window);
                        }

                        steps.Add(new RampStep(stepRate, evaluated, ctx.Immediate.SegmentSummary()));
                        stepStart += stepWindows * WindowLength;
                        if (RampEvaluator.Evaluate(steps).FirstViolation is not null) break;
                    }

                    summary = await statements.ReadAsync();
                }
                finally
                {
                    issued = producer.Issued;
                    await samplerStop.CancelAsync();
                    await sampling;
                }
            }

            await IScenario.ReportAsync(options.Out,
                Model(Data(), summary, aborted: false) with { DuplicatedExecutions = ctx.DuplicatedExecutions(issued) });
            // Упор в CPU стенда — результат ramp, а не сбой: в код попадает только потеря соединения среза.
            return sampler.Exhausted ? 2 : 0;
        }
        catch (Exception)
        {
            await IScenario.ReportAsync(options.Out, Model(Data(), StatementsSummary.NoData, aborted: true));
            throw;
        }
        finally
        {
            await env.DropSchemaAsync(schema);
        }
    }

    private static IEnumerable<int> Rates(LoadTestOptions options)
    {
        for (var rate = options.RampStart; rate <= options.RampMax; rate += options.RampStep)
            yield return rate;
    }
}
