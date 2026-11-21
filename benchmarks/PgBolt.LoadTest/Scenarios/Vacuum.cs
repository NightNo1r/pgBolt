using System.Data;
using Npgsql;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Env;
using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Run;
using PgBolt.LoadTest.Slo;

namespace PgBolt.LoadTest.Scenarios;

/// <summary>
/// Autovacuum <c>queue</c> под нагрузкой B: на каждый режим новая схема с reloptions из <c>--queue-reloptions</c>,
/// прогрев, фаза 1 (стационарная), фаза 2 (удержание xmin открытой транзакцией), фаза 3 (восстановление), без
/// дренажа. Окно замера — фазы 1–3. Код — наибольший по режимам.
/// </summary>
internal sealed class Vacuum : IScenario
{
    private const int Partitions = 15;

    public Task<int> RunAsync(LoadTestOptions options, CancellationToken ct) =>
        IScenario.RunModesAsync(options, (env, mode) => RunModeAsync(env, options, mode, ct), ct);

    private static async Task<int> RunModeAsync(LoadEnvironment env, LoadTestOptions options, LoadMode mode,
        CancellationToken ct)
    {
        var startedAt = DateTime.Now;
        var schema = await env.CreateSchemaNameAsync();
        // Окно замера SteadyRun — фазы 1–3 подряд.
        using var run = new SteadyRun(env, options with { Duration = options.Phase1 + options.HoldXmin + options.Phase3 },
            mode, schema, startedAt);
        PhaseMarks? phases = null;
        try
        {
            await run.PrepareAsync(Partitions, IndexSet.Current, ct);
            LatencySummary? phase1Immediate = null;
            var (immediate, delayed, summary) = await run.WarmupAndMeasureAsync(async collector =>
            {
                var p = phases = new PhaseMarks(run.MeasureStart, run.MeasureStart + options.Phase1,
                    run.MeasureStart + options.Phase1 + options.HoldXmin, run.MeasureEnd);
                await collector.CloseWindowsAsync(run.Windows, p.Phase1Start, p.Phase2Start, ct);
                // Общая гистограмма копится с начала фазы 1: здесь в ней ровно фаза 1.
                phase1Immediate = run.Ctx.Immediate.Total();
                await using (var hold = await HoldXminAsync(env.DirectConnectionString, schema, ct))
                {
                    await collector.CloseWindowsAsync(run.Windows, p.Phase2Start, p.Phase3Start, ct);
                    await EnsureHeldAsync(hold, ct);
                }

                await collector.CloseWindowsAsync(run.Windows, p.Phase3Start, p.End, ct);
            }, null, ct);

            var data = run.Data(immediate, delayed, null) with { Phases = phases };
            var verdicts = SloEvaluator.Vacuum(data);
            await IScenario.ReportAsync(options.Out,
                run.Model(data, verdicts, summary, [], aborted: false) with
                {
                    VacuumOutcome = Outcome(data, verdicts, phase1Immediate),
                    DuplicatedExecutions = run.Ctx.DuplicatedExecutions(run.Issued),
                });
            return ExitCode.For(verdicts, run.Sampler.Exhausted, SloEvaluator.IsCpuBound(data));
        }
        catch (Exception)
        {
            await IScenario.ReportAsync(options.Out,
                run.Model(run.Partial() with { Phases = phases }, [], StatementsSummary.NoData, [], aborted: true));
            throw;
        }
        finally
        {
            await env.DropSchemaAsync(schema);
        }
    }

    /// <summary>
    /// Прямое соединение стенда с открытой транзакцией <c>REPEATABLE READ</c> и выполненным <c>SELECT</c>: снимок
    /// держит xmin, и vacuum не убирает мёртвые строки, пока соединение не закрыто.
    /// </summary>
    internal static async Task<NpgsqlConnection> HoldXminAsync(string connectionString, string schema,
        CancellationToken ct)
    {
        var conn = await StandConnection.OpenAsync(connectionString, "hold-xmin", ct);
        try
        {
            var tx = await conn.BeginTransactionAsync(IsolationLevel.RepeatableRead, ct);
            await using var cmd = new NpgsqlCommand($"SELECT 1 FROM {schema}.queue LIMIT 1", conn, tx);
            await cmd.ExecuteScalarAsync(ct);
            return conn;
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Запрос в транзакции удержания: если сервер оборвал сессию (<c>idle_in_transaction_session_timeout</c>, сбой
    /// соединения), фаза 2 прошла без удержания xmin и её итог ничего не говорит — исключение, прогон прерывается.
    /// </summary>
    internal static async Task EnsureHeldAsync(NpgsqlConnection hold, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT 1", hold);
        await cmd.ExecuteScalarAsync(ct);
    }

    /// <summary>
    /// Входы выбора параметров autovacuum: выполнены ли SLO 7–9, пик
    /// <c>pg_total_relation_size(queue)</c> в фазе 1 и p99 немедленных фазы 1; без срезов или задач фазы 1 — <c>null</c>.
    /// </summary>
    private static ModeOutcome? Outcome(RunData data, IReadOnlyList<SloVerdict> verdicts, LatencySummary? phase1Immediate)
    {
        var p = data.Phases!;
        var peak = SloEvaluator.SamplesIn(data.Samples, p.Phase1Start, p.Phase2Start).Max(s => s.QueueTotalBytes);
        if (peak is null || phase1Immediate is not { Count: > 0 }) return null;
        return new ModeOutcome(SloEvaluator.VacuumStatus(verdicts.Where(v => v.Number is >= 7 and <= 9).Select(v => v.Result)),
            peak.Value, phase1Immediate.P99Ms);
    }
}
