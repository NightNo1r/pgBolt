using Npgsql;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Env;
using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Report;
using PgBolt.LoadTest.Slo;
using PgBolt.Postgres.Storage;

namespace PgBolt.LoadTest.Scenarios;

/// <summary>
/// Матрица «режим × набор индексов × число партиций <c>jobs</c>»: на каждую комбинацию новая схема, блокировки
/// statement'ов горячего пути до нагрузки, прогрев и окно замера нагрузки B без дренажа, отчёт; в конце — сводка.
/// Нарушения SLO и упор в CPU влияют на код возврата только у конфигурации по умолчанию (P = 15, текущие индексы),
/// сбой стенда в любой комбинации — код 2, см. <see cref="PartitionsVerdict"/>.
/// </summary>
internal sealed class Partitions : IScenario
{
    // Таблицы range table statement'а, кроме партиций jobs: родитель jobs и queue, у ack ещё completions.
    private static readonly Dictionary<string, int> OtherRelations = new()
    {
        [StatementClasses.Ack] = 3, [StatementClasses.Enqueue] = 2,
    };

    // У ack и enqueue с id одних суток начальное отсечение оставляет одну партицию.
    private const int SurvivingPartitions = 1;

    public async Task<int> RunAsync(LoadTestOptions options, CancellationToken ct)
    {
        var startedAt = DateTime.Now;
        // Время планирования в pg_stat_statements копится только при track_planning = on.
        await using var env = await LoadEnvironment.StartAsync(options, trackPlanning: true, ct);
        var warnings = await CheckPreparedStatementsAsync(env, options, ct);
        var capacity = await new LockProbe(env.DirectConnectionString).LockTableCapacityAsync(ct);

        var results = new List<CombinationResult>();
        try
        {
            foreach (var mode in options.Modes)
            foreach (var indexes in options.Indexes)
            foreach (var partitions in options.Partitions)
                results.Add(await RunCombinationAsync(env, options, mode, indexes, partitions,
                    mode == LoadMode.BouncerPrepared ? warnings : [], ct));
        }
        finally
        {
            // Сводка по готовым комбинациям нужна и при прерванном прогоне: матрица идёт часами.
            var path = await ReportWriter.WriteSummaryAsync(options.Out, startedAt, options, results, capacity, warnings);
            Console.WriteLine($"Сводка: {path}");
        }

        return PartitionsVerdict.ExitCode(results);
    }

    /// <summary>
    /// Без <c>max_prepared_statements</c> PgBouncer не переносит подготовленные statement'ы между серверными
    /// соединениями и режим <c>bouncer-prepared</c> падает на <c>prepared statement "…" does not exist</c>: 0 — сбой стенда.
    /// У внешнего PgBouncer канала к админ-консоли нет, а нераспознанный вывод <c>SHOW CONFIG</c> значения не даёт —
    /// в обоих случаях только предупреждение.
    /// </summary>
    private static async Task<IReadOnlyList<string>> CheckPreparedStatementsAsync(LoadEnvironment env,
        LoadTestOptions options, CancellationToken ct)
    {
        if (!options.Modes.Contains(LoadMode.BouncerPrepared)) return [];
        if (env.Console is null)
            return ["max_prepared_statements внешнего PgBouncer не проверен: канала к админ-консоли нет."];
        return await env.Console.MaxPreparedStatementsAsync(ct) switch
        {
            0 => throw new InvalidOperationException("У PgBouncer max_prepared_statements = 0: режим bouncer-prepared невозможен."),
            null => ["max_prepared_statements PgBouncer не проверен: в выводе SHOW CONFIG значения нет."],
            _ => [],
        };
    }

    /// <summary>
    /// Одна комбинация на своей схеме. Сбой стенда в ней (исключение, потеря соединения среза, сбой создания или
    /// удаления схемы) не обрывает матрицу: комбинация получает <see cref="CombinationResult.Failure"/>; отмена
    /// прогона пробрасывается.
    /// </summary>
    private static async Task<CombinationResult> RunCombinationAsync(LoadEnvironment env,
        LoadTestOptions options, LoadMode mode, IndexSet indexes, int partitions, IReadOnlyList<string> warnings,
        CancellationToken ct)
    {
        string? schema = null;
        CombinationResult? result = null;
        try
        {
            schema = await env.CreateSchemaNameAsync();
            result = await MeasureAsync(env, options, mode, indexes, partitions, schema, warnings, ct);
        }
        catch (Exception e) when (!ct.IsCancellationRequested)
        {
            Console.Error.WriteLine($"Сбой комбинации {mode}, P = {partitions}, {indexes}: {e}");
            result = new CombinationResult(partitions, indexes, mode, Verdict.NoData, Verdict.NoData, false,
                Failure: FailureText(e));
        }
        finally
        {
            if (schema is not null)
            {
                try
                {
                    await env.DropSchemaAsync(schema);
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine($"Схема {schema} не удалена: {e}");
                    if (result is { Failure: null }) result = result with { Failure = FailureText(e) };
                }
            }
        }

        return result!;
    }

    /// <summary>Текст ошибки для ячейки таблицы сводки: без переводов строк и разделителей колонок.</summary>
    private static string FailureText(Exception e) =>
        $"{e.GetType().Name}: {e.Message}".ReplaceLineEndings(" ").Replace('|', '/');

    /// <summary>Прогон комбинации на подготовленной пустой схеме; при исключении пишет частичный отчёт.</summary>
    private static async Task<CombinationResult> MeasureAsync(LoadEnvironment env, LoadTestOptions options,
        LoadMode mode, IndexSet indexes, int partitions, string schema, IReadOnlyList<string> warnings,
        CancellationToken ct)
    {
        using var run = new SteadyRun(env, options, mode, schema, DateTime.Now);
        var probes = new Dictionary<string, StatementResult>();
        try
        {
            await run.PrepareAsync(partitions, indexes, ct);
            await ProbeLocksAsync(env.DirectConnectionString, schema, mode, options.Queues, partitions, probes, ct);
            var (immediate, delayed, summary) = await run.WarmupAndMeasureAsync(null, null, ct);

            var data = run.Data(immediate, delayed, null);
            var verdicts = SloEvaluator.Partitions(data);
            var measured = SloEvaluator.SamplesIn(data.Samples, data.MeasureStart, data.MeasureEnd);
            var statements = probes.ToDictionary(p => p.Key, p => p.Value with { Stats = summary.Classes?.GetValueOrDefault(p.Key) });
            var missing = summary.MissingStats(statements.Keys);
            var failure = missing.Count > 0 ? $"Нет статистики SQL: {string.Join(", ", missing)}." : null;
            if (run.Sampler.Exhausted)
            {
                failure = $"Срез: {SteadyRun.SamplerLost}. {failure}".TrimEnd();
            }
            var result = new CombinationResult(partitions, indexes, mode,
                verdicts.Single(v => v.Number == 1).Result, verdicts.Single(v => v.Number == 2).Result,
                SloEvaluator.IsCpuBound(data), immediate.Count > 0 ? immediate.P99Ms : null,
                statements,
                measured.Max(s => s.LockManagerWaits), measured.Average(s => s.LockManagerWaits),
                run.Counters.Snapshot().StorageErrorsTotal, failure, run.Fetches);
            await IScenario.ReportAsync(options.Out,
                run.Model(data, verdicts, summary, warnings, aborted: false, result) with
                {
                    DuplicatedExecutions = run.Ctx.DuplicatedExecutions(run.Issued),
                });
            return result;
        }
        catch (Exception e)
        {
            var combination = new CombinationResult(partitions, indexes, mode, Verdict.NoData, Verdict.NoData, false,
                Statements: probes, Failure: FailureText(e));
            await IScenario.ReportAsync(options.Out,
                run.Model(run.Partial(), [], StatementsSummary.NoData, warnings, aborted: true, combination));
            if (ct.IsCancellationRequested) throw;
            Console.Error.WriteLine($"Сбой комбинации {mode}, P = {partitions}, {indexes}: {e}");
            return combination;
        }
    }

    /// <summary>
    /// Блокировки dequeue, ack и enqueue до нагрузки. Без prepare Npgsql шлёт безымянный statement — custom plan,
    /// блокировки измеряются. В режимах <c>*-prepared</c> сначала определяется план: custom — измерение, generic —
    /// нижняя оценка для ack/enqueue. У dequeue учитываются также блокировки внутреннего чтения реальных задач.
    /// </summary>
    private static async Task ProbeLocksAsync(string directConnectionString, string schema, LoadMode mode,
        QueueProfile queues, int partitions, Dictionary<string, StatementResult> probes, CancellationToken ct)
    {
        // Подготовка пробных задач и измерение выполняются на разных соединениях.
        await using var dataSource = NpgsqlDataSource.Create(directConnectionString);
        var store = new PgBoltStore(dataSource, schema, usePreparedStatements: false);
        var probe = new LockProbe(directConnectionString);
        var prepared = mode is LoadMode.DirectPrepared or LoadMode.BouncerPrepared;
        foreach (var (statement, sql, bind) in LockProbe.HotPath(store, queues))
        {
            PlanKind? plan = prepared ? await probe.DetectPlanAsync(sql, bind, ct) : null;
            if (statement is StatementClasses.Dequeue or StatementClasses.DequeueLimited)
            {
                if (sql.Contains(".read_jobs(", StringComparison.Ordinal))
                {
                    probes[StatementClasses.ReadJobs] = new StatementResult(null, null, false, null);
                }
                if (statement == StatementClasses.DequeueLimited)
                {
                    probes[StatementClasses.QueueLock] = new StatementResult(null, null, false, null);
                }
                var locks = await probe.MeasureDequeueAsync(store, sql, bind, plan, ct);
                probes[statement] = new StatementResult(null, locks, false, plan);
            }
            else if (plan == PlanKind.Generic)
            {
                var bound = LockProbe.GenericLowerBound(partitions, OtherRelations[statement], SurvivingPartitions);
                probes[statement] = new StatementResult(null,
                    new LockCount(Math.Max(0, bound - LockProbe.FastPathSlots), bound), true, plan);
            }
            else
            {
                probes[statement] = new StatementResult(null, await probe.MeasureAsync(sql, bind, ct), false, plan);
            }
        }
    }
}
