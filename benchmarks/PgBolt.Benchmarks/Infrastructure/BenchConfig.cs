using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;

namespace PgBolt.Benchmarks.Infrastructure;

/// <summary>
/// Общий конфиг: один Job для всей сетки (2 процесса × (4 прогрева + 12 замеров по ~250 мс)) — запуск в двух процессах
/// ловит разброс размещения потоков на P/E-ядрах между процессами. MemoryDiagnoser — аллокации на операцию (по всем
/// потокам процесса), ThreadingDiagnoser — конкуренция за lock'и и work items пула на операцию.
/// </summary>
public sealed class BenchConfig : ManualConfig
{
    public BenchConfig()
    {
        // PGBOLT_BENCH_DRY=1 — проверочный прогон (Job.Dry вместо основного): CLI --job добавил бы job, а не заменил.
        AddJob(Environment.GetEnvironmentVariable("PGBOLT_BENCH_DRY") == "1"
            ? Job.Dry.WithId("Dry")
            : Job.Default
                .WithLaunchCount(2)
                .WithWarmupCount(4)
                .WithIterationCount(12)
                .WithIterationTime(Perfolizer.Horology.TimeInterval.FromMilliseconds(250))
                .WithUnrollFactor(1)
                .WithId("Concurrent"));
        AddDiagnoser(MemoryDiagnoser.Default);
        AddDiagnoser(ThreadingDiagnoser.Default);
        AddColumn(StatisticColumn.Median, StatisticColumn.P95, StatisticColumn.Min, StatisticColumn.Max);
        HideColumns(Column.Job, Column.LaunchCount, Column.WarmupCount, Column.IterationCount, Column.IterationTime, Column.UnrollFactor);
        WithOrderer(new BenchmarkDotNet.Order.DefaultOrderer(BenchmarkDotNet.Order.SummaryOrderPolicy.Declared,
            BenchmarkDotNet.Order.MethodOrderPolicy.Declared));
    }
}
