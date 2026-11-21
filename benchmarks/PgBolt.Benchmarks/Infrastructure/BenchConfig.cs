using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;

namespace PgBolt.Benchmarks.Infrastructure;

/// <summary>
/// Shared configuration uses one job for the parameter grid: two processes, each with four warmups and twelve measurements of about 250 ms. Two
/// processes capture variation in thread placement on performance and efficiency cores. MemoryDiagnoser reports allocations per operation across
/// process threads. ThreadingDiagnoser reports lock contention and thread-pool work items per operation.
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
