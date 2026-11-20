using System.Diagnostics;
using Npgsql;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>Нагрузочный smoke (SLO — часть 7). Запуск: dotnet test tests/PgBolt.Server.Tests --filter "Category=Load".</summary>
[Trait("Category", "Load")]
[Collection(TimingSensitiveCollection.Name)]
public sealed class LoadSmokeTests(PostgresFixture pg) : ServerIntegrationTestBase(pg, ConnectionMode.Direct)
{
    private const int Total = 100_000;
    private const int MaxPoolSize = 20;

    [Fact]
    public async Task Hundred_thousand_short_jobs_keep_throughput_and_stay_within_the_pool()
    {
        var appName = "pgbolt-load-" + Db.Schema;
        var dataSource = Track(NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(Db.ConnectionString)
        {
            ApplicationName = appName,
            MaxPoolSize = MaxPoolSize,
        }.ConnectionString));

        for (var i = 0; i < Total; i += 1000)
            await Client.EnqueueManyAsync(Enumerable.Range(i, 1000).Select(n => new SuccessArgs(n)).ToList(), null, null, Ct);

        var clock = Stopwatch.StartNew();
        await StartHostAsync(h =>
        {
            h.Name = "load";
            h.DataSource = dataSource;
            h.Configure = s =>
            {
                s.Workers = 32;
                s.LeaseDuration = TimeSpan.FromSeconds(60);
                s.HeartbeatInterval = TimeSpan.FromSeconds(15);
                s.ShutdownTimeout = TimeSpan.FromSeconds(5);
                s.AckFlushInterval = TimeSpan.FromMilliseconds(20);
            };
        });

        using var sampling = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var samples = new List<(TimeSpan At, int Completed)>();
        long maxConnections = 0;
        var sampler = Task.Run(async () =>
        {
            while (!sampling.IsCancellationRequested)
            {
                samples.Add((clock.Elapsed, Probe.TotalCompleted));
                maxConnections = Math.Max(maxConnections, await Db.ScalarAsync<long>(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = @a", ("a", appName)));
                try
                {
                    await Task.Delay(100, sampling.Token);
                }
                catch (OperationCanceledException)
                {
                    break; // прогон завершён
                }
            }
        }, Ct);

        await WaitUntilAsync(() => Probe.TotalCompleted >= Total, TimeSpan.FromMinutes(10), $"all {Total} jobs complete");
        var finishedAt = clock.Elapsed;
        await sampling.CancelAsync();
        await sampler;
        samples.Add((finishedAt, Probe.TotalCompleted));

        await WaitUntilAsync(async () => await CountAsync("SELECT count(*) FROM {schema}.jobs WHERE state = 2") == Total,
            TimeSpan.FromSeconds(60), "all acks are flushed");
        Assert.Equal(0L, await CountAsync("SELECT count(*) FROM {schema}.queue"));
        var counts = Probe.RunCounts();
        Assert.Equal(Total, counts.Count);
        Assert.All(counts, c => Assert.Equal(1, c.Value));

        var (window, rates) = ThroughputWindows(samples);
        var output = TestContext.Current.TestOutputHelper;
        output?.WriteLine($"processed {Total} jobs in {finishedAt.TotalSeconds:0.0} s; window {window.TotalSeconds:0.##} s; " +
                          $"jobs/s: {string.Join(", ", rates.Select(r => r.ToString("0")))}; max connections {maxConnections}");

        Assert.True(rates.Count >= 2, $"need at least two full windows, got {rates.Count}");
        Assert.All(rates.Skip(1), r => Assert.True(r >= rates[0] * 0.5,
            $"throughput degraded: {r:0} jobs/s vs first window {rates[0]:0} jobs/s"));
        Assert.InRange(maxConnections, 1, MaxPoolSize);
    }

    /// <summary>Окна от первой завершённой задачи до последней; неполное последнее окно отбрасывается.</summary>
    private static (TimeSpan Window, List<double> Rates) ThroughputWindows(List<(TimeSpan At, int Completed)> samples)
    {
        var start = samples.First(s => s.Completed > 0).At;
        var end = samples.First(s => s.Completed >= Total).At;
        var span = end - start;
        var window = span >= TimeSpan.FromSeconds(40)
            ? TimeSpan.FromSeconds(10)
            : TimeSpan.FromSeconds(Math.Max(1, span.TotalSeconds / 4));
        var rates = new List<double>();
        for (var from = start; from + window <= end; from += window)
            rates.Add((CompletedAt(samples, from + window) - CompletedAt(samples, from)) / window.TotalSeconds);
        return (window, rates);
    }

    private static int CompletedAt(List<(TimeSpan At, int Completed)> samples, TimeSpan at) =>
        samples.Last(s => s.At <= at).Completed;
}
