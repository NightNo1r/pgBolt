using System.Diagnostics;

namespace PgBolt.LoadTest.Tests.Scenarios;

/// <summary>Без Docker и без категории Load: недоступная БД не должна поднимать контейнеры.</summary>
public sealed class StandFailureTests
{
    [Fact]
    public async Task Unreachable_connection_exits_with_code_2()
    {
        var outDir = Path.Combine(Path.GetTempPath(), "pgbolt-failure-" + Guid.NewGuid().ToString("N"));
        var watch = Stopwatch.StartNew();

        var code = await Program.RunAsync(
            ["sustained", "--connection", "Host=127.0.0.1;Port=1;Timeout=3", "--mode", "direct", "--out", outDir],
            TestContext.Current.CancellationToken);

        Assert.Equal(2, code);
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(30), $"код 2 через {watch.Elapsed}");
    }
}
