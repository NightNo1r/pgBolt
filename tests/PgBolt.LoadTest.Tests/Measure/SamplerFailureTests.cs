using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Tests.Measure;

public sealed class SamplerFailureTests
{
    [Fact]
    public async Task Unreachable_database_costs_one_open_per_attempt()
    {
        var sampler = new Sampler("Host=127.0.0.1;Port=1;Timeout=1", "pgbolt", console: null, [], () => 100, () => 0);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(2.5));

        await sampler.RunAsync(cts.Token);

        Assert.InRange(sampler.FailedAttempts, 1, 3);
        Assert.Equal(sampler.FailedAttempts, sampler.FailedQueries);
        Assert.Empty(sampler.Samples);
    }
}
