using Microsoft.Extensions.DependencyInjection;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Tests;

public sealed class CompletionSignalTests
{
    [Fact]
    public void Signal_calls_subscribers_until_disposed_and_isolates_failures()
    {
        var signal = new InProcessCompletionSignal();
        var calls = 0;
        using var broken = signal.Subscribe(() => throw new InvalidOperationException("subscriber failure"));
        var subscription = signal.Subscribe(() => calls++);

        signal.Signal();
        subscription.Dispose();
        subscription.Dispose();          // идемпотентно
        signal.Signal();

        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task AddPgBolt_shares_completion_signal_with_store()
    {
        var services = new ServiceCollection();
        services.AddPgBolt(o => o.ConnectionString = "Host=unused");
        await using var sp = services.BuildServiceProvider();

        var signal = sp.GetRequiredService<ICompletionSignal>();

        Assert.IsType<InProcessCompletionSignal>(signal);
        Assert.Same(signal, sp.GetRequiredService<PgBoltStore>().Completions);
    }
}
