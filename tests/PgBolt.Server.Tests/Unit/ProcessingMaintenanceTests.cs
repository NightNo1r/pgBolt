using System.Collections.Concurrent;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;
using I = PgBolt.PgBoltDiagnostics.Instruments;

namespace PgBolt.Server.Tests.Unit;

public sealed class ProcessingMaintenanceTests : IDisposable
{
    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly InProcessWakeupSignal _wakeup = new();
    private readonly ListLogger _logs = new();
    // Только promoter / sweeper / housekeeping / resolver / stats: IdleWaits == 5 считает их паузы, тикер (RunScheduler) — в SchedulerLoopTests.
    private readonly PgBoltProcessingOptions _options = new() { RunScheduler = false };
    private readonly PgBoltInstrumentation _instrumentation = TestTelemetry.Instrumentation();

    public void Dispose() => _instrumentation.Dispose();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private ProcessingMaintenance Create() =>
        new(_store, _wakeup, _options, new FakeMaintenance(), new PgBoltOptions(), new MonoClock(_time), _logs.CreateLogger("test"),
            TestTelemetry.Metrics(_instrumentation), new ClusterQueueStats(new MonoClock(_time), _options), new ClusterTableStats(new MonoClock(_time), _options))
        {
            NextRandom = () => 0.5,
        };

    [Fact]
    public async Task Promote_repeats_while_batch_is_full_and_signals_queues()
    {
        var signals = new ConcurrentQueue<string>();
        foreach (var q in new[] { "a", "b", "c" })
            _wakeup.Subscribe(q, () => signals.Enqueue(q));
        var batches = new ConcurrentQueue<int>();
        _store.OnPromote = (batch, _) =>
        {
            batches.Enqueue(batch);
            return Task.FromResult(batches.Count == 1 ? new PromoteResult(batch, ["a", "b"]) : new PromoteResult(3, ["c"]));
        };

        var promoted = await Create().PromoteOnceAsync(Ct);

        Assert.Equal(1003, promoted);
        Assert.Equal([1000, 1000], batches);
        Assert.Equal(["a", "b", "c"], signals);
    }

    [Fact]
    public async Task Sweep_repeats_while_batch_is_full()
    {
        var results = new Queue<int>([1000, 1000, 5]);
        _store.OnSweep = (_, _) => Task.FromResult(results.Dequeue());

        Assert.Equal(2005, await Create().SweepOnceAsync(Ct));
        Assert.Equal(3, _store.CallsOf("Sweep").Count());
    }

    [Fact]
    public async Task Loop_error_is_logged_and_retried_after_interval()
    {
        var calls = 0;
        _store.OnPromote = (_, _) => Interlocked.Increment(ref calls) == 1
            ? throw new IOException("db down")
            : Task.FromResult(new PromoteResult(0, []));
        var maintenance = Create();
        maintenance.Start();

        await Eventually.True(() => maintenance.IdleWaits == 5, "promoter, sweeper, housekeeping, resolver and stats are waiting");
        Assert.Contains(_logs.At(LogLevel.Error), e => e.Message.Contains("promoter"));
        _time.Advance(_options.PromoterInterval);

        await Eventually.True(() => _store.CallsOf("Promote").Count() == 2, "promoter runs again after 1 s");
        Assert.Single(_store.CallsOf("Sweep"));   // sweeper ждёт свои 15 с
        await maintenance.StopAsync();
    }

    [Fact]
    public async Task Stop_ends_both_loops()
    {
        var maintenance = Create();
        maintenance.Start();
        await Eventually.True(() => maintenance.IdleWaits == 5, "promoter, sweeper, housekeeping, resolver and stats are waiting");

        await maintenance.StopAsync();

        _time.Advance(TimeSpan.FromMinutes(1));
        Assert.Single(_store.CallsOf("Promote"));
        Assert.Single(_store.CallsOf("Sweep"));
    }

    [Fact]
    public async Task Loop_iteration_error_counts_storage_error()
    {
        using var errors = new MetricCollector<long>(_instrumentation.Meter, I.StorageErrors);
        _store.OnPromote = (_, _) => throw new IOException("db down");
        var maintenance = Create();
        maintenance.Start();

        await Eventually.True(() => maintenance.IdleWaits == 5, "promoter, sweeper, housekeeping, resolver and stats are waiting");
        await maintenance.StopAsync();

        Assert.Equal(1, TestTelemetry.Sum(errors, PgBoltTelemetry.Attr.StorageOperation, "promoter"));
        Assert.Equal(1, errors.GetMeasurementSnapshot().Sum(m => m.Value));
    }

    [Fact]
    public async Task Non_cancellation_exception_during_stop_is_not_counted()
    {
        using var errors = new MetricCollector<long>(_instrumentation.Meter, I.StorageErrors);
        _store.OnPromote = async (_, ct) =>
        {
            await Task.Delay(Timeout.Infinite, ct).ContinueWith(_ => { }, CancellationToken.None);
            throw new IOException("connection reset");
        };
        var maintenance = Create();
        maintenance.Start();
        await Eventually.True(() => _store.CallsOf("Promote").Any(), "promoter is in flight");

        await maintenance.StopAsync();

        Assert.Empty(errors.GetMeasurementSnapshot());
        Assert.DoesNotContain(_logs.Entries, e => e.EventId.Name == "MaintenanceLoopIterationFailed");
    }
}
