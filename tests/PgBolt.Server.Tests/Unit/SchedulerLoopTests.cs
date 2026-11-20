using System.Collections.Concurrent;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;
using I = PgBolt.PgBoltDiagnostics.Instruments;

namespace PgBolt.Server.Tests.Unit;

public sealed class SchedulerLoopTests : IDisposable
{
    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly InProcessWakeupSignal _wakeup = new();
    private readonly ListLogger _logs = new();
    private readonly PgBoltProcessingOptions _options = new();
    private readonly PgBoltInstrumentation _instrumentation = TestTelemetry.Instrumentation();

    public void Dispose() => _instrumentation.Dispose();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private ProcessingMaintenance Create(RecurringDeclarations? recurring = null) =>
        new(_store, _wakeup, _options, new FakeMaintenance(), new PgBoltOptions(), new MonoClock(_time), _logs.CreateLogger("test"),
            TestTelemetry.Metrics(_instrumentation), new ClusterQueueStats(new MonoClock(_time), _options), new ClusterTableStats(new MonoClock(_time), _options), recurring)
        {
            NextRandom = () => 0.5,
        };

    private static RecurringDeclarations Cleared(string owner) =>
        new(new PgBoltOptions { RecurringOwner = owner }.ClearRecurring(), environment: null);

    private static RecurringTickResult Tick(int fired, bool full, params string[] queues) =>
        RecurringTickResult.Empty with { Claimed = fired, Fired = fired, Full = full, Queues = queues };

    [Fact]
    public async Task Tick_repeats_while_full_and_signals_queues()
    {
        var signals = new ConcurrentQueue<string>();
        foreach (var q in new[] { "a", "b" })
            _wakeup.Subscribe(q, () => signals.Enqueue(q));
        var results = new Queue<RecurringTickResult>([Tick(100, true, "a"), Tick(3, false, "b")]);
        _store.OnTick = (_, _, _, _) => Task.FromResult(results.Dequeue());

        Assert.Equal(103, await Create().TickOnceAsync(Ct));

        Assert.Equal(["a", "b"], signals);
        Assert.All(_store.CallsOf("Tick"), c => Assert.Equal((100, 1000, TimeSpan.FromMinutes(1)), ((int, int, TimeSpan))c.Detail!));
    }

    [Fact]
    public async Task Tick_logs_row_errors_and_missed_fires()
    {
        var from = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        _store.OnTick = (_, _, _, _) => Task.FromResult(RecurringTickResult.Empty with
        {
            Claimed = 3,
            Errors = [new RecurringRowError("broken", "TimeZoneNotFoundException: Mars/Olympus")],
            Missed = [new RecurringMissed("stale", from, from.AddHours(5))],
            OverlapSkipped = ["busy"],
        });

        await Create().TickOnceAsync(Ct);

        Assert.Contains(_logs.At(LogLevel.Error), e => e.Message.Contains("broken") && e.Message.Contains("Mars/Olympus"));
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("stale"));
        Assert.Contains(_logs.At(LogLevel.Debug), e => e.Message.Contains("busy"));
    }

    [Fact]
    public void Effective_misfire_threshold_follows_scheduler_interval()
    {
        Assert.Equal(TimeSpan.FromMinutes(1), new PgBoltProcessingOptions().EffectiveMisfireThreshold);
        Assert.Equal(TimeSpan.FromSeconds(75), new PgBoltProcessingOptions { SchedulerInterval = TimeSpan.FromSeconds(15) }.EffectiveMisfireThreshold);
        Assert.Equal(TimeSpan.FromMinutes(3), new PgBoltProcessingOptions { MisfireThreshold = TimeSpan.FromMinutes(3) }.EffectiveMisfireThreshold);
    }

    [Fact]
    public async Task Sync_once_logs_every_write_and_collisions()
    {
        _store.OnSync = (_, _, _) => Task.FromResult(new RecurringSyncResult(
            ["a"], [new RecurringFieldChange("b", ["cron"])], ["c"], ["d"]));

        Assert.Equal(3, await Create(Cleared("svc")).SyncOnceAsync(Ct));

        Assert.Equal("svc", Assert.Single(_store.CallsOf("Sync")).Detail);
        Assert.Equal(3, _logs.At(LogLevel.Information).Count(e => e.Message.Contains("of owner svc")));
        Assert.Contains(_logs.At(LogLevel.Error), e => e.Message.Contains("recurring job d declared by owner svc"));
    }

    [Fact]
    public async Task Without_maintenance_only_scheduler_runs()
    {
        _options.RunMaintenance = false;
        var maintenance = Create();

        maintenance.Start();
        await Eventually.True(() => _store.CallsOf("Tick").Any(), "scheduler ticks");
        await maintenance.StopAsync();

        Assert.Empty(_store.CallsOf("Promote"));
        Assert.Empty(_store.CallsOf("Sweep"));
        Assert.Empty(_store.CallsOf("Claim"));
        Assert.Empty(_store.CallsOf("Sync"));
    }

    [Fact]
    public async Task Declarations_start_sync_loop_even_without_scheduler()
    {
        _options.RunMaintenance = false;
        _options.RunScheduler = false;
        var maintenance = Create(Cleared("svc"));

        maintenance.Start();
        await Eventually.True(() => _store.CallsOf("Sync").Any(), "recurring-sync runs");
        await maintenance.StopAsync();

        Assert.Empty(_store.CallsOf("Tick"));
    }

    private sealed class TestEnvironment(string applicationName) : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = "Test";
        public string ApplicationName { get; set; } = applicationName;
        public string ContentRootPath { get; set; } = "/";
        public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
    }

    [Fact]
    public async Task Default_owner_without_declarations_starts_no_sync_loop()
    {
        _options.RunMaintenance = false;
        var recurring = new RecurringDeclarations(new PgBoltOptions(), new TestEnvironment("orders"));
        Assert.Equal("orders", recurring.Owner);                                 // владелец по умолчанию есть, объявлений нет
        var maintenance = Create(recurring);

        maintenance.Start();
        await Eventually.True(() => maintenance.IdleWaits >= 2, "scheduler and stats pause after their first pass");
        _time.Advance(_options.RecurringSyncInterval * 2);                       // интервал recurring-sync прошёл бы дважды
        await Eventually.True(() => _store.CallsOf("Tick").Count() >= 2, "scheduler ticks again");
        await maintenance.StopAsync();

        Assert.Empty(_store.CallsOf("Sync"));
    }

    [Fact]
    public async Task Scheduler_tick_counts_fired()
    {
        using var fired = new MetricCollector<long>(_instrumentation.Meter, I.RecurringFired);
        _store.OnTick = (_, _, _, _) => Task.FromResult(Tick(3, full: false, "q"));

        await Create().TickOnceAsync(Ct);

        Assert.Equal(3, fired.GetMeasurementSnapshot().Sum(m => m.Value));
    }
}
