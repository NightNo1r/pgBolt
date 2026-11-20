using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Schema;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;
using I = PgBolt.PgBoltDiagnostics.Instruments;

namespace PgBolt.Server.Tests.Unit;

public sealed class HousekeepingTests : IDisposable
{
    private static readonly DateTime ClaimedAt = new(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Retry = TimeSpan.FromMinutes(1);

    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly FakeMaintenance _maintenance = new();
    private readonly ListLogger _logs = new();
    // Только promoter / sweeper / housekeeping / resolver / stats: IdleWaits == 5 считает их паузы, тикер (RunScheduler) — в SchedulerLoopTests.
    private readonly PgBoltProcessingOptions _options = new() { RunScheduler = false };
    private readonly PgBoltOptions _core = new();
    private readonly PgBoltInstrumentation _instrumentation = TestTelemetry.Instrumentation();

    public void Dispose() => _instrumentation.Dispose();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private ProcessingMaintenance Create() =>
        new(_store, new InProcessWakeupSignal(), _options, _maintenance, _core, new MonoClock(_time), _logs.CreateLogger("test"),
            TestTelemetry.Metrics(_instrumentation), new ClusterQueueStats(new MonoClock(_time), _options), new ClusterTableStats(new MonoClock(_time), _options))
        {
            NextRandom = () => 0.5,
        };

    private void Claim(params string[] names) =>
        _store.OnClaim = (name, _, _) => Task.FromResult<DateTime?>(names.Contains(name) ? ClaimedAt : null);

    private CompleteCall[] Completes =>
        [.. _store.CallsOf("Complete").Select(c => (CompleteCall)c.Detail!)];

    private static CompleteCall Done(string name, string? error = null, TimeSpan? retry = null) =>
        new(name, ClaimedAt, error, retry);

    [Fact]
    public async Task Claimed_tasks_run_in_order_and_complete_successfully()
    {
        Claim("partitions", "servers", "retention");

        Assert.Equal(3, await Create().HousekeepingOnceAsync(Ct));

        Assert.Equal(["Ensure", "RemoveDeadServers", "Retention"], _maintenance.Ops);
        Assert.Equal(
            new[]
            {
                new ClaimCall("partitions", TimeSpan.FromHours(1)),
                new ClaimCall("servers", TimeSpan.FromMinutes(5)),
                new ClaimCall("retention", TimeSpan.FromHours(1)),
            },
            _store.CallsOf("Claim").Select(c => (ClaimCall)c.Detail!));
        Assert.Equal(new[] { Done("partitions"), Done("servers"), Done("retention") }, Completes);
        Assert.Equal(
            new[]
            {
                new MaintenanceCall("Ensure", null),
                new MaintenanceCall("RemoveDeadServers", TimeSpan.FromMinutes(30)),
                new MaintenanceCall("Retention", null),
            },
            _maintenance.Calls.ToArray());
    }

    [Fact]
    public async Task Unclaimed_tasks_do_not_run()
    {
        Claim("servers");

        Assert.Equal(1, await Create().HousekeepingOnceAsync(Ct));

        Assert.Equal(["RemoveDeadServers"], _maintenance.Ops);
        Assert.Single(Completes);
    }

    [Theory]
    [InlineData(true, LogLevel.Warning)]
    [InlineData(false, LogLevel.Error)]
    public async Task Partitions_lock_timeout_completes_with_accelerated_retry(bool todayPresent, LogLevel level)
    {
        Claim("partitions");
        _maintenance.OnEnsure = _ => Task.FromResult(new PartitionEnsureResult(0, TimedOut: true, TodayPresent: todayPresent));

        await Create().HousekeepingOnceAsync(Ct);

        Assert.Equal(new[] { Done("partitions", "lock_timeout", Retry) }, Completes);
        Assert.Single(_logs.At(level), e => e.Message.Contains("partitions"));
    }

    [Fact]
    public async Task Failed_operation_is_completed_with_error_and_next_task_runs()
    {
        Claim("partitions", "servers", "retention");
        _maintenance.OnEnsure = _ => Task.FromException<PartitionEnsureResult>(new TimeoutException("db down"));
        _maintenance.OnRetention = _ => Task.FromException<RetentionResult>(new InvalidOperationException("boom"));

        Assert.Equal(3, await Create().HousekeepingOnceAsync(Ct));

        Assert.Equal(new[] { Done("partitions", "db down", Retry), Done("servers"), Done("retention", "boom") },
            Completes);   // retention ускоренно не повторяется никогда
        Assert.Equal(2, _logs.At(LogLevel.Error).Count(e => e.Message.Contains("maintenance task")));
    }

    [Fact]
    public async Task Servers_failure_gets_accelerated_retry()
    {
        Claim("servers");
        _maintenance.OnRemoveDeadServers = (_, _) => Task.FromException<int>(new TimeoutException("db down"));

        await Create().HousekeepingOnceAsync(Ct);

        Assert.Equal(new[] { Done("servers", "db down", Retry) }, Completes);
    }

    [Fact]
    public async Task Retention_deferrals_are_not_errors_and_are_reported()
    {
        Claim("retention");
        var d = new DateOnly(2026, 9, 1);
        _maintenance.OnRetention = _ => Task.FromResult(FakeMaintenance.Empty with
        {
            Deferred =
            [
                new DeferredPartition("jobs", d, DeferReason.LockTimeout, 3, null),
                new DeferredPartition("jobs", d.AddDays(1), DeferReason.HoldTimeout, 2, null),
            ],
        });

        await Create().HousekeepingOnceAsync(Ct);

        Assert.Equal(new[] { Done("retention") }, Completes);
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("deferred by lock_timeout"));
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("hit statement_timeout"));
    }

    [Fact]
    public async Task Claim_failure_is_logged_by_the_loop_and_retried_after_poll_interval()
    {
        var claims = 0;
        _store.OnClaim = (name, _, _) => name != "stats" && Interlocked.Increment(ref claims) == 1
            ? Task.FromException<DateTime?>(new TimeoutException("db down"))
            : Task.FromResult<DateTime?>(null);
        var maintenance = Create();
        maintenance.Start();

        await Eventually.True(() => maintenance.IdleWaits == 5, "promoter, sweeper, housekeeping, resolver and stats wait for their intervals");
        Assert.Contains(_logs.At(LogLevel.Error), e => e.Message.Contains("housekeeping"));
        Assert.Equal(1, Volatile.Read(ref claims));   // итерация прервана на первом захвате

        _time.Advance(_options.MaintenancePollInterval);
        await Eventually.True(() => Volatile.Read(ref claims) == 4, "next iteration tries all three tasks");
        await maintenance.StopAsync();
    }

    [Fact]
    public async Task Stop_cancels_running_task_without_complete()
    {
        Claim("retention");
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _maintenance.OnRetention = async ct =>
        {
            started.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            return FakeMaintenance.Empty;
        };
        var maintenance = Create();
        maintenance.Start();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);

        await maintenance.StopAsync().WaitAsync(TimeSpan.FromSeconds(5), Ct);

        Assert.Empty(Completes);
    }

    [Fact]
    public async Task Stop_treats_any_failure_of_the_cancelled_task_as_cancellation()
    {
        Claim("retention");
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _maintenance.OnRetention = async ct =>
        {
            started.SetResult();
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
            catch (OperationCanceledException)
            {
                // как Npgsql, рвущий соединение при отмене: не OperationCanceledException
                throw new InvalidOperationException("connection broken");
            }
            return FakeMaintenance.Empty;
        };
        var maintenance = Create();
        maintenance.Start();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);

        await maintenance.StopAsync().WaitAsync(TimeSpan.FromSeconds(5), Ct);

        Assert.Empty(Completes);
        Assert.Empty(_logs.At(LogLevel.Error));
    }

    [Fact]
    public async Task Operation_cancelled_by_its_own_timeout_is_a_task_failure()
    {
        Claim("partitions", "servers", "retention");
        _maintenance.OnEnsure = _ => Task.FromException<PartitionEnsureResult>(new OperationCanceledException("ensure timed out"));
        _maintenance.OnRetention = _ => Task.FromException<RetentionResult>(new TaskCanceledException("retention timed out"));

        Assert.Equal(3, await Create().HousekeepingOnceAsync(Ct));

        Assert.Equal(
            new[] { Done("partitions", "ensure timed out", Retry), Done("servers"), Done("retention", "retention timed out") },
            Completes);
        Assert.Equal(2, _logs.At(LogLevel.Error).Count(e => e.Message.Contains("maintenance task")));
    }

    [Fact]
    public async Task Maintenance_task_metrics()
    {
        using var runs = new MetricCollector<long>(_instrumentation.Meter, I.MaintenanceRuns);
        using var durations = new MetricCollector<double>(_instrumentation.Meter, I.MaintenanceDuration);
        var maintenance = Create();
        Claim("retention");
        _maintenance.OnRetention = _ =>
        {
            _time.Advance(TimeSpan.FromSeconds(2));
            return Task.FromResult(FakeMaintenance.Empty);
        };
        await maintenance.HousekeepingOnceAsync(Ct);

        _maintenance.OnRetention = _ => throw new IOException("boom");
        await maintenance.HousekeepingOnceAsync(Ct);

        Claim("partitions");
        _maintenance.OnEnsure = _ => Task.FromResult(new PartitionEnsureResult(0, TimedOut: true, TodayPresent: true));
        await maintenance.HousekeepingOnceAsync(Ct);

        Assert.Equal(
            [("retention", "ok"), ("retention", "error"), ("partitions", "deferred")],
            runs.GetMeasurementSnapshot().Select(m =>
                ((string)m.Tags[PgBoltTelemetry.Attr.MaintenanceTask]!, (string)m.Tags[PgBoltTelemetry.Attr.MaintenanceResult]!)));
        Assert.All(runs.GetMeasurementSnapshot(), m => Assert.Equal(1, m.Value));
        var first = durations.GetMeasurementSnapshot()[0];
        Assert.Equal("retention", first.Tags[PgBoltTelemetry.Attr.MaintenanceTask]);
        Assert.Equal(2, first.Value);
        Assert.Equal(3, durations.GetMeasurementSnapshot().Count);
    }
}
