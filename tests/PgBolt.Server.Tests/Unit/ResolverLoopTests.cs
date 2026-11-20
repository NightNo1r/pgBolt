using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Unit;

public sealed class ResolverLoopTests
{
    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly InProcessWakeupSignal _wakeup = new();
    private readonly InProcessCompletionSignal _completions = new();
    private readonly ListLogger _logs = new();
    private readonly PgBoltProcessingOptions _options = new() { RunScheduler = false };

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private ProcessingMaintenance Create() =>
        new(_store, _wakeup, _options, new FakeMaintenance(), new PgBoltOptions(), new MonoClock(_time), _logs.CreateLogger("test"),
            TestTelemetry.Metrics(), new ClusterQueueStats(new MonoClock(_time), _options), new ClusterTableStats(new MonoClock(_time), _options), recurring: null, completions: _completions)
        {
            NextRandom = () => 0.5,
        };

    [Fact]
    public async Task Iteration_repeats_while_work_remains_and_signals_launched_queues()   // §6.4
    {
        var signals = new ConcurrentQueue<string>();
        _wakeup.Subscribe("a", () => signals.Enqueue("a"));
        var counts = new Queue<CompletionCountResult>(
        [
            CompletionCountResult.Empty with { Captured = 500, Walking = 1 },
            CompletionCountResult.Empty with { Captured = 3, Finished = 1, Walking = 1 },
        ]);
        _store.OnCount = (_, _) => Task.FromResult(counts.Count > 0 ? counts.Dequeue() : CompletionCountResult.Empty with { WalkReady = true });
        _store.OnResolve = (_, _, _, _) => Task.FromResult(ResolveResult.Empty with { Captured = 1, Queues = ["a"] });

        Assert.Equal(506, await Create().ResolveOnceAsync(Ct));

        Assert.Equal(3, _store.CallsOf("Count").Count());
        Assert.Equal(["a", "a", "a"], signals);
        Assert.All(_store.CallsOf("Count"), c => Assert.Equal(500, (int)c.Detail!));
        Assert.All(_store.CallsOf("Resolve"), c => Assert.Equal((500, 5000, TimeSpan.FromSeconds(1)), ((int, int, TimeSpan))c.Detail!));
        Assert.All(_store.CallsOf("CancelMembers"),
            c => Assert.Equal((1000, 5000, 100, TimeSpan.FromSeconds(1)), ((int, int, int, TimeSpan))c.Detail!));
    }

    [Theory]
    [InlineData("walk-full")]
    [InlineData("new-signals")]
    [InlineData("budget")]
    [InlineData("cancel-portion")]
    public async Task Each_kind_of_remaining_work_repeats_the_iteration(string kind)   // §6.4: следующая итерация сразу
    {
        var first = true;
        _store.OnCount = (_, _) => Task.FromResult(CompletionCountResult.Empty with { WalkReady = true, CancelReady = true });
        _store.OnResolve = (_, _, _, _) => Task.FromResult(!first ? ResolveResult.Empty : kind switch
        {
            "walk-full" => ResolveResult.Empty with { Captured = 500 },
            "new-signals" => ResolveResult.Empty with { NewSignals = 1 },
            "budget" => ResolveResult.Empty with { BudgetExhausted = true },
            _ => ResolveResult.Empty,
        });
        _store.OnCancelMembers = (_, _, _, _, _) =>
        {
            var result = first && kind == "cancel-portion" ? CancelMembersResult.Empty with { Portions = 1 } : CancelMembersResult.Empty;
            first = false;
            return Task.FromResult(result);
        };

        await Create().ResolveOnceAsync(Ct);

        Assert.Equal(2, _store.CallsOf("Count").Count());
    }

    [Fact]
    public async Task Stages_without_work_are_skipped()   // §6.4: Т1 не передал сигналов, готовых к Т2 и CancelMembers нет
    {
        await Create().ResolveOnceAsync(Ct);

        Assert.Equal(["Count", "Oldest"], ResolverCalls());
    }

    [Theory]
    [InlineData("walking", "Resolve")]
    [InlineData("walk-ready", "Resolve")]
    [InlineData("cancel-ready", "CancelMembers")]
    [InlineData("walk-new-signals", "Resolve,CancelMembers")]
    public async Task Stage_runs_when_count_reports_its_work(string work, string expected)   // §6.4
    {
        var first = true;
        _store.OnCount = (_, _) =>
        {
            var result = !first ? CompletionCountResult.Empty : work switch
            {
                "walking" => CompletionCountResult.Empty with { Walking = 1 },
                "cancel-ready" => CompletionCountResult.Empty with { CancelReady = true },
                _ => CompletionCountResult.Empty with { WalkReady = true },
            };
            first = false;
            return Task.FromResult(result);
        };
        _store.OnResolve = (_, _, _, _) =>
            Task.FromResult(work == "walk-new-signals" ? ResolveResult.Empty with { NewSignals = 1 } : ResolveResult.Empty);

        await Create().ResolveOnceAsync(Ct);

        // walk-new-signals: новые сигналы повторяют итерацию, повтор без работы — только Т1.
        var expectedCalls = new[] { "Count" }.Concat(expected.Split(',')).Append("Oldest");
        Assert.Equal(work == "walk-new-signals" ? [.. expectedCalls, "Count"] : expectedCalls, ResolverCalls());
    }

    [Fact]
    public async Task Completion_signal_wakes_resolver_before_its_interval()   // §6.4: ICompletionSignal
    {
        _options.ResolverInterval = TimeSpan.FromMinutes(1);
        var maintenance = Create();
        maintenance.Start();
        await Eventually.True(() => _store.CallsOf("Count").Count() == 1 && maintenance.IdleWaits >= 5,
            "promoter, sweeper, housekeeping, resolver and stats are waiting");

        _completions.Signal();

        await Eventually.True(() => _store.CallsOf("Count").Count() == 2, "the signal wakes the resolver without advancing time");
        await maintenance.StopAsync();
        Assert.Empty(_store.CallsOf("Pending"));   // сигнал — работа известна: без проверки
    }

    private static readonly string[] ResolverOps = ["Pending", "Count", "Resolve", "CancelMembers", "Oldest"];

    private string[] ResolverCalls() => [.. _store.Calls.Select(c => c.Op).Where(ResolverOps.Contains)];

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Timer_iteration_checks_for_ready_signals_first(bool ready)   // простой: один запрос на интервал
    {
        _options.ResolverInterval = TimeSpan.FromSeconds(30);   // проверка лага (раз в минуту) прошла в первой итерации
        _options.MaintenancePollInterval = TimeSpan.FromSeconds(30);   // все пять циклов просыпаются по одному Advance
        _options.StatsSnapshotInterval = TimeSpan.FromSeconds(30);
        _store.OnPending = _ => Task.FromResult(ready);
        _store.OnCount = (_, _) => Task.FromResult(CompletionCountResult.Empty with { WalkReady = ready, CancelReady = ready });
        var maintenance = Create();
        maintenance.Start();
        await Eventually.True(() => _store.CallsOf("Count").Count() == 1 && maintenance.IdleWaits >= 5, "all loops are waiting");
        var before = ResolverCalls().Length;

        _time.Advance(TimeSpan.FromSeconds(30));

        await Eventually.True(() => maintenance.IdleWaits >= 10, "all loops are waiting again");
        await maintenance.StopAsync();
        Assert.Equal(ready ? ["Pending", "Count", "Resolve", "CancelMembers"] : ["Pending"], ResolverCalls().Skip(before));
    }

    [Fact]
    public async Task Immediate_repeats_after_a_timer_check_do_not_check_again()   // §6.4: следующая итерация сразу
    {
        _store.OnPending = _ => Task.FromResult(true);
        var first = true;
        _store.OnCount = (_, _) =>
        {
            var result = first ? CompletionCountResult.Empty with { Captured = 500 } : CompletionCountResult.Empty;
            first = false;
            return Task.FromResult(result);
        };

        await Create().ResolveOnTimerAsync(Ct);

        Assert.Single(_store.CallsOf("Pending"));
        Assert.Equal(2, _store.CallsOf("Count").Count());
    }

    [Fact]
    public async Task Lag_warning_is_checked_on_idle_timer_iterations()   // §8: отложенные сигналы без готовой работы
    {
        _store.OnOldest = _ => Task.FromResult<TimeSpan?>(TimeSpan.FromSeconds(30));

        await Create().ResolveOnTimerAsync(Ct);

        Assert.Equal(["Pending", "Oldest"], ResolverCalls());
        Assert.Single(_logs.At(LogLevel.Warning), e => e.Message.Contains("oldest workflow completion signal"));
    }

    [Fact]
    public async Task Lag_warning_is_checked_at_most_once_a_minute()   // §8
    {
        _store.OnOldest = _ => Task.FromResult<TimeSpan?>(TimeSpan.FromSeconds(30));   // > 10 × ResolverInterval
        var maintenance = Create();

        await maintenance.ResolveOnceAsync(Ct);
        await maintenance.ResolveOnceAsync(Ct);
        Assert.Single(_store.CallsOf("Oldest"));
        Assert.Single(_logs.At(LogLevel.Warning), e => e.Message.Contains("oldest workflow completion signal"));

        _time.Advance(TimeSpan.FromMinutes(1));
        await maintenance.ResolveOnceAsync(Ct);

        Assert.Equal(2, _store.CallsOf("Oldest").Count());
        Assert.Equal(2, _logs.At(LogLevel.Warning).Count(e => e.Message.Contains("oldest workflow completion signal")));
    }

    [Fact]
    public async Task Young_signals_do_not_warn()
    {
        _store.OnOldest = _ => Task.FromResult<TimeSpan?>(TimeSpan.FromSeconds(5));

        await Create().ResolveOnceAsync(Ct);

        Assert.DoesNotContain(_logs.At(LogLevel.Warning), e => e.Message.Contains("oldest workflow completion signal"));
    }

    [Fact]
    public async Task Missing_batches_are_logged_at_debug()   // §6.4 Т1
    {
        var batch = Guid.NewGuid();
        var first = true;
        _store.OnCount = (_, _) =>
        {
            var result = first ? CompletionCountResult.Empty with { Captured = 1, MissingBatches = [batch] } : CompletionCountResult.Empty;
            first = false;
            return Task.FromResult(result);
        };

        await Create().ResolveOnceAsync(Ct);

        Assert.Contains(_logs.At(LogLevel.Debug), e => e.Message.Contains(batch.ToString()));
    }

    [Fact]
    public async Task Resolver_runs_only_with_maintenance()   // §8: только RunMaintenance = true
    {
        _options.RunMaintenance = false;
        _options.RunScheduler = true;
        var maintenance = Create();

        maintenance.Start();
        await Eventually.True(() => _store.CallsOf("Tick").Any(), "scheduler ticks");
        await maintenance.StopAsync();

        Assert.Empty(_store.CallsOf("Count"));
    }
}
