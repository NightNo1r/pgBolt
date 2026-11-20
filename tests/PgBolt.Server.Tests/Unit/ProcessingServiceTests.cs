using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Unit;

public sealed record StubbornArgs(int N);
public sealed record PoliteArgs(int N);
public sealed record QuickArgs(int N);

[JsonSerializable(typeof(StubbornArgs))]
[JsonSerializable(typeof(PoliteArgs))]
[JsonSerializable(typeof(QuickArgs))]
internal sealed partial class ServerJson : JsonSerializerContext;

public sealed class ServerProbe
{
    public TaskCompletionSource Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public ConcurrentQueue<(string Job, CancellationToken Token)> Started { get; } = new();
}

/// <summary>Игнорирует токен: завершается только по Gate.</summary>
public sealed class StubbornJob(ServerProbe probe) : IJob<StubbornArgs>
{
    public Task ExecuteAsync(StubbornArgs args, JobContext context, CancellationToken cancellationToken)
    {
        probe.Started.Enqueue(("stubborn", cancellationToken));
        return probe.Gate.Task;
    }
}

/// <summary>Останавливается по токену.</summary>
public sealed class PoliteJob(ServerProbe probe) : IJob<PoliteArgs>
{
    public Task ExecuteAsync(PoliteArgs args, JobContext context, CancellationToken cancellationToken)
    {
        probe.Started.Enqueue(("polite", cancellationToken));
        return Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
    }
}

/// <summary>Завершается сразу.</summary>
public sealed class QuickJob(ServerProbe probe) : IJob<QuickArgs>
{
    public Task ExecuteAsync(QuickArgs args, JobContext context, CancellationToken cancellationToken)
    {
        probe.Started.Enqueue(("quick", cancellationToken));
        return Task.CompletedTask;
    }
}

public sealed class FilterTrace
{
    public ConcurrentQueue<string> Calls { get; } = new();
}

public sealed class TraceGlobalFilter(FilterTrace trace) : IJobFilter
{
    public async ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next)
    {
        trace.Calls.Enqueue("global:before");
        await next();
        trace.Calls.Enqueue("global:after");
    }
}

public sealed class TraceServerFilter(FilterTrace trace) : IJobFilter
{
    public async ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next)
    {
        trace.Calls.Enqueue("server:before");
        await next();
        trace.Calls.Enqueue("server:after");
    }
}

public sealed class ProcessingServiceTests : IAsyncDisposable
{
    private static readonly JobDescriptor Stubborn = new("server.stubborn", "slow", 3, 0, null);
    private static readonly JobDescriptor Polite = new("server.polite", "fast", 3, 0, null);
    private static readonly JobDescriptor Quick = new("server.quick", "quick", 3, 0, null);

    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly ListLogger _logs = new();
    private readonly ServerProbe _probe = new();
    private readonly ServiceCollection _services = new();
    private readonly ConcurrentDictionary<string, int> _polls = new();
    private ServiceProvider? _provider;
    private PgBoltProcessingService? _service;

    public ProcessingServiceTests()
    {
        _services.AddSingleton<TimeProvider>(_time);
        _services.AddSingleton<IServerStore>(_store);
        _services.AddSingleton<ILoggerFactory>(_logs);
        _services.AddSingleton(_probe);
        _services.AddPgBolt(o =>
        {
            o.ConnectionString = "Host=unused";
            o.AddJobs(b => b
                .AddExecutor<StubbornArgs, StubbornJob>(Stubborn, ServerJson.Default.StubbornArgs)
                .AddExecutor<PoliteArgs, PoliteJob>(Polite, ServerJson.Default.PoliteArgs)
                .AddExecutor<QuickArgs, QuickJob>(Quick, ServerJson.Default.QuickArgs));
        });
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private PgBoltOptions CoreOptions =>
        (PgBoltOptions)_services.Single(d => d.ServiceType == typeof(PgBoltOptions)).ImplementationInstance!;

    /// <summary>Первый dequeue очереди отдаёт одну задачу типа из map, дальше — пусто.</summary>
    private void OneJobPerQueue(IReadOnlyDictionary<string, string> typeByQueue) =>
        _store.OnDequeue = (queue, _, _) => Task.FromResult<IReadOnlyList<LeasedJob>>(
            _polls.AddOrUpdate(queue, 1, (_, n) => n + 1) == 1 && typeByQueue.TryGetValue(queue, out var type)
                ? [Leased.Job(type: type)]
                : []);

    private async Task<PgBoltProcessingService> StartAsync(Func<CancellationToken, Task>? waitReady = null)
    {
        _provider = _services.BuildServiceProvider();
        _service = new PgBoltProcessingService(_provider, waitReady ?? (_ => Task.CompletedTask));
        await _service.StartAsync(Ct);
        return _service;
    }

    public async ValueTask DisposeAsync()
    {
        _probe.Gate.TrySetResult();
        if (_service is not null)
            await _service.StopAsync(CancellationToken.None);
        if (_provider is not null)
            await _provider.DisposeAsync();
    }

    [Fact]
    public async Task Start_publishes_job_types_once_and_registers_every_server()
    {
        _services.AddPgBoltServer("orders", s => s.Queue("o1").Queue("o2", q => q.Reserved = 2));
        _services.AddPgBoltServer("mail", s =>
        {
            s.Workers = 3;
            s.Queue("m");
        });

        var service = await StartAsync();

        var publish = Assert.Single(_store.CallsOf("PublishJobTypes"));
        var (types, publishedBy) = ((IReadOnlyList<JobTypeRecord>, string))publish.Detail!;
        Assert.Equal($"{Environment.MachineName}/{Environment.ProcessId}", publishedBy);
        Assert.Equal(["server.polite", "server.quick", "server.stubborn"], types.Select(t => t.Name));

        var registrations = _store.CallsOf("RegisterServer").Select(c => (ServerRegistration)c.Detail!).ToArray();
        Assert.Equal(2, registrations.Length);
        var orders = registrations.Single(r => r.Name == "orders");
        Assert.Equal((Environment.MachineName, Environment.ProcessorCount * 2 + 2), (orders.Host, orders.Capacity));
        Assert.Equal(["o1", "o2"], orders.Queues);
        Assert.Equal(service.Servers.Select(s => s.Id).Order(), registrations.Select(r => r.Id).Order());
        Assert.Equal(3, registrations.Single(r => r.Name == "mail").Capacity);
    }

    [Fact]
    public async Task Start_waits_for_initializer_and_fails_with_it()
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            StartAsync(_ => Task.FromException(new InvalidOperationException("migrations failed"))));

        Assert.Empty(_store.Calls);
    }

    [Fact]
    public async Task Servers_start_only_after_all_are_registered_and_failed_registration_starts_none()
    {
        _services.AddPgBoltServer("a", s => s.Queue("qa"));
        _services.AddPgBoltServer("b", s => s.Queue("qb"));
        bool? startedBeforeLastRegistration = null;
        _store.OnRegisterServer = (server, _) =>
        {
            if (server.Name != "b")
                return Task.CompletedTask;
            // PgBoltServer.Start пишет «started» синхронно: при регистрации b ни один сервер ещё не должен работать.
            startedBeforeLastRegistration = _logs.Entries.Any(e => e.Message.Contains(" started: "));
            return Task.FromException(new IOException("db down"));
        };

        await Assert.ThrowsAsync<IOException>(() => StartAsync());

        Assert.False(startedBeforeLastRegistration);
        Assert.Equal(["a", "b"], _store.CallsOf("RegisterServer").Select(c => ((ServerRegistration)c.Detail!).Name));
        Assert.DoesNotContain(_logs.Entries, e => e.Message.Contains(" started: "));
        Assert.Empty(_store.CallsOf("Promote"));   // обслуживание тоже не запускалось
    }

    [Fact]
    public async Task Global_filters_wrap_server_filters()
    {
        var trace = new FilterTrace();
        _services.AddSingleton(trace);
        _services.AddPgBoltJobFilter<TraceGlobalFilter>();
        _services.AddPgBoltServer("a", s => s.Queue("slow").AddFilter<TraceServerFilter>());
        OneJobPerQueue(new Dictionary<string, string> { ["slow"] = "server.stubborn" });
        _probe.Gate.SetResult();                  // задача завершается сразу

        await StartAsync();

        await Eventually.True(() => trace.Calls.Count == 4, "both filters ran around the job");
        Assert.Equal(["global:before", "server:before", "server:after", "global:after"], trace.Calls);
    }

    [Fact]
    public async Task Start_warns_about_job_types_without_executor()
    {
        _services.AddPgBoltServer("a", s => s.Queue("slow"));
        IReadOnlyList<string>? asked = null;
        _store.OnJobTypeNames = (queues, _) =>
        {
            asked = queues;
            return Task.FromResult<IReadOnlyList<string>>(["server.stubborn", "ghost.job"]);
        };

        await StartAsync();

        Assert.Equal(["slow"], asked);
        var warning = Assert.Single(_logs.At(LogLevel.Warning), e => e.Message.Contains("NoExecutor"));
        Assert.Contains("ghost.job", warning.Message);
        Assert.DoesNotContain("server.stubborn", warning.Message);
    }

    [Fact]
    public async Task Stop_cancels_shutdown_tokens_of_all_servers_before_waiting_for_any()
    {
        _services.AddPgBoltServer("a", s => s.Queue("slow"));
        _services.AddPgBoltServer("b", s => s.Queue("slow2"));
        OneJobPerQueue(new Dictionary<string, string> { ["slow"] = "server.stubborn", ["slow2"] = "server.stubborn" });
        var service = await StartAsync();
        await Eventually.True(() => _probe.Started.Count == 2, "a job runs on each server");

        var stop = service.StopAsync(Ct);

        // Обе задачи игнорируют токен и ещё не завершились: последовательная остановка не отменила бы токен второго сервера.
        await Eventually.True(() => _probe.Started.All(s => s.Token.IsCancellationRequested), "both servers cancelled their jobs");
        Assert.False(stop.IsCompleted);

        _probe.Gate.SetResult();
        await stop;
        Assert.Equal(2, _store.CallsOf("RemoveServer").Count());
    }

    [Fact]
    public async Task Job_ignoring_token_is_abandoned_after_shutdown_timeout_without_blocking_others()
    {
        _services.AddPgBoltServer("stubborn", s =>
        {
            s.LeaseDuration = TimeSpan.FromSeconds(3);
            s.HeartbeatInterval = TimeSpan.FromSeconds(1);
            s.ShutdownTimeout = TimeSpan.FromSeconds(5);
            s.Queue("slow");
        });
        _services.AddPgBoltServer("polite", s =>
        {
            s.ShutdownTimeout = TimeSpan.FromSeconds(5);
            s.Queue("fast");
        });
        OneJobPerQueue(new Dictionary<string, string> { ["slow"] = "server.stubborn", ["fast"] = "server.polite" });
        var service = await StartAsync();
        await Eventually.True(() => _probe.Started.Count == 2, "both jobs run");

        var stop = service.StopAsync(Ct);
        await Eventually.True(() => _store.CallsOf("Release").Any(), "polite job is released right away");
        Assert.Equal((object?)true, Assert.Single(_store.CallsOf("Release")).Detail);
        Assert.False(stop.IsCompleted);
        // Момент отмены пишется колбэком на токене выполнения в пуле потоков (после отмены токена задачи):
        // без этого ожидания Advance мог бы опередить запись, и предупреждение «ignored cancellation» не появилось бы.
        var stubborn = service.Servers.Single(s => s.Name == "stubborn");
        await Eventually.True(() => stubborn.CancelledExecutions == 1, "cancellation of the stubborn job is recorded");

        _time.Advance(TimeSpan.FromSeconds(5));   // ShutdownTimeout от начала остановки
        await stop;

        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("is abandoned"));
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("ignored cancellation"));
        Assert.Equal(2, _store.CallsOf("RemoveServer").Count());

        var outcomesBefore = _store.Calls.Count(c => c.Op is "Ack" or "Retry" or "Fail" or "ConfirmCancel" or "Release");
        _probe.Gate.SetResult();                  // брошенная задача завершается позже
        await Eventually.True(() => _logs.Entries.Any(e => e.Message.Contains("discarded")), "late outcome is discarded");
        Assert.Equal(outcomesBefore, _store.Calls.Count(c => c.Op is "Ack" or "Retry" or "Fail" or "ConfirmCancel" or "Release"));
    }

    [Fact]
    public async Task Cancelled_stop_still_stops_heartbeat_sink_and_watchdog_but_keeps_servers_row()
    {
        _services.AddPgBoltServer("a", s =>
        {
            s.LeaseDuration = TimeSpan.FromSeconds(3);
            s.HeartbeatInterval = TimeSpan.FromSeconds(1);
            s.Queue("slow");
        });
        OneJobPerQueue(new Dictionary<string, string> { ["slow"] = "server.stubborn" });
        var service = await StartAsync();
        await Eventually.True(() => _probe.Started.Count == 1, "the job runs");

        using var expired = new CancellationTokenSource();
        await expired.CancelAsync();
        await service.StopAsync(expired.Token);   // токен хоста уже сработал

        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("host stop timeout"));
        Assert.Empty(_store.CallsOf("RemoveServer"));                  // строку servers уберёт уборка части 2
        var server = Assert.Single(service.Servers);
        await Eventually.True(() => server.CancelledExecutions == 1, "cancellation of the stubborn job is recorded");
        var heartbeats = _store.CallsOf("Heartbeat").Count();

        _time.Advance(TimeSpan.FromSeconds(10));

        Assert.Equal(heartbeats, _store.CallsOf("Heartbeat").Count());                          // LeaseKeeper остановлен: lease истекут сами
        Assert.DoesNotContain(_logs.Entries, e => e.Message.Contains("ignored cancellation"));   // watchdog освобождён
    }

    [Fact]
    public async Task Maintenance_runs_only_when_enabled()
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));
        _services.ConfigurePgBoltProcessing(p => { p.RunMaintenance = false; p.RunScheduler = false; });

        await StartAsync();
        await Eventually.True(() => _store.CallsOf("ReadQueueStats").Any(), "stats loop reads the snapshot");

        Assert.Empty(_store.CallsOf("Promote"));
        Assert.Empty(_store.CallsOf("Sweep"));
    }

    [Fact]
    public async Task Maintenance_is_created_for_servers_with_all_flags_off()
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));
        _services.ConfigurePgBoltProcessing(p => { p.RunMaintenance = false; p.RunScheduler = false; });

        var service = await StartAsync();

        Assert.NotNull(service.Maintenance);
        await Eventually.True(() => _store.CallsOf("ReadQueueStats").Any(), "stats loop reads the snapshot");
        Assert.Empty(_store.CallsOf("Claim"));
        Assert.Empty(_store.CallsOf("Promote"));
    }

    [Fact]
    public async Task Scheduler_runs_without_maintenance()
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));
        _services.ConfigurePgBoltProcessing(p => p.RunMaintenance = false);

        var service = await StartAsync();

        Assert.NotNull(service.Maintenance);
        await Eventually.True(() => _store.CallsOf("Tick").Any(), "scheduler ticks");
        Assert.Empty(_store.CallsOf("Promote"));
    }

    [Fact]
    public async Task Maintenance_starts_with_servers()
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));

        var service = await StartAsync();

        Assert.NotNull(service.Maintenance);
        await Eventually.True(() => _store.CallsOf("Promote").Any() && _store.CallsOf("Sweep").Any(), "promoter and sweeper run");
    }

    [Fact]
    public async Task Warns_when_host_shutdown_timeout_is_shorter_than_server_needs()
    {
        _services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(20));
        _services.AddPgBoltServer("a", s => s.Queue("q"));   // 25 с + 20 мс + 2 с > 20 с

        await StartAsync();

        Assert.Single(_logs.At(LogLevel.Warning), e => e.Message.Contains("HostOptions.ShutdownTimeout"));
    }

    [Fact]
    public async Task Shutdown_timeout_warning_uses_max_not_sum_of_servers()
    {
        _services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(28));
        _services.AddPgBoltServer("a", s => s.Queue("q"));                                                   // 25 с
        _services.AddPgBoltServer("b", s => { s.ShutdownTimeout = TimeSpan.FromSeconds(20); s.Queue("r"); });

        await StartAsync();

        Assert.DoesNotContain(_logs.At(LogLevel.Warning), e => e.Message.Contains("HostOptions.ShutdownTimeout"));
    }

    [Fact]
    public async Task Warns_about_short_partition_horizon_and_dead_server_timeout()
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));   // HeartbeatInterval 15 с
        _services.ConfigurePgBoltProcessing(p =>
        {
            p.PartitionsInterval = TimeSpan.FromDays(1);
            p.DeadServerTimeout = TimeSpan.FromSeconds(30);
        });
        CoreOptions.PartitionDaysAhead = 1;

        await StartAsync();

        Assert.Single(_logs.At(LogLevel.Warning), e => e.Message.Contains("PartitionDaysAhead"));
        Assert.Single(_logs.At(LogLevel.Warning), e => e.Message.Contains("DeadServerTimeout"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task No_maintenance_warnings_with_defaults_or_without_maintenance(bool runMaintenance)
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));
        _services.ConfigurePgBoltProcessing(p =>
        {
            p.RunMaintenance = runMaintenance;
            if (!runMaintenance)
                p.DeadServerTimeout = TimeSpan.FromSeconds(1);
        });

        await StartAsync();

        Assert.DoesNotContain(_logs.At(LogLevel.Warning), e => e.Message.Contains("PartitionDaysAhead") || e.Message.Contains("DeadServerTimeout"));
    }

    private ConcurrentQueue<OutcomeStatsRow> CaptureFlushedRows()
    {
        var flushed = new ConcurrentQueue<OutcomeStatsRow>();
        _store.OnFlushStats = (rows, _) =>
        {
            foreach (var row in rows)
                flushed.Enqueue(row);
            return Task.CompletedTask;
        };
        return flushed;
    }

    [Fact]
    public async Task Stop_flushes_outcome_stats_after_servers_stop()
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));
        OneJobPerQueue(new Dictionary<string, string> { ["q"] = "server.quick" });
        var flushed = CaptureFlushedRows();
        var service = await StartAsync();
        await Eventually.True(() => _probe.Started.Count == 1, "the job runs");

        await service.StopAsync(Ct);

        var ops = _store.Calls.Select(c => c.Op).ToList();
        Assert.True(ops.LastIndexOf("FlushStats") > ops.LastIndexOf("RemoveServer"), string.Join(", ", ops));
        var row = Assert.Single(flushed);
        Assert.Equal((service.Servers[0].Id, "q", 1, 0, 0), (row.ServerId, row.Queue, row.Succeeded, row.Failed, row.Retried));
    }

    [Fact]
    public async Task Virtual_servers_write_separate_rows()
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));
        _services.AddPgBoltServer("b", s => s.Queue("r"));
        OneJobPerQueue(new Dictionary<string, string> { ["q"] = "server.quick", ["r"] = "server.quick" });
        var flushed = CaptureFlushedRows();
        var service = await StartAsync();
        await Eventually.True(() => _probe.Started.Count == 2, "a job runs on each server");

        await service.StopAsync(Ct);

        Assert.Equal(service.Servers.Select(s => s.Id).Order(), flushed.Select(r => r.ServerId).Order());
    }

    [Fact]
    public async Task Flusher_runs_without_maintenance()
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));
        _services.ConfigurePgBoltProcessing(p => { p.RunMaintenance = false; p.RunScheduler = false; });

        var service = await StartAsync();

        Assert.NotNull(service.Flusher);
    }

    private int SettingsReads => _store.CallsOf("GetActiveQueueSettings").Count();

    /// <summary>Отрицательная проверка: живой цикл прочитал бы настройки за это время после RefreshNow.</summary>
    private static Task QuietPeriod() => Task.Delay(TimeSpan.FromMilliseconds(200), Ct);

    [Fact]
    public async Task Start_syncs_defaults_then_loads_before_creating_servers()
    {
        _services.AddPgBoltServer("x", s => s.Queue("a", q => { q.ClusterMaxConcurrency = 3; q.Weight = 2; }).Queue("b"));
        _services.AddPgBoltServer("y", s => s.Queue("a", q => { q.ClusterMaxConcurrency = 3; q.Weight = 2; }));

        await StartAsync();

        string[] steps = ["PublishJobTypes", "SyncQueueDefaults", "GetActiveQueueSettings", "RegisterServer"];
        Assert.Equal(["PublishJobTypes", "SyncQueueDefaults", "GetActiveQueueSettings", "RegisterServer", "RegisterServer"],
            _store.Calls.Select(c => c.Op).Where(steps.Contains));
        var (defaults, publishedBy) = ((IReadOnlyList<QueueDefault>, string))Assert.Single(_store.CallsOf("SyncQueueDefaults")).Detail!;
        Assert.Equal([new QueueDefault("a", 3, 2), new QueueDefault("b", null, null)], defaults);
        Assert.Equal($"{Environment.MachineName}/{Environment.ProcessId}", publishedBy);
    }

    [Fact]
    public async Task Conflict_warning_is_logged_per_queue()
    {
        _services.AddPgBoltServer("x", s => s.Queue("a", q => q.ClusterMaxConcurrency = 3).Queue("b", q => q.Weight = 4));
        _store.OnSyncQueueDefaults = (_, _, _) => Task.FromResult<IReadOnlyList<QueueDefaultConflict>>(
            [new QueueDefaultConflict("a", 5, null, "other/1"), new QueueDefaultConflict("b", null, 7, "other/2")]);

        await StartAsync();

        var warnings = _logs.At(LogLevel.Warning).Where(e => e.Message.Contains("other/")).Select(e => e.Message).ToArray();
        Assert.Equal(2, warnings.Length);
        var a = Assert.Single(warnings, m => m.Contains("'a'"));
        Assert.Contains("other/1", a);
        Assert.Contains("ClusterMaxConcurrency 5 -> 3", a);
        var b = Assert.Single(warnings, m => m.Contains("'b'"));
        Assert.Contains("Weight 7 -> 4", b);
    }

    [Fact]
    public async Task Conflicting_defaults_fail_start()
    {
        _services.AddPgBoltServer("x", s => s.Queue("a", q => q.Weight = 2));
        _services.AddPgBoltServer("y", s => s.Queue("a", q => q.Weight = 3));

        await Assert.ThrowsAsync<InvalidOperationException>(() => StartAsync());

        Assert.Empty(_store.CallsOf("SyncQueueDefaults"));
        Assert.Empty(_store.CallsOf("RegisterServer"));
    }

    [Fact]
    public async Task Initial_load_failure_fails_start()
    {
        _services.AddPgBoltServer("x", s => s.Queue("a"));
        _store.OnGetActiveQueueSettings = _ => throw new IOException("db down");

        await Assert.ThrowsAsync<IOException>(() => StartAsync());

        Assert.Empty(_store.CallsOf("RegisterServer"));
    }

    [Fact]
    public async Task Stop_after_failed_sync_completes()
    {
        _services.AddPgBoltServer("x", s => s.Queue("a"));
        _store.OnSyncQueueDefaults = (_, _, _) => throw new IOException("db down");

        await Assert.ThrowsAsync<IOException>(() => StartAsync());

        await _service!.StopAsync(Ct).WaitAsync(TimeSpan.FromSeconds(5), Ct);
    }

    [Fact]
    public async Task Loop_starts_first_and_stops_after_servers()
    {
        _services.AddPgBoltServer("x", s => s.Queue("q"));
        _services.ConfigurePgBoltProcessing(p => { p.RunMaintenance = false; p.RunScheduler = false; });
        // PgBoltServer.Start пишет «started» синхронно: RefreshNow до Start цикла — no-op, поэтому чтение доказывает,
        // что цикл запущен раньше сервера. Первый dequeue диспетчера для этого не годится: он идёт в пуле потоков.
        _logs.OnEntry = e =>
        {
            if (e.Message.Contains(" started: "))
                _service!.QueueSettings!.RefreshNow();
        };
        var service = await StartAsync();
        await Eventually.True(() => SettingsReads == 2, "refresh requested while the server starts is read");

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnRemoveServer = (_, _) => gate.Task;
        var stop = service.StopAsync(Ct);
        try
        {
            await Eventually.True(() => _store.CallsOf("RemoveServer").Any(), "servers are stopping");
            service.QueueSettings!.RefreshNow();
            await Eventually.True(() => SettingsReads == 3, "the loop is alive while servers stop");
        }
        finally
        {
            gate.TrySetResult();   // иначе при провале остановка в DisposeAsync повисла бы на воротах
        }
        await stop;
        service.QueueSettings.RefreshNow();
        await QuietPeriod();

        Assert.Equal(3, SettingsReads);
    }

    [Fact]
    public async Task Loop_stops_when_start_fails_after_cache_start()
    {
        _services.AddPgBoltServer("x", s => s.Queue("q"));
        var aliveBeforeFailure = false;
        _services.AddSingleton<IPgBoltMaintenance>(_ =>
        {
            // Обслуживание создаётся после старта цикла: RefreshNow до Start — no-op, поэтому чтение доказывает,
            // что цикл уже работал к моменту ошибки.
            _service!.QueueSettings!.RefreshNow();
            aliveBeforeFailure = SpinWait.SpinUntil(() => SettingsReads == 2, TimeSpan.FromSeconds(5));
            throw new InvalidOperationException("maintenance unavailable");
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() => StartAsync());
        Assert.True(aliveBeforeFailure, "the loop runs before maintenance is created");
        var reads = SettingsReads;
        _service!.QueueSettings!.RefreshNow();
        await QuietPeriod();

        Assert.Equal(reads, SettingsReads);
    }

    [Fact]
    public async Task Loop_runs_when_maintenance_is_disabled()
    {
        _services.AddPgBoltServer("x", s => s.Queue("q"));
        _services.ConfigurePgBoltProcessing(p => { p.RunMaintenance = false; p.RunScheduler = false; });

        var service = await StartAsync();
        await Eventually.True(() => service.QueueSettings!.Waits >= 1, "first wait is armed");
        _time.Advance(TimeSpan.FromSeconds(11));   // 10 с + 10 %

        await Eventually.True(() => SettingsReads == 2, "the loop refreshes the snapshot");
    }

    [Fact]
    public async Task Process_without_servers_does_not_sync()
    {
        _services.AddSingleton(new MonoClock(_time));
        _services.AddSingleton(new PgBoltProcessingOptions());

        var service = await StartAsync();

        Assert.Null(service.QueueSettings);
        Assert.Empty(_store.CallsOf("SyncQueueDefaults"));
        Assert.Empty(_store.CallsOf("GetActiveQueueSettings"));
    }
}
