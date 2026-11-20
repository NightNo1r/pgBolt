using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;
using static PgBolt.Postgres.Diagnostics.PgBoltTelemetry;
using I = PgBolt.PgBoltDiagnostics.Instruments;

namespace PgBolt.Server.Tests.Unit;

public sealed record RunProbeArgs(int N);
public sealed record HugeTimeoutArgs(int N);

[JsonSerializable(typeof(RunProbeArgs))]
[JsonSerializable(typeof(HugeTimeoutArgs))]
internal sealed partial class RunExecutionJson : JsonSerializerContext;

/// <summary>Тело задачи задаёт тест.</summary>
public sealed class RunProbe
{
    public Func<CancellationToken, Task> Run { get; set; } = _ => Task.CompletedTask;
    public int Started;
}

public sealed class RunProbeJob(RunProbe probe) : IJob<RunProbeArgs>
{
    public Task ExecuteAsync(RunProbeArgs args, JobContext context, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref probe.Started);
        return probe.Run(cancellationToken);
    }
}

/// <summary>TimeoutSeconds выше предела таймеров: JobRunner пишет предупреждение об урезании до запуска исполнителя.</summary>
public sealed class HugeTimeoutJob : IJob<HugeTimeoutArgs>
{
    public Task ExecuteAsync(HugeTimeoutArgs args, JobContext context, CancellationToken cancellationToken) => Task.CompletedTask;
}

/// <summary>Завершение выполнения в PgBoltServer: исключения не выходят наружу, слот и запись освобождаются, источники метрик снимаются.</summary>
public sealed class RunExecutionTests : IAsyncDisposable
{
    private const string Queue = "q";
    private static readonly JobDescriptor Probe = new("run.probe", Queue, 3, 0, null);
    private static readonly JobDescriptor HugeTimeout = new("run.huge", Queue, 3, 0, null) { TimeoutSeconds = int.MaxValue };

    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly ListLogger _logs = new();
    private readonly RunProbe _probe = new();
    private readonly ServiceCollection _services = new();
    private readonly TaskCompletionSource _dequeueGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _jobGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ConcurrentQueue<Task> _executions = new();
    private readonly List<IDisposable> _collectors = [];
    private ServiceProvider? _provider;
    private PgBoltProcessingService? _service;
    private int _polls;
    private string? _jobType;

    public RunExecutionTests()
    {
        _services.AddSingleton<TimeProvider>(_time);
        _services.AddSingleton<IServerStore>(_store);
        _services.AddSingleton<ILoggerFactory>(_logs);
        _services.AddSingleton(_probe);
        _services.AddPgBolt(o =>
        {
            o.ConnectionString = "Host=unused";
            o.AddJobs(b => b
                .AddExecutor<RunProbeArgs, RunProbeJob>(Probe, RunExecutionJson.Default.RunProbeArgs)
                .AddExecutor<HugeTimeoutArgs, HugeTimeoutJob>(HugeTimeout, RunExecutionJson.Default.HugeTimeoutArgs));
        });
        _services.ConfigurePgBoltProcessing(p => { p.RunMaintenance = false; p.RunScheduler = false; });
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private PgBoltServer Server => Assert.Single(_service!.Servers);

    private int Dequeues => _store.CallsOf("DequeueLimited").Count();

    /// <summary>
    /// Один сервер с одним рабочим: занятый слот оставляет диспетчер ждать пробуждения, поэтому следующий dequeue
    /// доказывает OnSlotFreed. Первый dequeue ждёт <see cref="RunOneAsync"/>, чтобы тест успел подписаться на выполнения;
    /// задачу отдаёт только он.
    /// </summary>
    private async Task StartAsync(Action<PgBoltServerOptions>? configure = null)
    {
        _services.AddPgBoltServer("a", s =>
        {
            s.Workers = 1;
            s.Queue(Queue, q => q.ClusterMaxConcurrency = 5);
            configure?.Invoke(s);
        });
        _store.OnGetActiveQueueSettings = _ => Task.FromResult<IReadOnlyList<ActiveQueueSetting>>([new(Queue, 5, null)]);
        _store.OnDequeueLimited = async (_, _, _) =>
        {
            await _dequeueGate.Task;
            return new LimitedDequeueResult(
                Interlocked.Increment(ref _polls) == 1 && _jobType is { } type ? [Leased.Job(type: type)] : [], false);
        };
        _provider = _services.BuildServiceProvider();
        _service = new PgBoltProcessingService(_provider, _ => Task.CompletedTask);
        await _service.StartAsync(Ct);
        Server.ExecutionStarted = _executions.Enqueue;
    }

    /// <summary>Отдаёт одну задачу типа <paramref name="type"/> первым dequeue и возвращает задачу её выполнения.</summary>
    private async Task<Task> RunOneAsync(string type)
    {
        _jobType = type;
        _dequeueGate.SetResult();
        await Eventually.True(() => !_executions.IsEmpty, "the job is started");
        return Assert.Single(_executions);
    }

    private PgBoltInstrumentation Instrumentation => _provider!.GetRequiredService<PgBoltInstrumentation>();

    private MetricCollector<T> Collect<T>(string instrument) where T : struct
    {
        var collector = new MetricCollector<T>(Instrumentation.Meter, instrument);
        _collectors.Add(collector);
        return collector;
    }

    private static long Dropped(MetricCollector<long> dropped, string reason) => TestTelemetry.Sum(dropped, Attr.DropReason, reason);

    private int Observed<T>(string instrument) where T : struct
    {
        using var collector = new MetricCollector<T>(Instrumentation.Meter, instrument);
        collector.RecordObservableInstruments();
        return collector.GetMeasurementSnapshot().Count;
    }

    /// <summary>Сколько измерений дали observable-instruments сервера и кластера за один сбор.</summary>
    private int ServerSourceMeasurements() =>
        Observed<int>(I.WorkerActive) + Observed<int>(I.WorkerCapacity) + Observed<int>(I.ClusterQueueLimit)
        + Observed<long>(I.ClusterQueueJobs) + Observed<double>(I.ClusterQueueOldestReadyAge)
        + Observed<long>(I.ClusterTableDeadTuples) + Observed<long>(I.ClusterTableSize);

    public async ValueTask DisposeAsync()
    {
        _logs.OnEntry = null;
        _dequeueGate.TrySetResult();
        _jobGate.TrySetResult();
        if (_service is not null)
            await _service.StopAsync(CancellationToken.None);
        foreach (var collector in _collectors)
            collector.Dispose();
        if (_provider is not null)
            await _provider.DisposeAsync();
    }

    [Fact]
    public async Task RunExecution_never_throws_with_throwing_logger()
    {
        await StartAsync();
        var dropped = Collect<long>(I.JobOutcomeDropped);
        _logs.OnEntry = _ => throw new InvalidOperationException("logger provider failed");

        var execution = await RunOneAsync(HugeTimeout.Name);   // RunAsync бросает из предупреждения TimeoutOf

        await execution.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.True(execution.IsCompletedSuccessfully);
        Assert.Equal(0, Server.Slots.InUse(Queue));
        await Eventually.True(() => Dequeues == 2, "the dispatcher is woken by the freed slot");
        Assert.Equal(1, Dropped(dropped, DropReasons.Abandoned));

        var heartbeats = _store.CallsOf("Heartbeat").Count();
        _time.Advance(Server.Options.HeartbeatInterval * 1.1);
        await Eventually.True(() => _store.CallsOf("Heartbeat").Count() > heartbeats, "a heartbeat is sent");
        Assert.All(_store.CallsOf("Heartbeat"), c => Assert.Empty((LeaseRef[])c.Detail!));   // запись снята из таблицы

        Server.BeginStop();
        await Server.WaitExecutionsAsync(Ct);
        await Record.ExceptionAsync(() => _service!.StopAsync(Ct));   // может бросить лог «stopped» после остановки
        heartbeats = _store.CallsOf("Heartbeat").Count();
        _time.Advance(Server.Options.HeartbeatInterval * 2);
        // LeaseKeeper останавливается после sink'а: heartbeat прекратился — остановлены оба.
        Assert.Equal(heartbeats, _store.CallsOf("Heartbeat").Count());
    }

    [Fact]
    public async Task RunExecution_completes_when_slot_release_throws()
    {
        await StartAsync();
        _probe.Run = _ =>
        {
            Server.Slots.Release(Queue, fromReserved: false);   // учёт слотов сломан: Release после задачи бросит
            return Task.CompletedTask;
        };

        var execution = await RunOneAsync(Probe.Name);

        await execution.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.True(execution.IsCompletedSuccessfully);
        var failure = Assert.Single(_logs.Entries, e => e.EventId.Id == 1112);
        Assert.Equal((LogLevel.Error, "SlotReleaseFailed"), (failure.Level, failure.EventId.Name));
        Assert.IsType<InvalidOperationException>(failure.Exception);
        _time.Advance(Server.Options.AckFlushInterval);
        await Eventually.True(() => _store.CallsOf("Ack").Any(), "the outcome is still completed");
    }

    [Fact]
    public async Task Complete_exception_on_stopped_sink_does_not_throw_or_count_abandoned()
    {
        await StartAsync();
        var dropped = Collect<long>(I.JobOutcomeDropped);
        _probe.Run = _ => _jobGate.Task;   // токен игнорируется
        var execution = await RunOneAsync(Probe.Name);
        await Eventually.True(() => Volatile.Read(ref _probe.Started) == 1, "the job runs");
        var threw = false;
        _logs.OnEntry = e =>
        {
            if (!e.Message.Contains("discarded: completion sink is stopped"))
                return;
            threw = true;
            throw new InvalidOperationException("logger provider failed");
        };

        using var expired = new CancellationTokenSource();
        await expired.CancelAsync();
        await _service!.StopAsync(expired.Token);   // ожидание выполнений прервано, sink остановлен, запись осталась Running
        _jobGate.SetResult();                       // исход приходит в остановленный sink: Complete бросает из лога

        await execution.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.True(threw);
        Assert.True(execution.IsCompletedSuccessfully);
        Assert.Equal(0, Server.Slots.InUse(Queue));
        Assert.Equal(0, Dropped(dropped, DropReasons.Abandoned));   // запись уже снята sink'ом: Unregister вернул false
        Assert.Equal(1, Dropped(dropped, DropReasons.Stopped));     // учтено до бросившего лога
    }

    [Fact]
    public async Task Abandoned_execution_counts_dropped()
    {
        await StartAsync(s => s.ShutdownTimeout = TimeSpan.FromSeconds(5));
        var dropped = Collect<long>(I.JobOutcomeDropped);
        _probe.Run = _ => _jobGate.Task;
        await RunOneAsync(Probe.Name);
        await Eventually.True(() => Volatile.Read(ref _probe.Started) == 1, "the job runs");

        var stop = _service!.StopAsync(Ct);   // BeginStop синхронно: срок ShutdownTimeout отсчитан от него
        _time.Advance(TimeSpan.FromSeconds(5));
        await stop;

        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("is abandoned"));
        Assert.Equal(1, Dropped(dropped, DropReasons.Abandoned));
    }

    [Fact]
    public async Task Server_sources_unregistered_on_stop_even_when_cancelled()
    {
        await StartAsync();
        Assert.NotEqual(0, ServerSourceMeasurements());

        using var expired = new CancellationTokenSource();
        await expired.CancelAsync();
        await _service!.StopAsync(expired.Token);

        Assert.Equal(0, ServerSourceMeasurements());
    }

    [Fact]
    public async Task Server_sources_unregistered_when_sink_stop_throws()
    {
        await StartAsync();
        _store.OnAck = async (_, ct) =>
        {
            // Отправка исхода в полёте до прерывания остановкой; _jobGate отпускает её при очистке после провала теста.
            await Task.WhenAny(Task.Delay(Timeout.InfiniteTimeSpan, ct), _jobGate.Task);
            return [];
        };
        var execution = await RunOneAsync(Probe.Name);
        await execution.WaitAsync(TimeSpan.FromSeconds(5), Ct);   // ack в буфере sink'а: остановка начнёт его отправку
        Assert.NotEqual(0, ServerSourceMeasurements());
        _logs.OnEntry = e =>
        {
            if (e.Message.Contains("completion sink stop was cancelled"))
                throw new InvalidOperationException("logger provider failed");
        };

        using var expired = new CancellationTokenSource();
        await expired.CancelAsync();
        var error = await Record.ExceptionAsync(() => _service!.StopAsync(expired.Token));

        Assert.NotNull(error);
        Assert.Equal(0, ServerSourceMeasurements());
    }
}
