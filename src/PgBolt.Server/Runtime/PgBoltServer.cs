using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Jobs;

namespace PgBolt.Server.Runtime;

/// <summary>
/// A virtual server with slots, dispatcher, runner, sink and lease keeper. Its lifecycle is managed outside the hosted
/// service abstraction.
/// </summary>
internal sealed partial class PgBoltServer
{
    private readonly IServerStore _store;
    private readonly JobRegistry _registry;
    private readonly MonoClock _clock;
    private readonly ILogger _logger;
    private readonly ServerRegistration _registration;
    private readonly ExecutionTable _table = new();
    private readonly SlotAllocator _slots;
    private readonly LeaseKeeper _keeper;
    private readonly CompletionSink _sink;
    private readonly JobRunner _runner;
    private readonly Dispatcher _dispatcher;
    private readonly PgBoltMetrics _metrics;
    private readonly QueueSettingsCache _settings;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly ConcurrentDictionary<Execution, Tracked> _running = new();
    private Task _shutdownCallbacks = Task.CompletedTask;
    private ITimer? _watchdog;
    private long _stopStartedAt;
    private IDisposable? _metricsRegistration;

    public PgBoltServer(ServerDefinition definition, IServerStore store, JobRegistry registry, IServiceScopeFactory scopes,
        IReadOnlyList<Type> globalFilters, IWakeupSignal wakeup, MonoClock clock, ILoggerFactory loggers, OutcomeStats stats, QueueSettingsCache settings,
        PgBoltMetrics metrics)
    {
        Id = Guid.CreateVersion7();
        Name = definition.Name;
        Options = definition.Options;
        _store = store;
        _registry = registry;
        _clock = clock;
        _logger = loggers.CreateLogger("PgBolt.Server");
        _metrics = metrics;
        _settings = settings;
        _registration = new ServerRegistration(Id, Name, Environment.MachineName, [.. Options.Queues.Select(q => q.Name)], Options.Capacity);

        _slots = new SlotAllocator(Options.Workers, Options.Queues, () => settings.Current.Weights);
        Dispatcher? dispatcher = null;   // onRecovered и onLeaseFreed будят диспетчер; keeper и sink стартуют после его создания
        _keeper = new LeaseKeeper(_registration, store, _table, Options, clock, _logger, () => dispatcher?.Wake(), metrics);
        _sink = new CompletionSink(Id, store, _table, Options, clock, _logger, stats, metrics,
            q => dispatcher?.OnLeaseFreed(q), q => dispatcher?.WaitingForLease(q) == true);
        var filters = new FilterPipeline([.. globalFilters, .. Options.FilterTypes]);
        _runner = new JobRunner(registry, scopes, filters, new RetryDecider(Options.RetryPolicy, _logger), Options, clock, loggers,
            metrics, Name);
        _dispatcher = dispatcher = new Dispatcher(Options, _slots, new DequeueFetchStrategy(store, Id, Options.LeaseDuration, () => settings.Current), _keeper, _sink,
            wakeup, clock, _logger, StartExecution, metrics);
    }

    public Guid Id { get; }
    public string Name { get; }
    public PgBoltServerOptions Options { get; }

    /// <summary>
    /// For tests, executions with a recorded cancellation time. These drive warnings about canceled executions that have
    /// not finished.
    /// </summary>
    internal int CancelledExecutions => _running.Values.Count(t => Volatile.Read(ref t.CancelledAt) != 0);

    internal SlotAllocator Slots => _slots;

    /// <summary>For tests, receives the Task of each started execution so its completion outcome can be observed.</summary>
    internal Action<Task>? ExecutionStarted { get; set; }

    /// <summary>
    /// Registers the servers row and issues the NoExecutor warning. <see cref="Start"/> starts the runtime separately.
    /// </summary>
    public async Task RegisterAsync(CancellationToken ct)
    {
        await _store.RegisterServerAsync(_registration, ct);
        await WarnMissingExecutorsAsync(_registration.Queues, ct);
    }

    /// <summary>Starts the LeaseKeeper, sink, dispatcher and watchdog after all servers in the process have registered.</summary>
    public void Start()
    {
        _metricsRegistration = _metrics.RegisterServer(Name, _slots, Options.Capacity, _registration.Queues, () => _settings.Current);
        _keeper.Start();
        _sink.Start();
        _dispatcher.Start();
        _watchdog = _clock.Time.CreateTimer(static s => ((PgBoltServer)s!).WarnIgnoredCancellations(), this,
            Options.HeartbeatInterval, Options.HeartbeatInterval);
        Log.ServerStarted(_logger, Name, Id, string.Join(", ", Options.Queues.Select(q => q.Name)), Options.Capacity);
    }

    /// <summary>
    /// Prevents new dequeues and cancels the shutdown token. Cancellation callbacks run on the thread pool so job code
    /// cannot run synchronously during shutdown and delay other servers.
    /// </summary>
    public void BeginStop()
    {
        if (Interlocked.CompareExchange(ref _stopStartedAt, _clock.Now, 0) != 0)
            return;
        _dispatcher.SignalStop();
        _shutdownCallbacks = _shutdown.CancelAsync();
    }

    /// <summary>
    /// Waits for in-flight dequeue, then for executions until ShutdownTimeout from BeginStop. Abandons executions that
    /// miss the deadline.
    /// </summary>
    public async Task WaitExecutionsAsync(CancellationToken ct)
    {
        try
        {
            await WaitDispatcherAsync(ct);
            await ObserveShutdownCallbacksAsync(ct);
            var deadline = _clock.Add(Volatile.Read(ref _stopStartedAt), Options.ShutdownTimeout);
            var running = _running.Values.Select(t => t.Task).ToArray();
            await Task.WhenAll(running).WaitAsync(_clock.Until(deadline), _clock.Time, ct);
        }
        catch (TimeoutException)
        {
            // Не остановившиеся выполнения бросаются; исход, сданный позже, отбрасывается sink'ом.
            foreach (var execution in _running.Keys)
                if (_keeper.Unregister(execution, DropReasons.Abandoned))
                    Log.JobAbandoned(_logger, Name, execution.Job.Id, Options.ShutdownTimeout);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            Log.ExecutionsWaitCancelled(_logger, Name);
        }
    }

    /// <summary>
    /// Flushes outcomes, releases leases and deletes the servers row. If ct is canceled, the sink interrupts sending
    /// immediately. LeaseKeeper and the watchdog still stop; only RemoveServerAsync is skipped. Always unregisters metric
    /// sources, even if shutdown throws, so a stopped server cannot continue publishing slots and cluster gauges for the
    /// lifetime of the process.
    /// </summary>
    public async Task FlushAndStopAsync(CancellationToken ct)
    {
        try
        {
            await _sink.StopAsync(ct);
            await _keeper.StopAsync();
            _watchdog?.Dispose();
            if (ct.IsCancellationRequested)
            {
                Log.ServerStopCancelled(_logger, Name);
                return;
            }
            try
            {
                await _store.RemoveServerAsync(Id, ct);
            }
            catch (Exception e)
            {
                Log.ServerRowRemoveFailed(_logger, e, Name);
            }
            Log.ServerStopped(_logger, Name, Id);
        }
        finally
        {
            _metricsRegistration?.Dispose();
        }
    }

    /// <summary>
    /// Handles a faulted Completion as a last resort. Dispatcher handles its own iteration errors. Logs the fault and
    /// continues shutdown.
    /// </summary>
    private async Task WaitDispatcherAsync(CancellationToken ct)
    {
        try
        {
            await _dispatcher.Completion.WaitAsync(ct);
        }
        catch (Exception e) when (e is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            Log.DispatcherFailed(_logger, e, Name);
        }
    }

    private async Task ObserveShutdownCallbacksAsync(CancellationToken ct)
    {
        try
        {
            await _shutdownCallbacks.WaitAsync(ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            Log.CancellationCallbackFailed(_logger, e, Name);
        }
    }

    private async Task WarnMissingExecutorsAsync(string[] queues, CancellationToken ct)
    {
        try
        {
            var missing = (await _store.GetJobTypeNamesByQueuesAsync(queues, ct))
                .Where(name => !_registry.TryGet(name, out _))
                .ToArray();
            if (missing.Length > 0)
                Log.JobTypesWithoutExecutor(_logger, Name, string.Join(", ", missing));
        }
        catch (Exception e) when (!ct.IsCancellationRequested)
        {
            Log.MissingExecutorsCheckFailed(_logger, e, Name);
        }
    }

    private void StartExecution(Execution execution)
    {
        var tracked = new Tracked();
        tracked.Registration = execution.Token.Register(static s =>
        {
            var (t, clock) = ((Tracked, MonoClock))s!;
            Interlocked.CompareExchange(ref t.CancelledAt, clock.Now, 0);
        }, (tracked, _clock));
        var run = new Task<Task>(() => RunExecutionAsync(execution, tracked));
        tracked.Task = run.Unwrap();
        ExecutionStarted?.Invoke(tracked.Task);   // до записи в _running: бросивший хук не оставит там выполнение, которое не запустится
        _running[execution] = tracked;
        run.Start(TaskScheduler.Default);   // последним: startExecution бросает только до запуска выполнения
    }

    /// <summary>
    /// Final exception boundary so failed executions cannot break execution waits or process shutdown. On execution or
    /// outcome submission failure, removes the entry before logging. This stops lease renewal and lets the sweeper count
    /// the attempt after lease expiry. A throwing log provider cannot leave the entry Running. Returns the slot exactly
    /// once on success or failure. On success it returns the slot before submitting the outcome so the dispatcher can use
    /// it immediately.
    /// </summary>
    private async Task RunExecutionAsync(Execution execution, Tracked tracked)
    {
        try
        {
            JobOutcome outcome;
            try
            {
                outcome = await _runner.RunAsync(execution, _shutdown.Token);
            }
            catch (Exception e)
            {
                AbandonFailed(execution, e);
                return;
            }
            finally
            {
                ReleaseSlot(execution);
            }

            try
            {
                _sink.Complete(execution, outcome);
            }
            catch (Exception e)
            {
                AbandonFailed(execution, e);
            }
        }
        finally
        {
            await tracked.Registration.DisposeAsync();
            _running.TryRemove(execution, out _);
        }
    }

    /// <summary>
    /// On execution or outcome submission failure, removes the entry before logging. Unregister returns false if it is no
    /// longer Running because the sink stopped or the lease was lost. Such executions do not count as abandoned.
    /// </summary>
    private void AbandonFailed(Execution execution, Exception error)
    {
        _keeper.Unregister(execution, DropReasons.Abandoned);
        try
        {
            Log.JobFinishFailed(_logger, error, Name, execution.Job.Id);
        }
        catch
        {
            // Бросающий логгер-провайдер не должен сорвать завершение выполнения.
        }
    }

    private void ReleaseSlot(Execution execution)
    {
        try
        {
            _slots.Release(execution.Queue, execution.FromReserved);
            _dispatcher.OnSlotFreed(execution.Queue);
        }
        catch (Exception e)
        {
            try
            {
                Log.SlotReleaseFailed(_logger, e, Name, execution.Queue, execution.Job.Id);
            }
            catch
            {
                // Как в AbandonFailed: сбой лога глотается.
            }
        }
    }

    /// <summary>Warns once per HeartbeatInterval about canceled executions that have not finished.</summary>
    private void WarnIgnoredCancellations()
    {
        foreach (var (execution, tracked) in _running)
        {
            var cancelledAt = Volatile.Read(ref tracked.CancelledAt);
            if (cancelledAt == 0)
                continue;
            var elapsed = _clock.Elapsed(cancelledAt);
            if (elapsed >= Options.HeartbeatInterval)
                Log.JobIgnoresCancellation(_logger, Name, execution.Job.Id, execution.FirstReason, elapsed);
        }
    }

    private sealed class Tracked
    {
        public Task Task = Task.CompletedTask;
        public CancellationTokenRegistration Registration;
        public long CancelledAt;
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1100, EventName = nameof(ServerStarted), Level = LogLevel.Information,
            Message = "PgBolt server {Server} ({ServerId}) started: queues {Queues}, capacity {Capacity}.")]
        internal static partial void ServerStarted(ILogger logger, string server, Guid serverId, string queues, int capacity);

        [LoggerMessage(EventId = 1101, EventName = nameof(ExecutionsWaitCancelled), Level = LogLevel.Warning,
            Message = "PgBolt server {Server}: waiting for executions was cancelled by the host.")]
        internal static partial void ExecutionsWaitCancelled(ILogger logger, string server);

        [LoggerMessage(EventId = 1102, EventName = nameof(ServerStopCancelled), Level = LogLevel.Warning,
            Message = "PgBolt server {Server}: stop was cancelled by the host; the servers row is left for cleanup.")]
        internal static partial void ServerStopCancelled(ILogger logger, string server);

        [LoggerMessage(EventId = 1103, EventName = nameof(ServerRowRemoveFailed), Level = LogLevel.Error,
            Message = "PgBolt server {Server}: failed to remove the servers row.")]
        internal static partial void ServerRowRemoveFailed(ILogger logger, Exception e, string server);

        [LoggerMessage(EventId = 1104, EventName = nameof(ServerStopped), Level = LogLevel.Information,
            Message = "PgBolt server {Server} ({ServerId}) stopped.")]
        internal static partial void ServerStopped(ILogger logger, string server, Guid serverId);

        [LoggerMessage(EventId = 1105, EventName = nameof(DispatcherFailed), Level = LogLevel.Critical,
            Message = "PgBolt server {Server}: dispatcher failed; stopping continues.")]
        internal static partial void DispatcherFailed(ILogger logger, Exception e, string server);

        [LoggerMessage(EventId = 1106, EventName = nameof(CancellationCallbackFailed), Level = LogLevel.Warning,
            Message = "PgBolt server {Server}: a cancellation callback of a job threw.")]
        internal static partial void CancellationCallbackFailed(ILogger logger, Exception e, string server);

        [LoggerMessage(EventId = 1107, EventName = nameof(JobTypesWithoutExecutor), Level = LogLevel.Warning,
            Message = "PgBolt server {Server}: job types {JobTypes} default to its queues but have no executor in this process; such jobs go to NoExecutor.")]
        internal static partial void JobTypesWithoutExecutor(ILogger logger, string server, string jobTypes);

        [LoggerMessage(EventId = 1108, EventName = nameof(MissingExecutorsCheckFailed), Level = LogLevel.Warning,
            Message = "PgBolt server {Server}: failed to check job types without executors.")]
        internal static partial void MissingExecutorsCheckFailed(ILogger logger, Exception e, string server);

        [LoggerMessage(EventId = 1109, EventName = nameof(JobFinishFailed), Level = LogLevel.Critical,
            Message = "PgBolt server {Server}: unexpected failure while finishing job {JobId}.")]
        internal static partial void JobFinishFailed(ILogger logger, Exception e, string server, Guid jobId);

        [LoggerMessage(EventId = 1110, EventName = nameof(JobAbandoned), Level = LogLevel.Warning,
            Message = "PgBolt server {Server}: job {JobId} did not stop within {ShutdownTimeout} and is abandoned; its lease will expire and the sweeper will count the attempt.")]
        internal static partial void JobAbandoned(ILogger logger, string server, Guid jobId, TimeSpan shutdownTimeout);

        [LoggerMessage(EventId = 1111, EventName = nameof(JobIgnoresCancellation), Level = LogLevel.Warning,
            Message = "PgBolt server {Server}: job {JobId} has ignored cancellation ({Reason}) for {Elapsed}; its slot stays occupied.")]
        internal static partial void JobIgnoresCancellation(ILogger logger, string server, Guid jobId, CancelReason reason, TimeSpan elapsed);

        [LoggerMessage(EventId = 1112, EventName = nameof(SlotReleaseFailed), Level = LogLevel.Error,
            Message = "PgBolt server {Server}: slot of queue {Queue} was not returned after job {JobId} finished.")]
        internal static partial void SlotReleaseFailed(ILogger logger, Exception e, string server, string queue, Guid jobId);
    }
}
