using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Jobs;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// The process's single hosted service starts virtual servers and maintenance, then stops all servers concurrently
/// regardless of the host's hosted-service shutdown order.
/// </summary>
internal sealed partial class PgBoltProcessingService : IHostedService
{
    /// <summary>
    /// Allowance above server ShutdownTimeout + AckFlushInterval when validating HostOptions.ShutdownTimeout. Includes
    /// the final outcome counter flush.
    /// </summary>
    private static readonly TimeSpan ShutdownReserve = TimeSpan.FromSeconds(2);

    private readonly IServiceProvider _services;
    private readonly Func<CancellationToken, Task> _waitReady;
    private readonly ILoggerFactory _loggers;
    private readonly ILogger _logger;
    private readonly List<PgBoltServer> _servers = [];
    private readonly OutcomeStats _stats;   // счётчики исходов процесса, общие для всех виртуальных серверов

    public PgBoltProcessingService(IServiceProvider services)
        : this(services, ct => services.GetRequiredService<PgBoltInitializer>().Ready.WaitAsync(ct))
    {
    }

    /// <summary>
    /// For tests, replaces the wait for PgBoltInitializer.Ready, whose TaskCompletionSource cannot be completed
    /// externally.
    /// </summary>
    internal PgBoltProcessingService(IServiceProvider services, Func<CancellationToken, Task> waitReady)
    {
        _services = services;
        _waitReady = waitReady;
        _loggers = services.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
        _logger = _loggers.CreateLogger("PgBolt.Processing");
        _stats = new OutcomeStats(services.GetRequiredService<TimeProvider>());
    }

    internal IReadOnlyList<PgBoltServer> Servers => _servers;

    /// <summary>For tests, null means there are no servers and outcome counter flushing never started.</summary>
    internal OutcomeStatsFlusher? Flusher { get; private set; }

    /// <summary>For tests, null means maintenance never started.</summary>
    internal ProcessingMaintenance? Maintenance { get; private set; }

    /// <summary>For tests, null means there are no servers and queue settings were not synchronized.</summary>
    internal QueueSettingsCache? QueueSettings { get; private set; }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _waitReady(cancellationToken);

        var store = _services.GetRequiredService<IServerStore>();
        var registry = _services.GetRequiredService<JobRegistry>();
        var wakeup = _services.GetRequiredService<IWakeupSignal>();
        var clock = _services.GetRequiredService<MonoClock>();
        var scopes = _services.GetRequiredService<IServiceScopeFactory>();

        var publishedBy = $"{Environment.MachineName}/{Environment.ProcessId}";
        var mismatches = await store.PublishJobTypesAsync(registry.ToJobTypeRecords(), publishedBy, cancellationToken);
        if (mismatches.Count > 0)
            Log.JobTypeSchemaMismatch(_logger, string.Join(", ", mismatches));

        var definitions = _services.GetServices<ServerDefinition>().ToList();
        PgBoltMetrics? metrics = null;
        if (definitions.Count > 0)
        {
            // Без списка лимитированных очередей сервер нарушил бы лимит с первого dequeue, а первый план слотов
            // не увидел бы веса: снимок загружается до создания серверов, ошибка — ошибка старта.
            var defaults = QueueDefaults.Collect(definitions);
            WarnAboutOverwrittenDefaults(defaults, await store.SyncQueueDefaultsAsync(defaults, publishedBy, cancellationToken));
            var settings = _services.GetRequiredService<QueueSettingsCache>();
            await settings.LoadAsync(cancellationToken);
            QueueSettings = settings;

            var globalFilters = _services.GetServices<GlobalJobFilter>().Select(f => f.FilterType).ToArray();
            metrics = _services.GetRequiredService<PgBoltMetrics>();
            foreach (var definition in definitions)
                _servers.Add(new PgBoltServer(definition, store, registry, scopes, globalFilters, wakeup, clock, _loggers, _stats, settings,
                    metrics));
        }

        // Все серверы регистрируются до запуска любого — ошибка регистрации не оставляет работающих серверов.
        foreach (var server in _servers)
            await server.RegisterAsync(cancellationToken);

        var processing = _services.GetRequiredService<PgBoltProcessingOptions>();
        var core = _services.GetRequiredService<PgBoltOptions>();
        WarnIfHostShutdownTooShort();
        WarnIfMaintenanceMisconfigured(processing, core);

        try
        {
            QueueSettings?.Start();   // до серверов: их выборка читает снимок с первого dequeue
            if (metrics is not null)
            {
                // Цикл stats публикует кластерный снимок на каждом поде с серверами, даже без обслуживания и планировщика.
                Maintenance = new ProcessingMaintenance(store, wakeup, processing, _services.GetRequiredService<IPgBoltMaintenance>(),
                    core, clock, _logger, metrics, _services.GetRequiredService<ClusterQueueStats>(),
                    _services.GetRequiredService<ClusterTableStats>(),
                    _services.GetService<RecurringDeclarations>(), _services.GetService<ICompletionSignal>());
                Maintenance.Start();
                Flusher = new OutcomeStatsFlusher(store, _stats, processing.StatsFlushInterval, clock, _logger, metrics);
                Flusher.Start();
            }
            foreach (var server in _servers)
                server.Start();
        }
        catch (Exception e)
        {
            // Хост не вызывает StopAsync после ошибки StartAsync: уже запущенное останавливается здесь.
            Log.ProcessingStartFailed(_logger, e);
            await StopAsync(CancellationToken.None);
            throw;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        var maintenance = Maintenance?.StopAsync() ?? Task.CompletedTask;
        foreach (var server in _servers)
            server.BeginStop();

        await Task.WhenAll(_servers.Select(s => s.WaitExecutionsAsync(cancellationToken)));
        if (cancellationToken.IsCancellationRequested)
            Log.HostStopTimeoutServers(_logger);
        // И при отменённом токене: sink прерывает отправку сразу, LeaseKeeper и watchdog останавливаются (иначе heartbeat
        // продлевал бы lease и после StopAsync, если процесс живёт дальше); RemoveServerAsync пропускается.
        await Task.WhenAll(_servers.Select(s => s.FlushAndStopAsync(cancellationToken)));
        if (QueueSettings is not null)
            await QueueSettings.StopAsync();   // после серверов: их dequeue в полёте ещё читали снимок
        if (Flusher is not null)
            await Flusher.StopAsync(cancellationToken);   // последние исходы учтены sink'ами
        try
        {
            await maintenance.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            Log.HostStopTimeoutMaintenance(_logger);
        }
    }

    /// <summary>
    /// Another process recently published different values for the same queues during a mixed-version rolling deployment.
    /// The last write wins, so only the log shows the discrepancy.
    /// </summary>
    private void WarnAboutOverwrittenDefaults(IReadOnlyList<QueueDefault> defaults, IReadOnlyList<QueueDefaultConflict> conflicts)
    {
        foreach (var conflict in conflicts)
        {
            var current = defaults.Single(d => d.Queue == conflict.Queue);
            List<string> changes = [];
            if (conflict.PreviousMaxConcurrency != current.MaxConcurrency)
                changes.Add($"{nameof(QueueOptions.ClusterMaxConcurrency)} {QueueDefaults.Show(conflict.PreviousMaxConcurrency)} -> {QueueDefaults.Show(current.MaxConcurrency)}");
            if (conflict.PreviousWeight != current.Weight)
                changes.Add($"{nameof(QueueOptions.Weight)} {QueueDefaults.Show(conflict.PreviousWeight)} -> {QueueDefaults.Show(current.Weight)}");
            Log.QueueDefaultsConflict(_logger, conflict.Queue, conflict.PreviousPublishedBy, string.Join(", ", changes));
        }
    }

    /// <summary>Uses the maximum timeout because servers stop in parallel.</summary>
    private void WarnIfHostShutdownTooShort()
    {
        if (_servers.Count == 0 || _services.GetService<IOptions<HostOptions>>() is not { } host)
            return;
        var required = _servers.Max(s => s.Options.ShutdownTimeout + s.Options.AckFlushInterval) + ShutdownReserve;
        if (required > host.Value.ShutdownTimeout)
            Log.HostShutdownTimeoutTooShort(_logger, host.Value.ShutdownTimeout, required);
    }

    /// <summary>Configuration that prevents maintenance from meeting its guarantees.</summary>
    private void WarnIfMaintenanceMisconfigured(PgBoltProcessingOptions processing, PgBoltOptions core)
    {
        if (!processing.RunMaintenance || _servers.Count == 0)
            return;
        if (TimeSpan.FromDays(core.PartitionDaysAhead) < 2 * processing.PartitionsInterval)
            Log.PartitionDaysAheadTooShort(_logger, core.PartitionDaysAhead, processing.PartitionsInterval);
        var heartbeat = _servers.Max(s => s.Options.HeartbeatInterval);
        if (processing.DeadServerTimeout < 3 * heartbeat)
            Log.DeadServerTimeoutTooShort(_logger, processing.DeadServerTimeout, heartbeat);
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1000, EventName = nameof(JobTypeSchemaMismatch), Level = LogLevel.Warning,
            Message = "PgBolt: job types {JobTypes} were published by another process with a different argument schema.")]
        internal static partial void JobTypeSchemaMismatch(ILogger logger, string jobTypes);

        [LoggerMessage(EventId = 1001, EventName = nameof(ProcessingStartFailed), Level = LogLevel.Critical,
            Message = "PgBolt: starting processing failed; stopping what was started.")]
        internal static partial void ProcessingStartFailed(ILogger logger, Exception e);

        [LoggerMessage(EventId = 1002, EventName = nameof(HostStopTimeoutServers), Level = LogLevel.Warning,
            Message = "PgBolt: host stop timeout reached; pending outcomes are dropped and servers rows are left, leases will expire.")]
        internal static partial void HostStopTimeoutServers(ILogger logger);

        [LoggerMessage(EventId = 1003, EventName = nameof(HostStopTimeoutMaintenance), Level = LogLevel.Warning,
            Message = "PgBolt: host stop timeout reached while stopping maintenance loops.")]
        internal static partial void HostStopTimeoutMaintenance(ILogger logger);

        [LoggerMessage(EventId = 1004, EventName = nameof(QueueDefaultsConflict), Level = LogLevel.Warning,
            Message = "PgBolt: queue '{Queue}' defaults were recently published by {PublishedBy} with other values; overwritten: {Changes}.")]
        internal static partial void QueueDefaultsConflict(ILogger logger, string queue, string publishedBy, string changes);

        [LoggerMessage(EventId = 1005, EventName = nameof(HostShutdownTimeoutTooShort), Level = LogLevel.Warning,
            Message = "PgBolt: HostOptions.ShutdownTimeout {HostTimeout} is shorter than {Required} (max server ShutdownTimeout + AckFlushInterval + 2 s); executions may be abandoned and their attempts counted.")]
        internal static partial void HostShutdownTimeoutTooShort(ILogger logger, TimeSpan hostTimeout, TimeSpan required);

        [LoggerMessage(EventId = 1006, EventName = nameof(PartitionDaysAheadTooShort), Level = LogLevel.Warning,
            Message = "PgBolt: PartitionDaysAhead ({DaysAhead} days) is shorter than 2 × PartitionsInterval ({Interval}); a single failed iteration may leave no partition for the next day.")]
        internal static partial void PartitionDaysAheadTooShort(ILogger logger, int daysAhead, TimeSpan interval);

        [LoggerMessage(EventId = 1007, EventName = nameof(DeadServerTimeoutTooShort), Level = LogLevel.Warning,
            Message = "PgBolt: DeadServerTimeout ({Timeout}) is shorter than 3 × HeartbeatInterval ({Heartbeat}); rows of live servers will be removed on short heartbeat hiccups and restored by the next heartbeat.")]
        internal static partial void DeadServerTimeoutTooShort(ILogger logger, TimeSpan timeout, TimeSpan heartbeat);
    }
}
