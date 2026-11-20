using System.Diagnostics;
using System.Diagnostics.Metrics;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Storage;
using static PgBolt.Postgres.Diagnostics.PgBoltTelemetry;
using I = PgBolt.PgBoltDiagnostics.Instruments;

namespace PgBolt.Server.Runtime;

/// <summary>Значения тега <c>pgbolt.drop.reason</c>.</summary>
internal static class DropReasons
{
    public const string LeaseLost = "lease_lost";
    public const string Abandoned = "abandoned";
    public const string Stopped = "stopped";
    public const string SendTimeout = "send_timeout";
}

/// <summary>Значения тега <c>pgbolt.maintenance.result</c>.</summary>
internal static class MaintenanceResults
{
    public const string Ok = "ok";
    public const string Error = "error";
    /// <summary>Задача вернула ошибку без исключения (например, lock_timeout партиций).</summary>
    public const string Deferred = "deferred";
}

/// <summary>
/// Значения тега <c>pgbolt.storage.operation</c>. Исключения итераций циклов обслуживания пишутся с именем цикла.
/// </summary>
internal static class StorageOperations
{
    public const string Dequeue = "dequeue";
    public const string Ack = "ack";
    public const string Outcome = "outcome";
    public const string Heartbeat = "heartbeat";
    public const string Settings = "settings";
    public const string StatsFlush = "stats_flush";
    public const string StatsRead = "stats_read";
    public const string TableStatsRead = "table_stats_read";
}

/// <summary>
/// Метрики сервера. Синхронная запись под защитой: сбой слушателя пишет 8001 и не ломает вызывающего. Observable-instruments
/// создаются один раз; их callbacks перечисляют зарегистрированные виртуальные серверы и читают только память.
/// Кластерные gauges публикуются, только пока зарегистрирован хотя бы один сервер: под без серверов их не публикует.
/// </summary>
internal sealed class PgBoltMetrics
{
    private readonly PgBoltInstrumentation _instrumentation;
    private readonly ClusterQueueStats _cluster;
    private readonly ClusterTableStats _tables;
    private readonly Lock _lock = new();
    private readonly List<ServerSource> _servers = [];
    private readonly Histogram<double> _jobDuration;
    private readonly Histogram<double> _jobWaitTime;
    private readonly Histogram<double> _maintenanceDuration;
    private readonly Counter<long> _dropped;
    private readonly Counter<long> _storageErrors;
    private readonly Counter<long> _maintenanceRuns;
    private readonly Counter<long> _recurringFired;
    private readonly Counter<long> _retentionDeferred;

    public PgBoltMetrics(PgBoltInstrumentation instrumentation, ClusterQueueStats cluster, ClusterTableStats tables)
    {
        _instrumentation = instrumentation;
        _cluster = cluster;
        _tables = tables;
        var meter = instrumentation.Meter;
        var jobBuckets = new InstrumentAdvice<double> { HistogramBucketBoundaries = PgBoltDiagnostics.HistogramBuckets.Job };
        _jobDuration = meter.CreateHistogram(I.JobDuration, "s", null, null, jobBuckets);
        _jobWaitTime = meter.CreateHistogram(I.JobWaitTime, "s", null, null, jobBuckets);
        _maintenanceDuration = meter.CreateHistogram(I.MaintenanceDuration, "s", null, null,
            new InstrumentAdvice<double> { HistogramBucketBoundaries = PgBoltDiagnostics.HistogramBuckets.Maintenance });
        _dropped = meter.CreateCounter<long>(I.JobOutcomeDropped, "{job}");
        _storageErrors = meter.CreateCounter<long>(I.StorageErrors, "{error}");
        _maintenanceRuns = meter.CreateCounter<long>(I.MaintenanceRuns, "{run}");
        _recurringFired = meter.CreateCounter<long>(I.RecurringFired, "{job}");
        _retentionDeferred = meter.CreateCounter<long>(I.RetentionDeferred, "{partition}");

        meter.CreateObservableUpDownCounter(I.WorkerActive, () => Observe(WorkerActive), "{worker}");
        meter.CreateObservableUpDownCounter(I.WorkerCapacity, () => Observe(WorkerCapacity), "{worker}");
        meter.CreateObservableGauge(I.ClusterQueueJobs, () => Observe(ClusterJobs), "{job}");
        meter.CreateObservableGauge(I.ClusterQueueOldestReadyAge, () => Observe(OldestReadyAge), "s");
        meter.CreateObservableGauge(I.ClusterQueueLimit, () => Observe(Limits), "{worker}");
        meter.CreateObservableGauge(I.ClusterTableDeadTuples, () => Observe(_ => TableMeasurements(r => r.DeadTuples)), "{tuple}");
        meter.CreateObservableGauge(I.ClusterTableSize, () => Observe(_ => TableMeasurements(r => r.SizeBytes)), "By");
    }

    /// <summary>Общая точка сообщения о сбоях телеметрии для компонентов, которым нужны и метрики, и spans.</summary>
    public PgBoltInstrumentation Instrumentation => _instrumentation;

    /// <summary>Значение тега <c>pgbolt.job.outcome</c>.</summary>
    public static string OutcomeName(JobOutcome outcome) => outcome switch
    {
        JobOutcome.Ack => "succeeded",
        JobOutcome.Retry => "retry",
        JobOutcome.Fail => "failed",
        JobOutcome.ConfirmCancel => "cancelled",
        JobOutcome.Release => "released",
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, null),
    };

    public void JobDuration(string queue, string jobType, string outcome, TimeSpan duration) =>
        GuardedInstruments.Record(_jobDuration, duration.TotalSeconds,
            new TagList { { Attr.Queue, queue }, { Attr.JobType, jobType }, { Attr.JobOutcome, outcome } }, _instrumentation);

    public void JobWaitTime(string queue, string jobType, double seconds) =>
        GuardedInstruments.Record(_jobWaitTime, seconds, new TagList { { Attr.Queue, queue }, { Attr.JobType, jobType } },
            _instrumentation);

    public void OutcomeDropped(string queue, string reason, int count = 1) =>
        GuardedInstruments.Add(_dropped, count, new TagList { { Attr.Queue, queue }, { Attr.DropReason, reason } }, _instrumentation);

    /// <param name="operation">Константа <see cref="StorageOperations"/> или имя цикла обслуживания.</param>
    public void StorageError(string operation) =>
        GuardedInstruments.Add(_storageErrors, 1, new TagList { { Attr.StorageOperation, operation } }, _instrumentation);

    public void MaintenanceRun(string task, string result, TimeSpan duration)
    {
        GuardedInstruments.Add(_maintenanceRuns, 1, new TagList { { Attr.MaintenanceTask, task }, { Attr.MaintenanceResult, result } }, _instrumentation);
        GuardedInstruments.Record(_maintenanceDuration, duration.TotalSeconds, new TagList { { Attr.MaintenanceTask, task } },
            _instrumentation);
    }

    public void RecurringFired(int count) => GuardedInstruments.Add(_recurringFired, count, default, _instrumentation);

    public void RetentionDeferred(string reason, int count) =>
        GuardedInstruments.Add(_retentionDeferred, count, new TagList { { Attr.RetentionReason, reason } }, _instrumentation);

    /// <summary>
    /// Источник observable-метрик виртуального сервера до <c>Dispose</c> результата. <paramref name="settings"/> читается
    /// у первого зарегистрированного сервера: все серверы процесса делят один кэш настроек очередей.
    /// </summary>
    public IDisposable RegisterServer(string serverName, SlotAllocator slots, int capacity, IReadOnlyList<string> queues,
        Func<QueueSettingsSnapshot> settings)
    {
        var source = new ServerSource(this, serverName, slots, capacity, [.. queues], settings);
        lock (_lock)
            _servers.Add(source);
        return source;
    }

    /// <summary>Сбой callback'а пишет 8002 и даёт пустой набор, а не исключение в сборщик метрик.</summary>
    private IEnumerable<Measurement<T>> Observe<T>(Func<ServerSource[], List<Measurement<T>>> collect) where T : struct
    {
        try
        {
            ServerSource[] servers;
            lock (_lock)
                servers = [.. _servers];
            return servers.Length == 0 ? [] : collect(servers);
        }
        catch (Exception e)
        {
            _instrumentation.ReportCallbackFailure(e);
            return [];
        }
    }

    private static List<Measurement<int>> WorkerActive(ServerSource[] servers)
    {
        var result = new List<Measurement<int>>();
        foreach (var server in servers)
            foreach (var queue in server.Queues)
                result.Add(new(server.Slots.InUse(queue), new TagList { { Attr.ServerName, server.Name }, { Attr.Queue, queue } }));
        return result;
    }

    private static List<Measurement<int>> WorkerCapacity(ServerSource[] servers) =>
        [.. servers.Select(s => new Measurement<int>(s.Capacity, new TagList { { Attr.ServerName, s.Name } }))];

    /// <summary>
    /// Четыре состояния на каждую известную очередь — очереди серверов пода и строки снимка, без дублей; очередь без строки —
    /// нули, чтобы простой отличался от устаревшего снимка (тогда рядов нет).
    /// </summary>
    private List<Measurement<long>> ClusterJobs(ServerSource[] servers)
    {
        if (!_cluster.TryGetFresh(out var rows))
            return [];
        var known = new Dictionary<string, QueueStatsRow?>(StringComparer.Ordinal);
        foreach (var server in servers)
            foreach (var queue in server.Queues)
                known.TryAdd(queue, null);
        foreach (var row in rows)
            known[row.Queue] = row;

        var result = new List<Measurement<long>>(known.Count * 4);
        foreach (var (queue, row) in known)
        {
            result.Add(Jobs(row?.Scheduled ?? 0, queue, "scheduled"));
            result.Add(Jobs(row?.Enqueued ?? 0, queue, "enqueued"));
            result.Add(Jobs(row?.Processing ?? 0, queue, "processing"));
            result.Add(Jobs(row?.Awaiting ?? 0, queue, "awaiting"));
        }
        return result;

        static Measurement<long> Jobs(long value, string queue, string state) =>
            new(value, new TagList { { Attr.Queue, queue }, { Attr.JobState, state } });
    }

    private List<Measurement<double>> OldestReadyAge(ServerSource[] _) =>
        _cluster.TryGetFresh(out var rows)
            ? [.. rows.Where(r => r.OldestReadySeconds is not null)
                .Select(r => new Measurement<double>(r.OldestReadySeconds!.Value, new TagList { { Attr.Queue, r.Queue } }))]
            : [];

    private List<Measurement<long>> TableMeasurements(Func<TableStatsRow, long> value) =>
        _tables.TryGetFresh(out var rows)
            ? [.. rows.Select(r => new Measurement<long>(value(r), new TagList { { Attr.Table, r.Table } }))]
            : [];

    private static List<Measurement<int>> Limits(ServerSource[] servers) =>
        [.. servers[0].Settings().Limits.Select(l => new Measurement<int>(l.Value, new TagList { { Attr.Queue, l.Key } }))];

    private sealed class ServerSource(PgBoltMetrics owner, string name, SlotAllocator slots, int capacity, string[] queues,
        Func<QueueSettingsSnapshot> settings) : IDisposable
    {
        public string Name { get; } = name;
        public SlotAllocator Slots { get; } = slots;
        public int Capacity { get; } = capacity;
        public string[] Queues { get; } = queues;
        public Func<QueueSettingsSnapshot> Settings { get; } = settings;

        public void Dispose()
        {
            lock (owner._lock)
                owner._servers.Remove(this);
        }
    }
}
