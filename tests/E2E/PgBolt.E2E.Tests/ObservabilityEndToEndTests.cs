using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Hosting;
using PgBolt.Testing;
using I = PgBolt.PgBoltDiagnostics.Instruments;

namespace PgBolt.E2E.Tests;

/// <summary>
/// Трассировка и кластерные метрики на настоящих хостах. Слушатель источника <c>PgBolt</c> видит span'ы всех тестов процесса,
/// поэтому класс в непараллельной коллекции, а span'ы отбираются по TraceId своей корневой Activity.
/// </summary>
[Collection(TelemetryE2ECollection.Name)]
public sealed class ObservabilityEndToEndTests(PostgresFixture pg) : IAsyncLifetime
{
    private const int QueueLimit = 3;
    private const int FirstHostWorkers = 2;
    private const int SecondHostWorkers = 3;
    private const int ThirdHostWorkers = 4;
    private static readonly TimeSpan StatsInterval = TimeSpan.FromMilliseconds(200);

    // Проходы stats идут раз в 0,8–1,2 интервала, первый — после старта хоста.
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    private readonly List<IHost> _hosts = [];
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, ConnectionMode.Direct, migrate: false);

    public async ValueTask DisposeAsync()
    {
        for (var i = _hosts.Count - 1; i >= 0; i--)
            await StopAsync(_hosts[i]);
        await _db.DisposeAsync();
    }

    [Fact]
    public async Task Trace_flows_from_enqueue_to_job_code()
    {
        using var pgBoltSpans = new ActivityCapture(PgBoltDiagnostics.ActivitySourceName);
        using var jobSpans = new ActivityCapture(TraceJob.Source.Name);
        await StartHostAsync(name: "trace-server", queue: "e2e", runMaintenance: true);
        var client = await StartHostAsync(name: null, queue: null, runMaintenance: false);

        ActivityTraceId trace;
        using (var root = TraceJob.Source.StartActivity("root"))
        {
            Assert.NotNull(root);
            trace = root.TraceId;
            await client.Services.GetRequiredService<IJobClient>().EnqueueAsync(new TraceArgs(trace.ToString()), ct: Ct);
        }

        Activity[] spans = [];
        await Eventually.True(
            () => (spans = [.. pgBoltSpans.Stopped.Concat(jobSpans.Stopped).Where(a => a.TraceId == trace)]).Length == 4,
            "root, producer, consumer and job spans share the trace", WaitTimeout);
        var byName = spans.ToDictionary(a => a.DisplayName);
        var producer = byName["enqueue e2e.trace"];
        var consumer = byName["e2e.trace"];
        Assert.Equal(ActivityKind.Producer, producer.Kind);
        Assert.Equal(ActivityKind.Consumer, consumer.Kind);
        Assert.Equal(byName["root"].SpanId, producer.ParentSpanId);
        Assert.Equal(producer.SpanId, consumer.ParentSpanId);
        Assert.Equal(consumer.SpanId, byName["e2e.work"].ParentSpanId);
    }

    [Fact]
    public async Task Cluster_metrics_agree_across_two_hosts()
    {
        var first = await StartHostAsync(name: "stats-1", queue: "q", runMaintenance: true, workers: FirstHostWorkers);
        var second = await StartHostAsync(name: "stats-2", queue: "q", runMaintenance: false, workers: SecondHostWorkers);
        var client = first.Services.GetRequiredService<IJobClient>();
        for (var i = 0; i < 5; i++)
            await client.EnqueueAsync(new TraceArgs("delayed"), new EnqueueOptions { Queue = "q", Delay = TimeSpan.FromHours(1) }, Ct);
        await client.EnqueueAsync(new TraceArgs("ready"), new EnqueueOptions { Queue = "unserved" }, Ct);

        using (var firstMetrics = new ClusterMetrics(first))
        using (var secondMetrics = new ClusterMetrics(second))
        {
            foreach (var metrics in new[] { firstMetrics, secondMetrics })
                await Eventually.True(() => metrics.Read().Has(scheduled: 5), "each host publishes the cluster snapshot and limit", WaitTimeout);
        }

        // Первый хост остановлен, снимки stats теперь делает только третий (единственный с RunMaintenance). Шестая отложенная
        // задача поставлена после этого, поэтому scheduled: 6 появится лишь в снимке нового владельца.
        await StopAsync(first);
        var third = await StartHostAsync(name: "stats-3", queue: "q", runMaintenance: true, workers: ThirdHostWorkers);
        await second.Services.GetRequiredService<IJobClient>()
            .EnqueueAsync(new TraceArgs("delayed"), new EnqueueOptions { Queue = "q", Delay = TimeSpan.FromHours(1) }, Ct);

        using var a = new ClusterMetrics(second);
        using var b = new ClusterMetrics(third);
        ClusterReading readA = null!, readB = null!;
        await Eventually.True(() => (readA = a.Read()).Has(scheduled: 6) && (readB = b.Read()).Has(scheduled: 6),
            "both hosts publish the new owner's snapshot", WaitTimeout);

        Assert.Equal(readA.Jobs, readB.Jobs);
        Assert.Equal(readA.Limits, readB.Limits);
        Assert.Equal(readA.OldestReadyAge["unserved"], readB.OldestReadyAge["unserved"], StatsInterval.TotalSeconds * 5);
        Assert.Equal(SecondHostWorkers + ThirdHostWorkers, readA.Capacity + readB.Capacity);
    }

    [Fact]
    public async Task Idle_cluster_publishes_zeros()
    {
        var first = await StartHostAsync(name: "idle-1", queue: "idle-a", runMaintenance: true);
        var second = await StartHostAsync(name: "idle-2", queue: "idle-b", runMaintenance: false);

        foreach (var (host, queue) in new[] { (first, "idle-a"), (second, "idle-b") })
        {
            using var metrics = new ClusterMetrics(host);
            ClusterReading reading = null!;
            await Eventually.True(() => (reading = metrics.Read()).Jobs.Count > 0, "the host reads the first snapshot", WaitTimeout);
            Assert.Equal(
                new Dictionary<(string, string), long>
                {
                    [(queue, "scheduled")] = 0, [(queue, "enqueued")] = 0, [(queue, "processing")] = 0, [(queue, "awaiting")] = 0,
                },
                reading.Jobs);
        }
    }

    /// <summary>
    /// Хост с сервером на очереди <paramref name="queue"/> (лимит кластера <see cref="QueueLimit"/>), снимком stats раз в
    /// <see cref="StatsInterval"/>; <paramref name="name"/> = null — хост-клиент без сервера. Схему мигрирует первый хост.
    /// </summary>
    private async Task<IHost> StartHostAsync(string? name, string? queue, bool runMaintenance, int workers = FirstHostWorkers)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Services.AddMetrics();
        builder.Services.AddPgBolt(o =>
        {
            o.ConnectionString = _db.ConnectionString;
            o.Schema = _db.Schema;
            o.AddJobs(PgBoltJobRegistry.Register);
        });
        if (name is not null)
        {
            builder.Services.AddPgBoltServer(name, o =>
            {
                o.Workers = workers;
                o.Queue(queue!, q => q.ClusterMaxConcurrency = QueueLimit);
            });
            builder.Services.ConfigurePgBoltProcessing(o =>
            {
                o.RunMaintenance = runMaintenance;
                o.StatsSnapshotInterval = StatsInterval;
            });
        }
        var host = builder.Build();
        _hosts.Add(host);
        await host.StartAsync(Ct);
        return host;
    }

    private async Task StopAsync(IHost host)
    {
        _hosts.Remove(host);
        await host.StopAsync(CancellationToken.None);
        host.Dispose();
    }

    /// <summary>Кластерные gauges и ёмкость серверов одного хоста: только instruments его <see cref="IMeterFactory"/>.</summary>
    private sealed class ClusterMetrics(IHost host) : IDisposable
    {
        private readonly MetricCollector<long> _jobs = Collector<long>(host, I.ClusterQueueJobs);
        private readonly MetricCollector<double> _age = Collector<double>(host, I.ClusterQueueOldestReadyAge);
        private readonly MetricCollector<int> _limit = Collector<int>(host, I.ClusterQueueLimit);
        private readonly MetricCollector<int> _capacity = Collector<int>(host, I.WorkerCapacity);

        /// <summary>Один сбор observable-instruments: значения последнего вызова callbacks.</summary>
        public ClusterReading Read()
        {
            // У каждого collector'а свой MeterListener: callbacks его instrument вызывает только он сам.
            _jobs.RecordObservableInstruments();
            _age.RecordObservableInstruments();
            _limit.RecordObservableInstruments();
            _capacity.RecordObservableInstruments();
            return new ClusterReading(
                _jobs.GetMeasurementSnapshot(clear: true).ToDictionary(m => ((string)m.Tags["pgbolt.queue"]!, (string)m.Tags["pgbolt.job.state"]!), m => m.Value),
                _age.GetMeasurementSnapshot(clear: true).ToDictionary(m => (string)m.Tags["pgbolt.queue"]!, m => m.Value),
                _limit.GetMeasurementSnapshot(clear: true).ToDictionary(m => (string)m.Tags["pgbolt.queue"]!, m => m.Value),
                _capacity.GetMeasurementSnapshot(clear: true).Sum(m => m.Value));
        }

        public void Dispose()
        {
            _jobs.Dispose();
            _age.Dispose();
            _limit.Dispose();
            _capacity.Dispose();
        }

        private static MetricCollector<T> Collector<T>(IHost host, string instrument) where T : struct =>
            new(host.Services.GetRequiredService<IMeterFactory>(), PgBoltDiagnostics.MeterName, instrument);
    }

    private sealed record ClusterReading(
        Dictionary<(string Queue, string State), long> Jobs,
        Dictionary<string, double> OldestReadyAge,
        Dictionary<string, int> Limits,
        int Capacity)
    {
        /// <summary>Свежий снимок с <paramref name="scheduled"/> отложенными в q, готовой задачей в unserved и лимитом q.</summary>
        public bool Has(int scheduled) =>
            Jobs.GetValueOrDefault(("q", "scheduled")) == scheduled
            && OldestReadyAge.ContainsKey("unserved")
            && Limits.GetValueOrDefault("q") == QueueLimit;
    }
}

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class TelemetryE2ECollection
{
    /// <summary>Тесты со слушателями источника и Meter'а PgBolt: идут поочерёдно, без span'ов и измерений соседей.</summary>
    public const string Name = "e2e-telemetry";
}
