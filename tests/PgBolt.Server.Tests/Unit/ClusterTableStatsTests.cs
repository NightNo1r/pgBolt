using System.Collections.Frozen;
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

public sealed class ClusterTableStatsTests : IDisposable
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(10);

    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly ListLogger _logs = new();
    private readonly PgBoltProcessingOptions _options = new() { StatsSnapshotInterval = Interval };
    private readonly PgBoltInstrumentation _instrumentation = TestTelemetry.Instrumentation();
    private readonly PgBoltMetrics _metrics;
    private readonly ProcessingMaintenance _maintenance;
    private readonly MetricCollector<long> _errors;

    public ClusterTableStatsTests()
    {
        var clock = new MonoClock(_time);
        var queues = new ClusterQueueStats(clock, _options);
        var tables = new ClusterTableStats(clock, _options);
        _metrics = TestTelemetry.Metrics(_instrumentation, queues, tables);
        _maintenance = new ProcessingMaintenance(_store, new InProcessWakeupSignal(), _options, new FakeMaintenance(),
            new PgBoltOptions(), clock, _logs.CreateLogger("test"), _metrics, queues, tables);
        _errors = new MetricCollector<long>(_instrumentation.Meter, I.StorageErrors);
    }

    public void Dispose()
    {
        _errors.Dispose();
        _instrumentation.Dispose();
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly TableStatsRow[] Rows = [new("queue", 5, 1000), new("completions", 7, 2000)];

    private IDisposable RegisterServer()
    {
        var options = new PgBoltServerOptions { Workers = 1 };
        options.Queue("q");
        var settings = new QueueSettingsSnapshot(FrozenDictionary<string, int>.Empty, FrozenDictionary<string, int>.Empty);
        return _metrics.RegisterServer("server", new SlotAllocator(options.Workers, options.Queues), 1, ["q"], () => settings);
    }

    private string[] Observe(string instrument)
    {
        using var collector = new MetricCollector<long>(_instrumentation.Meter, instrument);
        collector.RecordObservableInstruments();
        return [.. collector.GetMeasurementSnapshot()
            .Select(m => FormattableString.Invariant($"{m.Tags[PgBoltTelemetry.Attr.Table]}:{m.Value}")).Order(StringComparer.Ordinal)];
    }

    [Fact]
    public async Task Stats_pass_publishes_table_gauges()
    {
        using var _ = RegisterServer();
        _store.OnReadTableStats = _ => Task.FromResult<IReadOnlyList<TableStatsRow>>(Rows);

        await _maintenance.StatsOnceAsync(Ct);

        Assert.Equal(["completions:7", "queue:5"], Observe(I.ClusterTableDeadTuples));
        Assert.Equal(["completions:2000", "queue:1000"], Observe(I.ClusterTableSize));
    }

    [Fact]
    public async Task Stale_rows_are_hidden()
    {
        using var _ = RegisterServer();
        _store.OnReadTableStats = _ => Task.FromResult<IReadOnlyList<TableStatsRow>>(Rows);
        await _maintenance.StatsOnceAsync(Ct);

        _time.Advance(4 * Interval + TimeSpan.FromMilliseconds(1));

        Assert.Empty(Observe(I.ClusterTableDeadTuples));
        Assert.Empty(Observe(I.ClusterTableSize));
    }

    [Fact]
    public async Task Read_failure_counts_storage_error_and_keeps_rows()
    {
        using var _ = RegisterServer();
        _store.OnReadTableStats = _ => Task.FromResult<IReadOnlyList<TableStatsRow>>(Rows);
        await _maintenance.StatsOnceAsync(Ct);
        _store.OnReadTableStats = _ => throw new IOException("db down");

        await _maintenance.StatsOnceAsync(Ct);

        Assert.Equal(1, TestTelemetry.Sum(_errors, PgBoltTelemetry.Attr.StorageOperation, StorageOperations.TableStatsRead));
        Assert.Equal(["completions:7", "queue:5"], Observe(I.ClusterTableDeadTuples));
        var entry = Assert.Single(_logs.Entries);
        Assert.Equal(5013, entry.EventId.Id);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.IsType<IOException>(entry.Exception);
    }

    [Fact]
    public async Task Read_cancelled_by_stop_is_not_an_error()
    {
        using var stop = new CancellationTokenSource();
        _store.OnReadTableStats = async ct =>
        {
            await stop.CancelAsync();
            ct.ThrowIfCancellationRequested();
            return Rows;
        };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _maintenance.StatsOnceAsync(stop.Token));

        Assert.Equal(0, TestTelemetry.Sum(_errors, PgBoltTelemetry.Attr.StorageOperation, StorageOperations.TableStatsRead));
        Assert.Empty(_logs.Entries);
    }

    [Fact]
    public async Task Published_when_maintenance_disabled()
    {
        _options.RunMaintenance = false;
        using var _ = RegisterServer();
        _store.OnReadTableStats = _ => Task.FromResult<IReadOnlyList<TableStatsRow>>(Rows);

        await _maintenance.StatsOnceAsync(Ct);

        Assert.Equal(["completions:7", "queue:5"], Observe(I.ClusterTableDeadTuples));
        Assert.Empty(_store.CallsOf("Claim"));
    }

    [Fact]
    public async Task Pod_without_servers_does_not_publish()
    {
        _store.OnReadTableStats = _ => Task.FromResult<IReadOnlyList<TableStatsRow>>(Rows);

        await _maintenance.StatsOnceAsync(Ct);

        Assert.Empty(Observe(I.ClusterTableDeadTuples));
        Assert.Empty(Observe(I.ClusterTableSize));
    }
}
