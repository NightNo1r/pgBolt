using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;
using I = PgBolt.PgBoltDiagnostics.Instruments;

namespace PgBolt.Server.Tests.Unit;

public sealed class QueueSettingsCacheTests : IDisposable
{
    private const string FailedMessage = "refreshing queue settings failed";

    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 10, 12, 12, 0, 0, TimeSpan.Zero));
    private readonly FakeServerStore _store = new();
    private readonly ListLogger _logs = new();
    private readonly PgBoltInstrumentation _instrumentation = TestTelemetry.Instrumentation();
    private readonly MetricCollector<long> _errors;
    private readonly QueueSettingsCache _cache;

    public QueueSettingsCacheTests()
    {
        var options = new PgBoltProcessingOptions { QueueSettingsRefreshInterval = TimeSpan.FromSeconds(10) };
        _cache = new QueueSettingsCache(_store, options, new MonoClock(_time), _logs.CreateLogger("test"), TestTelemetry.Metrics(_instrumentation))
        {
            NextRandom = () => 1,
        };
        _errors = new MetricCollector<long>(_instrumentation.Meter, I.StorageErrors);
    }

    public void Dispose()
    {
        _errors.Dispose();
        _instrumentation.Dispose();
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private int Reads => _store.CallsOf("GetActiveQueueSettings").Count();

    private void Returns(params ActiveQueueSetting[] rows) =>
        _store.OnGetActiveQueueSettings = _ => Task.FromResult<IReadOnlyList<ActiveQueueSetting>>(rows);

    [Fact]
    public async Task Load_builds_snapshot()
    {
        Returns(new("a", 1, null), new("b", null, 3), new("c", 0, 2), new("Emails", 4, null), new("emails", null, 1));

        await _cache.LoadAsync(Ct);

        var snapshot = _cache.Current;
        Assert.Equal([KeyValuePair.Create("Emails", 4), KeyValuePair.Create("a", 1), KeyValuePair.Create("c", 0)],
            snapshot.Limits.OrderBy(l => l.Key, StringComparer.Ordinal).ToArray());
        Assert.False(snapshot.Limits.ContainsKey("emails"));
        Assert.Equal([KeyValuePair.Create("b", 3), KeyValuePair.Create("c", 2), KeyValuePair.Create("emails", 1)],
            snapshot.Weights.OrderBy(w => w.Key, StringComparer.Ordinal).ToArray());
        Assert.False(snapshot.Weights.ContainsKey("Emails"));
    }

    [Fact]
    public async Task Snapshot_keeps_limit_values()
    {
        Returns(new("a", 3, null), new("b", null, 2));

        await _cache.LoadAsync(Ct);

        Assert.Equal([KeyValuePair.Create("a", 3)], _cache.Current.Limits.ToArray());
        Assert.Equal([KeyValuePair.Create("b", 2)], _cache.Current.Weights.ToArray());
    }

    [Fact]
    public async Task Load_failure_throws_and_keeps_empty()
    {
        _store.OnGetActiveQueueSettings = _ => throw new IOException("db down");

        await Assert.ThrowsAsync<IOException>(() => _cache.LoadAsync(Ct));

        Assert.Same(QueueSettingsSnapshot.Empty, _cache.Current);
    }

    [Fact]
    public async Task Loop_refreshes_every_interval_with_jitter()
    {
        var randoms = new Queue<double>([1, 0, 0.5]);
        _cache.NextRandom = () => randoms.Dequeue();
        Returns(new ActiveQueueSetting("a", 1, null));
        await _cache.LoadAsync(Ct);
        Returns(new ActiveQueueSetting("b", 1, null));
        _cache.Start();

        await Eventually.True(() => _cache.Waits >= 1, "first wait is armed");
        _time.Advance(TimeSpan.FromSeconds(11));   // 10 s × 1.1
        await Eventually.True(() => _cache.Waits >= 2, "second wait is armed");

        Assert.Equal(2, Reads);
        Assert.Equal(["b"], _cache.Current.Limits.Keys);

        _time.Advance(TimeSpan.FromSeconds(9));    // 10 s × 0.9
        await Eventually.True(() => _cache.Waits >= 3, "third wait is armed");
        Assert.Equal(3, Reads);

        await _cache.StopAsync();
    }

    [Fact]
    public async Task Refresh_failure_keeps_previous_snapshot_and_logs_warning()
    {
        Returns(new ActiveQueueSetting("a", 1, null));
        await _cache.LoadAsync(Ct);
        var loaded = _cache.Current;
        _store.OnGetActiveQueueSettings = _ => throw new IOException("db down");
        _cache.Start();

        await Eventually.True(() => _cache.Waits >= 1, "first wait is armed");
        _time.Advance(TimeSpan.FromSeconds(11));
        await Eventually.True(() => _cache.Waits >= 2, "failed refresh is over");

        Assert.Same(loaded, _cache.Current);
        var warning = Assert.Single(_logs.At(LogLevel.Warning));
        Assert.Contains(FailedMessage, warning.Message);
        Assert.IsType<IOException>(warning.Exception);

        await _cache.StopAsync();
    }

    [Fact]
    public async Task RefreshNow_during_refresh_schedules_one_more()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnGetActiveQueueSettings = async _ =>
        {
            if (Reads == 1)
                await gate.Task;
            return [];
        };
        _cache.Start();
        await Eventually.True(() => _cache.Waits >= 1, "first wait is armed");

        _cache.RefreshNow();
        await Eventually.True(() => Reads == 1, "refresh is in flight");
        _cache.RefreshNow();
        _cache.RefreshNow();
        Assert.Equal(1, Reads);

        gate.SetResult();
        await Eventually.True(() => _cache.Waits >= 3, "pending refresh is over");

        Assert.Equal(2, Reads);
        await _cache.StopAsync();
    }

    [Fact]
    public async Task RefreshNow_before_start_and_after_stop_is_noop()
    {
        _cache.RefreshNow();
        _cache.Start();
        await Eventually.True(() => _cache.Waits >= 1, "first wait is armed");
        await _cache.StopAsync();

        _cache.RefreshNow();

        Assert.Equal(0, Reads);
    }

    [Fact]
    public async Task Stop_completes_loop()
    {
        var hung = new TaskCompletionSource<IReadOnlyList<ActiveQueueSetting>>(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnGetActiveQueueSettings = ct => hung.Task.WaitAsync(ct);
        _cache.Start();
        await Eventually.True(() => _cache.Waits >= 1, "first wait is armed");
        _cache.RefreshNow();
        await Eventually.True(() => Reads == 1, "refresh is in flight");

        await _cache.StopAsync().WaitAsync(TimeSpan.FromSeconds(10), Ct);

        Assert.Empty(_logs.At(LogLevel.Warning));
    }

    [Fact]
    public async Task Refresh_failure_counts_storage_error()
    {
        _store.OnGetActiveQueueSettings = _ => throw new IOException("db down");
        _cache.Start();

        await Eventually.True(() => _cache.Waits >= 1, "first wait is armed");
        _time.Advance(TimeSpan.FromSeconds(11));
        await Eventually.True(() => _cache.Waits >= 2, "failed refresh is over");
        await _cache.StopAsync();

        var m = Assert.Single(_errors.GetMeasurementSnapshot());
        Assert.Equal((1L, StorageOperations.Settings), (m.Value, m.Tags[PgBoltTelemetry.Attr.StorageOperation]));
    }

    [Fact]
    public async Task Stop_is_not_counted_as_storage_error()
    {
        var hung = new TaskCompletionSource<IReadOnlyList<ActiveQueueSetting>>(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnGetActiveQueueSettings = ct => hung.Task.WaitAsync(ct);
        _cache.Start();
        await Eventually.True(() => _cache.Waits >= 1, "first wait is armed");
        _cache.RefreshNow();
        await Eventually.True(() => Reads == 1, "refresh is in flight");

        await _cache.StopAsync().WaitAsync(TimeSpan.FromSeconds(10), Ct);

        Assert.Empty(_errors.GetMeasurementSnapshot());
    }
}
