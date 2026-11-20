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

public sealed class StatsTaskTests : IDisposable
{
    private static readonly DateTime ClaimedAt = new(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Day = TimeSpan.FromDays(1);
    private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(100);

    // promoter / sweeper / housekeeping / resolver: первый проход и пауза в сутки; дальше просыпается только stats.
    private const int OtherLoops = 4;

    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly ListLogger _logs = new();
    private readonly PgBoltProcessingOptions _options = new()
    {
        RunScheduler = false,
        PromoterInterval = Day,
        SweeperInterval = Day,
        MaintenancePollInterval = Day,
        ResolverInterval = Day,
    };
    private readonly List<DateTimeOffset> _claims = [];
    private readonly PgBoltInstrumentation _instrumentation = TestTelemetry.Instrumentation();
    private readonly ClusterQueueStats _cluster;
    private readonly ClusterTableStats _tables;
    private readonly PgBoltMetrics _metrics;
    private readonly MetricCollector<long> _errors;

    public StatsTaskTests()
    {
        _cluster = new ClusterQueueStats(new MonoClock(_time), _options);
        _tables = new ClusterTableStats(new MonoClock(_time), _options);
        _metrics = TestTelemetry.Metrics(_instrumentation, _cluster, _tables);
        _errors = new MetricCollector<long>(_instrumentation.Meter, I.StorageErrors);
    }

    public void Dispose()
    {
        _errors.Dispose();
        _instrumentation.Dispose();
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private ProcessingMaintenance Create(Func<double>? random = null) =>
        new(_store, new InProcessWakeupSignal(), _options, new FakeMaintenance(), new PgBoltOptions(), new MonoClock(_time),
            _logs.CreateLogger("test"), _metrics, _cluster, _tables)
        {
            NextRandom = random ?? (() => 0.5),
        };

    private int StatsClaims => _store.CallsOf("Claim").Count(c => ((ClaimCall)c.Detail!).Name == "stats");

    private CompleteCall[] Completes => [.. _store.CallsOf("Complete").Select(c => (CompleteCall)c.Detail!)];

    private int ErrorsWith(string text) => _logs.At(LogLevel.Error).Count(e => e.Message.Contains(text));

    private void ClaimStats() =>
        _store.OnClaim = (name, _, _) => Task.FromResult<DateTime?>(name == "stats" ? ClaimedAt : null);

    /// <summary>Модель maintenance_tasks: захват, если наступил next_run_at; захват ставит next_run_at = now + interval.</summary>
    private void ModelClaims()
    {
        var next = DateTimeOffset.MinValue;
        _store.OnClaim = (name, interval, _) =>
        {
            if (name != "stats")
                return Task.FromResult<DateTime?>(null);
            lock (_claims)
            {
                var now = _time.GetUtcNow();
                if (now < next)
                    return Task.FromResult<DateTime?>(null);
                next = now + interval;
                _claims.Add(now);
                return Task.FromResult<DateTime?>(now.UtcDateTime);
            }
        };
    }

    /// <summary>Семь проходов stats: в 0 с и после каждого из шести Advance(10 с); каждый ждёт взведённой паузы.</summary>
    private async Task SevenPassesAsync(ProcessingMaintenance maintenance)
    {
        maintenance.Start();
        for (var pass = 1; pass <= 7; pass++)
        {
            if (pass > 1)
                _time.Advance(Interval);
            var expected = pass;
            await Eventually.True(() => StatsClaims == expected && maintenance.IdleWaits == OtherLoops + expected,
                $"stats pass #{expected} is over");
        }
        await maintenance.StopAsync();
    }

    [Fact]
    public async Task Stats_claims_with_four_fifths_of_interval_and_runs_snapshot_then_trim_portions()
    {
        ClaimStats();
        var portions = new Queue<int>([10_000, 10_000, 3]);
        _store.OnTrim = (_, _, _) => Task.FromResult(portions.Dequeue());

        Assert.Equal(1, await Create().StatsOnceAsync(Ct));

        var trim = (TimeSpan.FromDays(7), 10_000);
        Assert.Equal(
            new StoreCall[]
            {
                new("Claim", Detail: new ClaimCall("stats", TimeSpan.FromSeconds(8))),
                new("Snapshot"),
                new("Trim", Detail: trim),
                new("Trim", Detail: trim),
                new("Trim", Detail: trim),
                new("Complete", Detail: new CompleteCall("stats", ClaimedAt, null, null)),
                new("ReadQueueStats"),
                new("ReadTableStats"),
            },
            _store.Calls);
    }

    [Fact]
    public async Task Not_claimed_stats_only_reads()
    {
        Assert.Equal(0, await Create().StatsOnceAsync(Ct));

        Assert.Equal(["Claim", "ReadQueueStats", "ReadTableStats"], _store.Calls.Select(c => c.Op));
    }

    private static QueueStatsSnapshot Snapshot(string queue) => new([new QueueStatsRow(queue, 1, 2, 3, 4, 5)], 0);

    private IReadOnlyList<QueueStatsRow> Published() =>
        _cluster.TryGetFresh(out var rows) ? rows : throw new InvalidOperationException("no fresh snapshot");

    [Fact]
    public async Task Owner_reads_after_complete()
    {
        ClaimStats();
        var snapshot = Snapshot("q");
        _store.OnReadQueueStats = _ => Task.FromResult(snapshot);

        Assert.Equal(1, await Create().StatsOnceAsync(Ct));

        Assert.Equal(["Claim", "Snapshot", "Trim", "Complete", "ReadQueueStats", "ReadTableStats"], _store.Calls.Select(c => c.Op));
        Assert.Same(snapshot.Rows, Published());
    }

    [Fact]
    public async Task Stats_loop_reads_without_maintenance()
    {
        _options.RunMaintenance = false;
        var reads = 0;
        _store.OnReadQueueStats = _ => Task.FromResult(Snapshot($"q{Interlocked.Increment(ref reads)}"));
        var maintenance = Create();
        maintenance.Start();

        await Eventually.True(() => reads == 1 && maintenance.IdleWaits == 1, "first stats pass is over");
        Assert.Equal("q1", Assert.Single(Published()).Queue);
        _time.Advance(Interval);
        await Eventually.True(() => reads == 2 && maintenance.IdleWaits == 2, "second stats pass is over");
        await maintenance.StopAsync();

        Assert.Equal("q2", Assert.Single(Published()).Queue);
        Assert.Empty(_store.CallsOf("Claim"));
    }

    /// <summary>Запуск циклов до первой ошибки итерации stats; число ошибок в логе в момент чтения снимка.</summary>
    private async Task<int> LogsAtReadAfterFailedPassAsync()
    {
        var logsAtRead = -1;
        _store.OnReadQueueStats = _ =>
        {
            logsAtRead = ErrorsWith("stats iteration failed");
            return Task.FromResult(new QueueStatsSnapshot([], 0));
        };
        var maintenance = Create();
        maintenance.Start();
        await Eventually.True(() => ErrorsWith("stats iteration failed") == 1, "stats iteration failure is logged");
        await maintenance.StopAsync();
        return logsAtRead;
    }

    [Fact]
    public async Task Claim_exception_does_not_skip_read()
    {
        _store.OnClaim = (name, _, _) => name == "stats"
            ? Task.FromException<DateTime?>(new IOException("db down"))
            : Task.FromResult<DateTime?>(null);

        Assert.Equal(0, await LogsAtReadAfterFailedPassAsync());
        Assert.Equal(["Claim", "ReadQueueStats"],
            _store.Calls.Where(c => c.Op == "ReadQueueStats" || c.Detail is ClaimCall { Name: "stats" }).Select(c => c.Op).Take(2));
    }

    [Fact]
    public async Task Complete_exception_does_not_skip_read()
    {
        ClaimStats();
        _store.OnComplete = (_, _, _, _, _) => throw new IOException("db down");

        Assert.Equal(0, await LogsAtReadAfterFailedPassAsync());
        Assert.Equal(["Complete", "ReadQueueStats"],
            _store.Calls.Select(c => c.Op).Where(op => op is "Complete" or "ReadQueueStats").Take(2));
    }

    [Fact]
    public async Task Read_error_keeps_snapshot()
    {
        _options.RunMaintenance = false;
        var snapshot = Snapshot("q");
        _store.OnReadQueueStats = _ => Task.FromResult(snapshot);
        var maintenance = Create();
        await maintenance.StatsOnceAsync(Ct);
        _store.OnReadQueueStats = _ => throw new IOException("db down");

        await maintenance.StatsOnceAsync(Ct);

        Assert.Equal(1, TestTelemetry.Sum(_errors, PgBoltTelemetry.Attr.StorageOperation, StorageOperations.StatsRead));
        Assert.Same(snapshot.Rows, Published());
        var entry = Assert.Single(_logs.Entries);
        Assert.Equal(5012, entry.EventId.Id);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.IsType<IOException>(entry.Exception);

        _time.Advance(TimeSpan.FromSeconds(30));
        await maintenance.StatsOnceAsync(Ct);

        Assert.Equal(2, TestTelemetry.Sum(_errors, PgBoltTelemetry.Attr.StorageOperation, StorageOperations.StatsRead));
        Assert.Single(_logs.Entries);   // ограничитель: не чаще раза в минуту
    }

    [Fact]
    public async Task Read_cancelled_by_stop_is_not_an_error()
    {
        _options.RunMaintenance = false;
        using var stop = new CancellationTokenSource();
        _store.OnReadQueueStats = async ct =>
        {
            await stop.CancelAsync();
            ct.ThrowIfCancellationRequested();
            return new QueueStatsSnapshot([], 0);
        };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Create().StatsOnceAsync(stop.Token));

        Assert.Equal(0, TestTelemetry.Sum(_errors, PgBoltTelemetry.Attr.StorageOperation, StorageOperations.StatsRead));
        Assert.Empty(_logs.Entries);
    }

    [Fact]
    public async Task Stop_during_snapshot_propagates_it_without_reading_queue_stats()
    {
        ClaimStats();
        using var stop = new CancellationTokenSource();
        _store.OnSnapshot = async ct =>
        {
            await stop.CancelAsync();
            ct.ThrowIfCancellationRequested();
        };
        var reads = 0;
        _store.OnReadQueueStats = _ =>
        {
            reads++;
            return Task.FromResult(new QueueStatsSnapshot([], 0));
        };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Create().StatsOnceAsync(stop.Token));

        Assert.Equal(0, reads);
    }

    [Fact]
    public async Task Failing_pass_completes_with_error_each_time_but_logs_once_per_minute()
    {
        ClaimStats();
        _store.OnSnapshot = _ => throw new IOException("boom");

        await SevenPassesAsync(Create());

        Assert.Equal(Enumerable.Repeat(new CompleteCall("stats", ClaimedAt, "boom", null), 7), Completes);
        Assert.Equal(2, ErrorsWith("stats"));   // 0 с и 60 с
    }

    [Fact]
    public async Task Pass_period_with_four_fifths_claim_is_at_most_six_fifths_interval()
    {
        ModelClaims();
        var calls = 0;
        var maintenance = Create(() => Interlocked.Increment(ref calls) % 2 == 0 ? 0.0 : 0.999);
        maintenance.Start();

        var end = _time.GetUtcNow() + TimeSpan.FromMinutes(5);
        while (_time.GetUtcNow() < end)
        {
            await Eventually.True(() => maintenance.IdleWaits == OtherLoops + StatsClaims, "stats loop is waiting");
            _time.Advance(Step);
        }
        await maintenance.StopAsync();

        Assert.True(_claims.Count >= 25, $"{_claims.Count} passes in 5 min");
        foreach (var (prev, next) in _claims.Zip(_claims.Skip(1)))
            Assert.InRange(next - prev, 0.8 * Interval, 1.2 * Interval + Step);
    }

    [Fact]
    public async Task Two_pods_share_one_pass_per_interval()
    {
        ModelClaims();
        var a = Create();
        var b = Create(() => 0.0);
        a.Start();
        b.Start();

        var end = _time.GetUtcNow() + TimeSpan.FromMinutes(2);
        while (_time.GetUtcNow() < end)
        {
            await Eventually.True(() => a.IdleWaits + b.IdleWaits == 2 * OtherLoops + StatsClaims, "both stats loops are waiting");
            _time.Advance(Step);
        }
        await a.StopAsync();
        await b.StopAsync();

        Assert.InRange(_store.CallsOf("Snapshot").Count(), 1, (int)(TimeSpan.FromMinutes(2) / (0.8 * Interval)) + 1);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Stats_claim_failure_is_logged_once_per_minute(bool completeFails)
    {
        if (completeFails)
        {
            ClaimStats();
            _store.OnComplete = (_, _, _, _, _) => throw new IOException("db down");
        }
        else
        {
            _store.OnClaim = (name, _, _) => name == "stats"
                ? Task.FromException<DateTime?>(new IOException("db down"))
                : Task.FromResult<DateTime?>(null);
        }

        await SevenPassesAsync(Create());

        Assert.Equal(2, ErrorsWith("stats iteration failed"));   // 0 с и 60 с
        Assert.Equal(2, ErrorsWith("stats"));                    // RunTaskAsync не пишет: проход успешен или не начат
        Assert.Equal(completeFails ? 7 : 0, Completes.Length);
    }
}
