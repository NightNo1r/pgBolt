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

public sealed class OutcomeStatsFlusherTests : IDisposable
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(15);
    private const string FailedMessage = "flushing outcome statistics failed";

    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 10, 5, 12, 0, 0, TimeSpan.Zero));
    private readonly FakeServerStore _store = new();
    private readonly ListLogger _logs = new();
    private readonly PgBoltInstrumentation _instrumentation = TestTelemetry.Instrumentation();
    private readonly MetricCollector<long> _errors;
    private readonly OutcomeStats _stats;
    private readonly OutcomeStatsFlusher _flusher;

    public OutcomeStatsFlusherTests()
    {
        _stats = new OutcomeStats(_time);
        _flusher = new OutcomeStatsFlusher(_store, _stats, Interval, new MonoClock(_time), _logs.CreateLogger("test"),
            TestTelemetry.Metrics(_instrumentation));
        _errors = new MetricCollector<long>(_instrumentation.Meter, I.StorageErrors);
    }

    public void Dispose()
    {
        _errors.Dispose();
        _instrumentation.Dispose();
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task NextIntervalAsync(int idleWaits)
    {
        await Eventually.True(() => _flusher.IdleWaits == idleWaits, $"idle wait #{idleWaits} is armed");
        _time.Advance(Interval);
    }

    [Fact]
    public async Task Flushes_every_interval_one_statement_with_all_rows()
    {
        _stats.Record(Guid.NewGuid(), "q", succeeded: 1);
        _stats.Record(Guid.NewGuid(), "q", failed: 1);
        _flusher.Start();

        await NextIntervalAsync(1);
        await Eventually.True(() => _store.CallsOf("FlushStats").Any(), "first interval flushes");
        Assert.Equal(2, ((string[])Assert.Single(_store.CallsOf("FlushStats")).Detail!).Length);

        await NextIntervalAsync(2);
        await Eventually.True(() => _flusher.IdleWaits == 3, "second interval has passed");
        Assert.Single(_store.CallsOf("FlushStats"));

        await _flusher.StopAsync(Ct);
    }

    [Fact]
    public async Task Failed_flush_restores_rows_and_merges_with_new()
    {
        var s = Guid.NewGuid();
        var calls = 0;
        var flushed = new List<IReadOnlyList<OutcomeStatsRow>>();
        _store.OnFlushStats = (rows, _) =>
        {
            if (Interlocked.Increment(ref calls) == 1)
                throw new IOException("db down");
            flushed.Add(rows);
            return Task.CompletedTask;
        };
        _stats.Record(s, "q", succeeded: 2);
        _flusher.Start();

        await NextIntervalAsync(1);
        await Eventually.True(() => _flusher.IdleWaits == 2, "failed flush is over");
        _stats.Record(s, "q", succeeded: 1);
        _time.Advance(Interval);

        await Eventually.True(() => flushed.Count == 1, "second flush succeeds");
        Assert.Equal([new OutcomeStatsRow(new DateTime(2026, 10, 5, 12, 0, 0, DateTimeKind.Utc), s, "q", 3, 0, 0)], flushed[0]);
        Assert.Single(_logs.At(LogLevel.Warning), e => e.Message.Contains(FailedMessage));

        _store.OnFlushStats = (_, _) => throw new IOException("db down");
        _stats.Record(s, "q", succeeded: 1);
        for (var k = 3; k < 3 + 8; k++)
            await NextIntervalAsync(k);
        await Eventually.True(() => _flusher.IdleWaits == 11, "eight failing intervals are over");

        Assert.Equal(10, _store.CallsOf("FlushStats").Count());
        Assert.Equal(3, _logs.At(LogLevel.Warning).Count(e => e.Message.Contains(FailedMessage)));   // 12:00:15, 12:01:15, 12:02:15
        await _flusher.StopAsync(Ct);
    }

    [Fact]
    public async Task Stop_waits_for_running_flush_then_flushes_once_more()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var current = 0;
        var max = 0;
        var flushed = new List<IReadOnlyList<OutcomeStatsRow>>();
        _store.OnFlushStats = async (rows, _) =>
        {
            var now = Interlocked.Increment(ref current);
            lock (flushed)
            {
                max = Math.Max(max, now);
                flushed.Add(rows);
            }
            if (flushed.Count == 1)
                await gate.Task;
            Interlocked.Decrement(ref current);
        };
        var s = Guid.NewGuid();
        _stats.Record(s, "a", succeeded: 1);
        _flusher.Start();
        await NextIntervalAsync(1);
        await Eventually.True(() => _store.CallsOf("FlushStats").Any(), "first flush is running");

        var stop = _flusher.StopAsync(Ct);
        _stats.Record(s, "b", succeeded: 1);
        Assert.False(stop.IsCompleted);

        gate.SetResult();
        await stop;

        Assert.Equal(2, flushed.Count);
        Assert.Equal("b", Assert.Single(flushed[1]).Queue);
        Assert.Equal(1, max);
    }

    [Fact]
    public async Task Cancelled_stop_does_not_wait_for_store_ignoring_token_and_keeps_rows()
    {
        var hung = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnFlushStats = (_, _) => hung.Task;   // токен не соблюдается
        _stats.Record(Guid.NewGuid(), "q", succeeded: 1);
        _flusher.Start();
        await NextIntervalAsync(1);
        await Eventually.True(() => _store.CallsOf("FlushStats").Any(), "flush is running");
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);

        var stopping = _flusher.StopAsync(stop.Token);
        await stop.CancelAsync();
        await stopping.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        Assert.Single(_stats.Take());
        hung.SetResult();
    }

    [Fact]
    public async Task Stop_with_cancelled_token_skips_final_flush()
    {
        _stats.Record(Guid.NewGuid(), "q", succeeded: 1);
        _flusher.Start();
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await _flusher.StopAsync(cancelled.Token);

        Assert.Empty(_store.CallsOf("FlushStats"));
    }

    [Fact]
    public async Task Each_failed_flush_counts_storage_error_regardless_of_log_throttle()
    {
        _store.OnFlushStats = (_, _) => throw new IOException("db down");
        _stats.Record(Guid.NewGuid(), "q", succeeded: 1);
        _flusher.Start();

        await NextIntervalAsync(1);
        await NextIntervalAsync(2);
        await Eventually.True(() => _flusher.IdleWaits == 3, "two failing intervals are over");

        Assert.Single(_logs.At(LogLevel.Warning), e => e.Message.Contains(FailedMessage));
        Assert.Equal(2, _errors.GetMeasurementSnapshot().Sum(m => m.Value));
        Assert.All(_errors.GetMeasurementSnapshot(),
            m => Assert.Equal(StorageOperations.StatsFlush, m.Tags[PgBoltTelemetry.Attr.StorageOperation]));
        await _flusher.StopAsync(Ct);
    }

    [Fact]
    public async Task Flush_aborted_by_cancelled_stop_is_not_storage_error()
    {
        var hung = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnFlushStats = (_, _) => hung.Task;   // токен не соблюдается
        _stats.Record(Guid.NewGuid(), "q", succeeded: 1);
        _flusher.Start();
        await NextIntervalAsync(1);
        await Eventually.True(() => _store.CallsOf("FlushStats").Any(), "flush is running");
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);

        var stopping = _flusher.StopAsync(stop.Token);
        await stop.CancelAsync();
        await stopping.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        Assert.Empty(_errors.GetMeasurementSnapshot());
        hung.SetResult();
    }
}
