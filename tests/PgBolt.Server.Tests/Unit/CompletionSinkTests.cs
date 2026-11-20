using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;
using static PgBolt.Postgres.Diagnostics.PgBoltTelemetry;
using I = PgBolt.PgBoltDiagnostics.Instruments;

namespace PgBolt.Server.Tests.Unit;

public sealed class CompletionSinkTests : IDisposable
{
    private static readonly TimeSpan Ms = TimeSpan.FromMilliseconds(1);

    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly ExecutionTable _table = new();
    private readonly ListLogger _logs = new();
    private readonly MonoClock _clock;
    private readonly OutcomeStats _stats;
    private readonly Guid _serverId = Guid.NewGuid();
    private readonly PgBoltInstrumentation _instrumentation = TestTelemetry.Instrumentation();
    private readonly PgBoltMetrics _metrics;
    private readonly MetricCollector<long> _dropped;
    private readonly MetricCollector<long> _errors;
    private readonly PgBoltServerOptions _options = new PgBoltServerOptions
    {
        LeaseDuration = TimeSpan.FromSeconds(60),
        HeartbeatInterval = TimeSpan.FromSeconds(15),
        AckBatchSize = 100,
        AckFlushInterval = TimeSpan.FromMilliseconds(20),
    }.Queue("q");

    public CompletionSinkTests()
    {
        _clock = new MonoClock(_time);
        _stats = new OutcomeStats(_time);
        _metrics = TestTelemetry.Metrics(_instrumentation);
        _dropped = new MetricCollector<long>(_instrumentation.Meter, I.JobOutcomeDropped);
        _errors = new MetricCollector<long>(_instrumentation.Meter, I.StorageErrors);
    }

    public void Dispose()
    {
        _dropped.Dispose();
        _errors.Dispose();
        _instrumentation.Dispose();
    }

    private long Dropped(string reason) => TestTelemetry.Sum(_dropped, Attr.DropReason, reason);

    private long Errors(string operation) => TestTelemetry.Sum(_errors, Attr.StorageOperation, operation);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private CompletionSink Sink(Action<string>? onLeaseFreed = null)
    {
        var sink = new CompletionSink(_serverId, _store, _table, _options, _clock, _logs.CreateLogger("test"), _stats, _metrics, onLeaseFreed);
        sink.Start();
        return sink;
    }

    private DateTime Bucket()
    {
        var now = _time.GetUtcNow().UtcDateTime;
        return new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
    }

    private Execution Running(string queue = "q")
    {
        var execution = new Execution(Leased.Job(), queue, _clock.Add(_clock.Now, _options.LeaseDuration), ExecutionState.Running);
        _table.Add(execution);
        return execution;
    }

    [Fact]
    public async Task Ack_batch_is_sent_when_size_is_reached()
    {
        _options.AckBatchSize = 3;
        var sink = Sink();
        var executions = Enumerable.Range(0, 3).Select(_ => Running()).ToArray();

        foreach (var execution in executions)
            Assert.True(sink.Complete(execution, new JobOutcome.Ack()));

        await Eventually.True(() => _table.Count == 0, "the batch of 3 is acked without advancing time");
        var ack = Assert.Single(_store.CallsOf("Ack"));
        Assert.Equal(executions.Select(e => e.Lease), (LeaseRef[])ack.Detail!);
        Assert.All(executions, e => Assert.Equal(ExecutionState.Removed, e.State));
    }

    [Fact]
    public async Task Ack_batch_is_sent_after_flush_interval_from_first_item()
    {
        var sink = Sink();
        var first = Running();
        var second = Running();

        sink.Complete(first, new JobOutcome.Ack());
        _time.Advance(10 * Ms);
        sink.Complete(second, new JobOutcome.Ack());
        _time.Advance(9 * Ms);
        Assert.Empty(_store.CallsOf("Ack"));

        _time.Advance(1 * Ms);
        await Eventually.True(() => _table.Count == 0, "the window of 20 ms from the first item has passed");
        var ack = Assert.Single(_store.CallsOf("Ack"));
        Assert.Equal(new[] { first.Lease, second.Lease }, (LeaseRef[])ack.Detail!);
    }

    [Fact]
    public async Task Execution_stays_in_table_until_send_completes()
    {
        _options.AckBatchSize = 1;
        var gate = new TaskCompletionSource<IReadOnlyList<Guid>>(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnAck = (_, _) => gate.Task;
        var sink = Sink();
        var execution = Running();

        sink.Complete(execution, new JobOutcome.Ack());
        await Eventually.True(() => _store.CallsOf("Ack").Any(), "ack is sent");

        Assert.Equal(ExecutionState.OutcomeInFlight, execution.State);
        Assert.True(_table.TryGet(execution.Job.LeaseId, out _));

        gate.SetResult([execution.Job.Id]);
        await Eventually.True(() => _table.Count == 0, "record leaves the table after the answer");
        Assert.Equal(ExecutionState.Removed, execution.State);
    }

    [Fact]
    public async Task Any_send_error_is_retried_with_doubling_backoff_capped_at_5s()
    {
        _options.AckBatchSize = 1;
        var times = new List<DateTimeOffset>();
        _store.OnAck = (leases, _) =>
        {
            lock (times)
                times.Add(_time.GetUtcNow());
            return times.Count <= 8 ? throw new IOException("network") : FakeServerStore.AckAll(leases);
        };
        var sink = Sink();
        var execution = Running();
        sink.Complete(execution, new JobOutcome.Ack());

        int[] expectedMs = [100, 200, 400, 800, 1600, 3200, 5000, 5000];
        for (var k = 1; k <= expectedMs.Length; k++)
        {
            await Eventually.True(() => sink.BackoffWaits == k, $"backoff wait #{k} is armed");
            Assert.Equal(ExecutionState.OutcomePending, execution.State);
            Assert.True(_table.TryGet(execution.Job.LeaseId, out _));
            _time.Advance(expectedMs[k - 1] * Ms);
        }

        await Eventually.True(() => _table.Count == 0, "the 9th attempt succeeds");
        Assert.Equal(9, times.Count);
        Assert.Equal(expectedMs, times.Zip(times.Skip(1), (a, b) => (int)(b - a).TotalMilliseconds));
        Assert.Equal(ExecutionState.Removed, execution.State);
    }

    [Fact]
    public async Task Lease_lost_after_ambiguous_error_finishes_send()
    {
        var calls = 0;
        _store.OnFail = (_, _, _) => Interlocked.Increment(ref calls) == 1
            ? throw new TimeoutException("commit state unknown")
            : Task.FromResult(FailOutcome.LeaseLost);
        var sink = Sink();
        var execution = Running();

        sink.Complete(execution, new JobOutcome.Fail(JobEventKind.Failed, "boom", "trace"));
        await Eventually.True(() => sink.BackoffWaits == 1, "first attempt failed");
        _time.Advance(100 * Ms);

        await Eventually.True(() => _table.Count == 0, "LeaseLost ends the send");
        Assert.Equal(2, _store.CallsOf("Fail").Count());
        Assert.Equal(ExecutionState.Removed, execution.State);
        Assert.Contains(_logs.At(LogLevel.Information), e => e.Message.Contains("probably already applied"));
    }

    [Fact]
    public async Task Lease_lost_without_error_is_logged_as_lost()
    {
        _store.OnRetry = (_, _, _) => Task.FromResult(FailOutcome.LeaseLost);
        var sink = Sink();
        var execution = Running();

        sink.Complete(execution, new JobOutcome.Retry(TimeSpan.FromSeconds(1), JobEventKind.Retry, "m", "x"));

        await Eventually.True(() => _table.Count == 0, "LeaseLost ends the send");
        Assert.Single(_store.CallsOf("Retry"));
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("lease was lost"));
    }

    [Fact]
    public async Task Exhausted_budget_drops_execution_from_table_and_next_heartbeat()
    {
        _options.LeaseDuration = TimeSpan.FromSeconds(3);
        _options.HeartbeatInterval = TimeSpan.FromSeconds(1);
        _options.AckBatchSize = 1;
        _store.OnAck = (_, _) => throw new IOException("db down");
        var sink = Sink();
        var execution = Running();
        var other = Running();

        sink.Complete(execution, new JobOutcome.Ack());
        int[] expectedMs = [100, 200, 400, 800, 1500];   // последняя пауза урезана до остатка бюджета 3 с
        for (var k = 1; k <= expectedMs.Length; k++)
        {
            await Eventually.True(() => sink.BackoffWaits == k, $"backoff wait #{k} is armed");
            _time.Advance(expectedMs[k - 1] * Ms);
        }

        // Abandon переводит в Removed, затем убирает из таблицы и последним пишет Error — ждём последний эффект.
        await Eventually.True(() => execution.State == ExecutionState.Removed
                && !_table.TryGet(execution.Job.LeaseId, out _)
                && _logs.At(LogLevel.Error).Any(e => e.Message.Contains(execution.Job.Id.ToString())),
            "budget of LeaseDuration is exhausted");
        Assert.Equal(6, _store.CallsOf("Ack").Count());
        Assert.False(_table.TryGet(execution.Job.LeaseId, out _));
        Assert.Contains(_logs.At(LogLevel.Error), e => e.Message.Contains(execution.Job.Id.ToString()));

        var keeper = new LeaseKeeper(new ServerRegistration(Guid.NewGuid(), "s", "h", ["q"], 1), _store, _table, _options, _clock, NullLogger.Instance, () => { }, _metrics);
        await keeper.HeartbeatOnceAsync(Ct);
        var heartbeat = (LeaseRef[])_store.CallsOf("Heartbeat").Last().Detail!;
        Assert.Equal(new[] { other.Lease }, heartbeat);
    }

    [Fact]
    public async Task Hung_send_is_cut_off_when_budget_ends_even_if_store_ignores_token()
    {
        _options.LeaseDuration = TimeSpan.FromSeconds(3);
        _options.HeartbeatInterval = TimeSpan.FromSeconds(1);
        var hung = new TaskCompletionSource<FailOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnFail = (_, _, _) => hung.Task;       // токен игнорируется: чёрная дыра TCP, Command Timeout=0
        var sink = Sink();
        var execution = Running();

        sink.Complete(execution, new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await Eventually.True(() => _store.CallsOf("Fail").Any(), "fail is sent");

        _time.Advance(TimeSpan.FromSeconds(3) - Ms);
        Assert.Equal(ExecutionState.OutcomeInFlight, execution.State);   // heartbeat пока держит lease
        _time.Advance(Ms);                               // бюджет LeaseDuration с первой попытки исчерпан

        // Abandon переводит в Removed, затем убирает из таблицы и последним пишет Error — ждём последний эффект.
        await Eventually.True(() => execution.State == ExecutionState.Removed
                && !_table.TryGet(execution.Job.LeaseId, out _)
                && _logs.At(LogLevel.Error).Any(e => e.Message.Contains(execution.Job.Id.ToString())),
            "hung attempt is cut off at the end of the budget");
        Assert.False(_table.TryGet(execution.Job.LeaseId, out _));
        Assert.Single(_store.CallsOf("Fail"));
        Assert.Contains(_logs.At(LogLevel.Error), e => e.Message.Contains(execution.Job.Id.ToString()));

        hung.SetResult(FailOutcome.Failed);             // поздний ответ брошенного вызова ничего не меняет
        Assert.Equal(ExecutionState.Removed, execution.State);
    }

    [Fact]
    public async Task Complete_on_removed_execution_returns_false_and_sends_nothing()
    {
        var sink = Sink();
        var execution = Running();
        Assert.True(execution.TryTransition(ExecutionState.Running, ExecutionState.Removed));   // брошено при остановке / LeaseLost
        _table.Remove(execution);

        Assert.False(sink.Complete(execution, new JobOutcome.Ack()));

        await sink.StopAsync(Ct);
        Assert.DoesNotContain(_store.Calls, c => c.Op is "Ack" or "Fail");
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("discarded"));
    }

    [Fact]
    public async Task Complete_after_stop_returns_false_and_removes_execution()
    {
        var sink = Sink();
        await sink.StopAsync(Ct);
        var execution = Running();

        Assert.False(sink.Complete(execution, new JobOutcome.Ack()));

        Assert.Equal(ExecutionState.Removed, execution.State);
        Assert.Equal(0, _table.Count);
        Assert.Empty(_store.Calls);
    }

    [Fact]
    public async Task Stop_sends_buffered_acks_immediately()
    {
        var sink = Sink();
        var a = Running();
        var b = Running();
        sink.Complete(a, new JobOutcome.Ack());
        sink.Complete(b, new JobOutcome.Ack());

        await sink.StopAsync(Ct);

        var ack = Assert.Single(_store.CallsOf("Ack"));
        Assert.Equal(new[] { a.Lease, b.Lease }, (LeaseRef[])ack.Detail!);
        Assert.Equal(0, _table.Count);
    }

    [Fact]
    public async Task Stop_waits_for_retrying_sends_within_budget()
    {
        var calls = 0;
        _store.OnFail = (_, _, _) => Interlocked.Increment(ref calls) == 1
            ? throw new IOException("network")
            : Task.FromResult(FailOutcome.Failed);
        var sink = Sink();
        var execution = Running();
        sink.Complete(execution, new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await Eventually.True(() => sink.BackoffWaits == 1, "first attempt failed");

        var stop = sink.StopAsync(Ct);
        Assert.False(stop.IsCompleted);
        _time.Advance(100 * Ms);
        await stop;

        Assert.Equal(2, _store.CallsOf("Fail").Count());
        Assert.Equal(0, _table.Count);
    }

    [Fact]
    public async Task Cancelled_stop_aborts_retries_and_drops_executions()
    {
        _store.OnFail = (_, _, _) => throw new IOException("db down");
        var sink = Sink();
        var execution = Running();
        sink.Complete(execution, new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await Eventually.True(() => sink.BackoffWaits == 1, "first attempt failed");

        using var cts = new CancellationTokenSource();
        var stop = sink.StopAsync(cts.Token);
        await cts.CancelAsync();
        await stop;

        // Abandon переводит в Removed до удаления из таблицы (на другом потоке) — ждём и удаление, и лог.
        await Eventually.True(() => execution.State == ExecutionState.Removed
                && _table.Count == 0
                && _logs.At(LogLevel.Warning).Any(e => e.Message.Contains(execution.Job.Id.ToString())),
            "aborted send drops the record");
        Assert.Equal(0, _table.Count);
        Assert.Single(_store.CallsOf("Fail"));
        Assert.False(sink.Complete(Running(), new JobOutcome.Ack()));
    }

    [Fact]
    public async Task Ack_buffered_while_stop_is_cancelled_is_discarded_with_log()
    {
        _store.OnFail = (_, _, _) => throw new IOException("db down");
        var sink = Sink();
        var failing = Running();
        sink.Complete(failing, new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await Eventually.True(() => sink.BackoffWaits == 1, "first attempt failed");

        using var cts = new CancellationTokenSource();
        var stop = sink.StopAsync(cts.Token);                          // буфер ack сброшен (пуст), ждёт отправку Fail
        Assert.False(stop.IsCompleted);
        var late = Running();
        Assert.True(sink.Complete(late, new JobOutcome.Ack()));        // sink ещё принимает: ack лёг в буфер после сброса
        await cts.CancelAsync();
        await stop;

        Assert.Equal(ExecutionState.Removed, late.State);
        Assert.False(_table.TryGet(late.Job.LeaseId, out _));
        await Eventually.True(() => _table.Count == 0, "aborted send leaves the table too");
        Assert.Empty(_store.CallsOf("Ack"));
        Assert.Contains(_logs.At(LogLevel.Warning),
            e => e.Message.Contains("discarded") && e.Message.Contains(late.Job.Id.ToString()));
    }

    [Fact]
    public async Task Each_outcome_calls_matching_store_method()
    {
        var sink = Sink();
        var retry = Running();
        var fail = Running();
        var cancel = Running();
        var release = Running();

        sink.Complete(retry, new JobOutcome.Retry(TimeSpan.FromSeconds(5), JobEventKind.NoExecutor, "r-msg", "r-exc"));
        sink.Complete(fail, new JobOutcome.Fail(JobEventKind.Failed, "f-msg", "f-exc"));
        sink.Complete(cancel, new JobOutcome.ConfirmCancel("c-msg"));
        sink.Complete(release, new JobOutcome.Release(Ran: true));

        await Eventually.True(() => _table.Count == 0, "all four outcomes are sent without batching");
        var r = Assert.Single(_store.CallsOf("Retry"));
        Assert.Equal((retry.Job.Id, retry.Job.LeaseId), (r.JobId!.Value, r.LeaseId!.Value));
        Assert.Equal((TimeSpan.FromSeconds(5), JobEventKind.NoExecutor, "r-msg"), r.Detail);
        var f = Assert.Single(_store.CallsOf("Fail"));
        Assert.Equal((fail.Job.Id, fail.Job.LeaseId), (f.JobId!.Value, f.LeaseId!.Value));
        Assert.Equal((JobEventKind.Failed, "f-msg"), f.Detail);
        var c = Assert.Single(_store.CallsOf("ConfirmCancel"));
        Assert.Equal((cancel.Job.Id, cancel.Job.LeaseId, (object?)"c-msg"), (c.JobId!.Value, c.LeaseId!.Value, c.Detail));
        var rel = Assert.Single(_store.CallsOf("Release"));
        Assert.Equal((release.Job.Id, release.Job.LeaseId, (object?)true), (rel.JobId!.Value, rel.LeaseId!.Value, rel.Detail));
        Assert.Empty(_store.CallsOf("Ack"));
    }

    [Fact]
    public async Task Registered_release_is_sent_with_ran_false()
    {
        var sink = Sink();
        var execution = new Execution(Leased.Job(), "q", _clock.Add(_clock.Now, _options.LeaseDuration), ExecutionState.OutcomePending)
        {
            Outcome = new JobOutcome.Release(Ran: false),
        };
        _table.Add(execution);

        sink.EnqueueRegistered(execution);

        await Eventually.True(() => _table.Count == 0, "release is sent");
        Assert.Equal((object?)false, Assert.Single(_store.CallsOf("Release")).Detail);
    }

    [Fact]
    public async Task Registered_release_after_stop_is_dropped_not_sent()
    {
        var sink = Sink();
        await sink.StopAsync(Ct);
        var execution = new Execution(Leased.Job(), "q", _clock.Add(_clock.Now, _options.LeaseDuration), ExecutionState.OutcomePending)
        {
            Outcome = new JobOutcome.Release(Ran: false),
        };
        _table.Add(execution);

        sink.EnqueueRegistered(execution);

        Assert.Equal(ExecutionState.Removed, execution.State);
        Assert.Equal(0, _table.Count);
        Assert.Empty(_store.Calls);
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("discarded"));
    }

    [Fact]
    public async Task Ack_batch_skips_records_removed_by_heartbeat_and_logs_partial_ack()
    {
        _options.AckBatchSize = 3;
        _store.OnAck = (leases, _) => Task.FromResult<IReadOnlyList<Guid>>([.. leases.Skip(1).Select(l => l.JobId)]);
        var sink = Sink();
        var removed = Running();
        var a = Running();
        var b = Running();

        sink.Complete(removed, new JobOutcome.Ack());
        Assert.True(removed.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed));   // heartbeat: lease потерян
        _table.Remove(removed);
        sink.Complete(a, new JobOutcome.Ack());
        sink.Complete(b, new JobOutcome.Ack());

        await Eventually.True(() => _table.Count == 0, "batch is acked");
        Assert.Equal(new[] { a.Lease, b.Lease }, (LeaseRef[])Assert.Single(_store.CallsOf("Ack")).Detail!);
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("affected 1 of 2"));
    }

    [Fact]
    public async Task Concurrent_complete_and_heartbeat_removal_have_exactly_one_winner()
    {
        _options.AckBatchSize = 7;
        var sink = Sink();
        var executions = Enumerable.Range(0, 500).Select(_ => Running()).ToArray();
        var removedByHeartbeat = new System.Collections.Concurrent.ConcurrentBag<Execution>();
        var accepted = new System.Collections.Concurrent.ConcurrentBag<Execution>();
        using var start = new Barrier(2);

        var runner = Task.Run(() =>
        {
            start.SignalAndWait(Ct);
            foreach (var e in executions)
                if (sink.Complete(e, new JobOutcome.Ack()))
                    accepted.Add(e);
        }, Ct);
        var heartbeat = Task.Run(() =>
        {
            start.SignalAndWait(Ct);
            foreach (var e in executions.Reverse())
                if (e.TryTransition(ExecutionState.Running, ExecutionState.Removed)
                    || e.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed))
                {
                    _table.Remove(e);
                    removedByHeartbeat.Add(e);
                }
        }, Ct);
        await Task.WhenAll(runner, heartbeat);
        await sink.StopAsync(Ct);

        var acked = _store.CallsOf("Ack").SelectMany(c => (LeaseRef[])c.Detail!).ToHashSet();
        var removed = removedByHeartbeat.Select(e => e.Lease).ToHashSet();
        Assert.Empty(acked.Intersect(removed));
        Assert.Equal(executions.Length, acked.Count + removed.Count);
        Assert.Subset(accepted.Select(e => e.Lease).ToHashSet(), acked);
        Assert.All(executions, e => Assert.Equal(ExecutionState.Removed, e.State));
        Assert.Equal(0, _table.Count);
    }

    [Fact]
    public async Task Ack_counts_only_returned_ids_per_queue()
    {
        var a1 = Running("a");
        var a2 = Running("a");
        var b = Running("b");
        _store.OnAck = (_, _) => Task.FromResult<IReadOnlyList<Guid>>([a1.Job.Id, b.Job.Id]);
        var sink = Sink();

        foreach (var execution in new[] { a1, a2, b })
            sink.Complete(execution, new JobOutcome.Ack());
        await sink.StopAsync(Ct);

        Assert.Equal(
            [new OutcomeStatsRow(Bucket(), _serverId, "a", 1, 0, 0), new OutcomeStatsRow(Bucket(), _serverId, "b", 1, 0, 0)],
            _stats.Take());
    }

    [Theory]
    [InlineData(false, FailOutcome.Retried, 0, 1)]
    [InlineData(false, FailOutcome.Failed, 1, 0)]
    [InlineData(false, FailOutcome.Cancelled, 0, 0)]
    [InlineData(false, FailOutcome.LeaseLost, 0, 0)]
    [InlineData(true, FailOutcome.Retried, 0, 1)]
    [InlineData(true, FailOutcome.Failed, 1, 0)]
    [InlineData(true, FailOutcome.Cancelled, 0, 0)]
    [InlineData(true, FailOutcome.LeaseLost, 0, 0)]
    public async Task Retry_and_fail_are_counted_by_store_outcome(bool fail, FailOutcome answer, int failed, int retried)
    {
        _store.OnRetry = (_, _, _) => Task.FromResult(answer);
        _store.OnFail = (_, _, _) => Task.FromResult(answer);
        var sink = Sink();

        sink.Complete(Running(), fail
            ? new JobOutcome.Fail(JobEventKind.Failed, "m", "x")
            : new JobOutcome.Retry(TimeSpan.FromSeconds(1), JobEventKind.Retry, "m", "x"));
        await sink.StopAsync(Ct);

        var rows = _stats.Take();
        if (failed + retried == 0)
            Assert.Empty(rows);
        else
            Assert.Equal([new OutcomeStatsRow(Bucket(), _serverId, "q", 0, failed, retried)], rows);
    }

    [Fact]
    public async Task Confirm_cancel_and_release_are_not_counted()
    {
        var sink = Sink();

        sink.Complete(Running(), new JobOutcome.ConfirmCancel("c"));
        sink.Complete(Running(), new JobOutcome.Release(Ran: true));
        await sink.StopAsync(Ct);

        Assert.Equal(2, _store.Calls.Count);
        Assert.Empty(_stats.Take());
    }

    [Fact]
    public async Task Bucket_is_minute_of_confirmation()
    {
        _time.SetUtcNow(new DateTimeOffset(2026, 10, 5, 12, 0, 50, TimeSpan.Zero));
        _options.AckBatchSize = 1;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnAck = async (leases, _) =>
        {
            await gate.Task;
            return [.. leases.Select(l => l.JobId)];
        };
        var sink = Sink();

        sink.Complete(Running(), new JobOutcome.Ack());
        await Eventually.True(() => _store.CallsOf("Ack").Any(), "ack is sent at 12:00:50");
        _time.Advance(TimeSpan.FromSeconds(20));
        gate.SetResult();
        await sink.StopAsync(Ct);

        Assert.Equal(new DateTime(2026, 10, 5, 12, 1, 0, DateTimeKind.Utc), Assert.Single(_stats.Take()).BucketUtc);
    }

    [Fact]
    public async Task Ambiguous_retry_after_failed_attempt_counts_only_store_answer()
    {
        _options.AckBatchSize = 1;
        var calls = 0;
        _store.OnAck = (_, _) => Interlocked.Increment(ref calls) == 1
            ? throw new IOException("commit state unknown")
            : Task.FromResult<IReadOnlyList<Guid>>([]);
        var sink = Sink();

        sink.Complete(Running(), new JobOutcome.Ack());
        await Eventually.True(() => sink.BackoffWaits == 1, "first attempt failed");
        _time.Advance(100 * Ms);
        await Eventually.True(() => _table.Count == 0, "second attempt finishes the send");

        Assert.Equal(2, _store.CallsOf("Ack").Count());
        Assert.Empty(_stats.Take());
    }

    [Fact]
    public async Task On_lease_freed_after_each_outcome_statement()
    {
        _options.AckBatchSize = 3;
        var freed = new ConcurrentQueue<string>();
        var ackGate = new TaskCompletionSource<IReadOnlyList<Guid>>(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnAck = (_, _) => ackGate.Task;
        var lost = Running("lost");
        _store.OnFail = (jobId, _, _) => Task.FromResult(jobId == lost.Job.Id ? FailOutcome.LeaseLost : FailOutcome.Failed);
        var sink = Sink(freed.Enqueue);
        var acks = new[] { Running("a"), Running("b"), Running("a") };

        foreach (var execution in acks)
            sink.Complete(execution, new JobOutcome.Ack());
        await Eventually.True(() => _store.CallsOf("Ack").Any(), "ack batch is sent");
        Assert.Empty(freed);                                   // statement ещё не вернулся
        ackGate.SetResult([.. acks.Select(e => e.Job.Id)]);
        await Eventually.True(() => freed.Count == 3, "each record of the batch frees its lease");
        Assert.Equal(["a", "a", "b"], freed.Order(StringComparer.Ordinal));

        sink.Complete(Running("retry"), new JobOutcome.Retry(TimeSpan.FromSeconds(1), JobEventKind.Retry, "m", "x"));
        sink.Complete(Running("fail"), new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        sink.Complete(Running("cancel"), new JobOutcome.ConfirmCancel("c"));
        sink.Complete(Running("release"), new JobOutcome.Release(Ran: true));
        sink.Complete(lost, new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await sink.StopAsync(Ct);

        Assert.Equal(["a", "a", "b", "cancel", "fail", "lost", "release", "retry"], freed.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task On_lease_freed_is_not_called_for_abandoned()
    {
        _options.LeaseDuration = TimeSpan.FromSeconds(3);
        _options.HeartbeatInterval = TimeSpan.FromSeconds(1);
        var freed = new ConcurrentQueue<string>();
        _store.OnFail = (_, _, _) => throw new IOException("db down");
        var sink = Sink(freed.Enqueue);

        var exhausted = Running();
        sink.Complete(exhausted, new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        int[] backoffMs = [100, 200, 400, 800, 1500];
        for (var k = 1; k <= backoffMs.Length; k++)
        {
            await Eventually.True(() => sink.BackoffWaits == k, $"backoff wait #{k} is armed");
            _time.Advance(backoffMs[k - 1] * Ms);
        }
        await Eventually.True(() => _logs.At(LogLevel.Error).Any(e => e.Message.Contains(exhausted.Job.Id.ToString())),
            "budget of LeaseDuration is exhausted");

        var aborted = Running();
        sink.Complete(aborted, new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await Eventually.True(() => sink.BackoffWaits == backoffMs.Length + 1, "attempt failed");
        using var cts = new CancellationTokenSource();
        var stop = sink.StopAsync(cts.Token);
        await cts.CancelAsync();
        await stop;
        await Eventually.True(() => _logs.At(LogLevel.Warning).Any(e => e.Message.Contains(aborted.Job.Id.ToString())),
            "aborted send drops the record");

        Assert.Empty(freed);
    }

    [Fact]
    public async Task On_lease_freed_failure_is_logged_and_the_rest_of_the_batch_finishes()
    {
        _options.AckBatchSize = 2;
        var calls = 0;
        var sink = Sink(_ =>
        {
            Interlocked.Increment(ref calls);
            throw new InvalidOperationException("callback failed");
        });
        var a = Running();
        var b = Running();

        sink.Complete(a, new JobOutcome.Ack());
        sink.Complete(b, new JobOutcome.Ack());

        await Eventually.True(() => _table.Count == 0, "both records leave the table");
        await sink.StopAsync(Ct);
        Assert.Equal(2, calls);
        Assert.Single(_store.CallsOf("Ack"));                 // сбой колбэка — не сбой отправки
        Assert.Equal(2, _logs.At(LogLevel.Error).Count(e => e.Exception is InvalidOperationException));
    }

    // ---- метрики потерянных исходов и ошибок хранилища ------------------------------------

    [Fact]
    public async Task Lease_lost_answer_counts_dropped()
    {
        _store.OnRetry = (_, _, _) => Task.FromResult(FailOutcome.LeaseLost);
        var sink = Sink();

        sink.Complete(Running(), new JobOutcome.Retry(TimeSpan.FromSeconds(1), JobEventKind.Retry, "m", "x"));
        await sink.StopAsync(Ct);

        var m = Assert.Single(_dropped.GetMeasurementSnapshot());
        Assert.Equal((1L, "q", DropReasons.LeaseLost), (m.Value, m.Tags[Attr.Queue], m.Tags[Attr.DropReason]));
    }

    [Fact]
    public async Task Ack_partial_counts_lease_lost_per_queue()
    {
        var a1 = Running("a");
        var a2 = Running("a");
        var a3 = Running("a");
        var b = Running("b");
        _store.OnAck = (_, _) => Task.FromResult<IReadOnlyList<Guid>>([a1.Job.Id]);
        var sink = Sink();

        foreach (var execution in new[] { a1, a2, a3, b })
            sink.Complete(execution, new JobOutcome.Ack());
        await sink.StopAsync(Ct);

        var lost = _dropped.GetMeasurementSnapshot()
            .Select(m => (m.Value, (string)m.Tags[Attr.Queue]!, (string)m.Tags[Attr.DropReason]!))
            .OrderBy(m => m.Item2, StringComparer.Ordinal);
        Assert.Equal([(2L, "a", DropReasons.LeaseLost), (1L, "b", DropReasons.LeaseLost)], lost);
    }

    [Fact]
    public async Task Retry_attempt_lease_lost_is_not_counted()
    {
        var calls = 0;
        _store.OnFail = (_, _, _) => Interlocked.Increment(ref calls) == 1
            ? throw new TimeoutException("commit state unknown")
            : Task.FromResult(FailOutcome.LeaseLost);
        var sink = Sink();

        sink.Complete(Running(), new JobOutcome.Fail(JobEventKind.Failed, "boom", "trace"));
        await Eventually.True(() => sink.BackoffWaits == 1, "first attempt failed");
        _time.Advance(100 * Ms);
        await Eventually.True(() => _table.Count == 0, "LeaseLost ends the send");

        Assert.Empty(_dropped.GetMeasurementSnapshot());
    }

    [Fact]
    public async Task No_double_count_after_lease_lost()
    {
        _options.AckFlushInterval = TimeSpan.FromSeconds(1);
        var sink = Sink();
        var execution = Running();
        Assert.True(sink.Complete(execution, new JobOutcome.Ack()));   // OutcomePending в буфере ack
        _store.OnHeartbeat = (_, _) => Task.FromResult<IReadOnlyList<HeartbeatLease>>([]);
        var keeper = new LeaseKeeper(new ServerRegistration(Guid.NewGuid(), "s", "h", ["q"], 1), _store, _table, _options, _clock,
            NullLogger.Instance, () => { }, _metrics);

        await keeper.HeartbeatOnceAsync(Ct);                           // OutcomePending → Removed: lease потерян
        Assert.False(sink.Complete(execution, new JobOutcome.Ack()));   // Complete над Removed
        _time.Advance(_options.AckFlushInterval);                       // BeginAttempt над Removed
        await sink.StopAsync(Ct);

        Assert.Empty(_store.CallsOf("Ack"));
        Assert.Equal(1, Dropped(DropReasons.LeaseLost));
        Assert.Single(_dropped.GetMeasurementSnapshot());
    }

    [Fact]
    public async Task Complete_after_stop_counts_stopped()
    {
        var sink = Sink();
        await sink.StopAsync(Ct);

        sink.Complete(Running(), new JobOutcome.Ack());

        Assert.Equal(1, Dropped(DropReasons.Stopped));
    }

    [Fact]
    public async Task Registered_release_after_stop_counts_stopped()
    {
        var sink = Sink();
        await sink.StopAsync(Ct);
        var execution = new Execution(Leased.Job(), "q", _clock.Add(_clock.Now, _options.LeaseDuration), ExecutionState.OutcomePending)
        {
            Outcome = new JobOutcome.Release(Ran: false),
        };
        _table.Add(execution);

        sink.EnqueueRegistered(execution);

        Assert.Equal(1, Dropped(DropReasons.Stopped));
    }

    [Fact]
    public async Task Cancelled_stop_counts_discarded_and_aborted_as_stopped()
    {
        _store.OnFail = (_, _, _) => throw new IOException("db down");
        var sink = Sink();
        var failing = Running();
        sink.Complete(failing, new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await Eventually.True(() => sink.BackoffWaits == 1, "first attempt failed");

        using var cts = new CancellationTokenSource();
        var stop = sink.StopAsync(cts.Token);
        Assert.True(sink.Complete(Running(), new JobOutcome.Ack()));   // ack-буфер отменённой остановки
        await cts.CancelAsync();
        await stop;

        // Abandon прерванной отправки идёт на другом потоке: ждём её запись.
        await Eventually.True(() => Dropped(DropReasons.Stopped) == 2, "discarded ack and aborted send are counted");
        Assert.Equal(0, Dropped(DropReasons.SendTimeout));
        Assert.Equal(1, Errors(StorageOperations.Outcome));   // только ошибка первой попытки
    }

    [Fact]
    public async Task Exhausted_budget_counts_send_timeout()
    {
        _options.LeaseDuration = TimeSpan.FromSeconds(3);
        _options.HeartbeatInterval = TimeSpan.FromSeconds(1);
        _options.AckBatchSize = 1;
        _store.OnAck = (_, _) => throw new IOException("db down");
        var sink = Sink();

        sink.Complete(Running(), new JobOutcome.Ack());
        int[] expectedMs = [100, 200, 400, 800, 1500];
        for (var k = 1; k <= expectedMs.Length; k++)
        {
            await Eventually.True(() => sink.BackoffWaits == k, $"backoff wait #{k} is armed");
            _time.Advance(expectedMs[k - 1] * Ms);
        }

        await Eventually.True(() => Dropped(DropReasons.SendTimeout) == 1, "budget of LeaseDuration is exhausted");
        Assert.Equal(6, Errors(StorageOperations.Ack));
        Assert.Single(_dropped.GetMeasurementSnapshot());
    }

    [Fact]
    public async Task Failed_outcome_attempt_counts_storage_error()
    {
        var calls = 0;
        _store.OnRetry = (_, _, _) => Interlocked.Increment(ref calls) == 1
            ? throw new IOException("network")
            : Task.FromResult(FailOutcome.Retried);
        var sink = Sink();

        sink.Complete(Running(), new JobOutcome.Retry(TimeSpan.FromSeconds(1), JobEventKind.Retry, "m", "x"));
        await Eventually.True(() => sink.BackoffWaits == 1, "first attempt failed");
        _time.Advance(100 * Ms);
        await sink.StopAsync(Ct);

        var m = Assert.Single(_errors.GetMeasurementSnapshot());
        Assert.Equal((1L, StorageOperations.Outcome), (m.Value, m.Tags[Attr.StorageOperation]));
    }

    [Fact]
    public async Task Failed_ack_attempt_counts_storage_error()
    {
        var calls = 0;
        _store.OnAck = (leases, _) => Interlocked.Increment(ref calls) == 1 ? throw new IOException("network") : FakeServerStore.AckAll(leases);
        var sink = Sink();

        sink.Complete(Running(), new JobOutcome.Ack());
        _time.Advance(_options.AckFlushInterval);
        await Eventually.True(() => sink.BackoffWaits == 1, "first attempt failed");
        _time.Advance(100 * Ms);
        await sink.StopAsync(Ct);

        var m = Assert.Single(_errors.GetMeasurementSnapshot());
        Assert.Equal((1L, StorageOperations.Ack), (m.Value, m.Tags[Attr.StorageOperation]));
    }

    [Fact]
    public async Task Attempt_cut_off_by_budget_counts_storage_error()
    {
        _options.LeaseDuration = TimeSpan.FromSeconds(3);
        _options.HeartbeatInterval = TimeSpan.FromSeconds(1);
        var hung = new TaskCompletionSource<FailOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnFail = (_, _, _) => hung.Task;
        var sink = Sink();

        sink.Complete(Running(), new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await Eventually.True(() => _store.CallsOf("Fail").Any(), "fail is sent");
        _time.Advance(TimeSpan.FromSeconds(3));

        await Eventually.True(() => Dropped(DropReasons.SendTimeout) == 1, "hung attempt is cut off at the end of the budget");
        Assert.Equal(1, Errors(StorageOperations.Outcome));
        hung.SetResult(FailOutcome.Failed);
    }

    [Fact]
    public async Task Attempt_aborted_by_cancelled_stop_is_not_storage_error()
    {
        var hung = new TaskCompletionSource<FailOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnFail = (_, _, _) => hung.Task;   // токен игнорируется
        var sink = Sink();
        sink.Complete(Running(), new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await Eventually.True(() => _store.CallsOf("Fail").Any(), "fail is sent");

        using var cts = new CancellationTokenSource();
        var stop = sink.StopAsync(cts.Token);
        await cts.CancelAsync();
        await stop;

        await Eventually.True(() => Dropped(DropReasons.Stopped) == 1, "aborted send drops the record");
        Assert.Empty(_errors.GetMeasurementSnapshot());
        hung.SetResult(FailOutcome.Failed);
    }

    [Fact]
    public async Task Throwing_logger_after_successful_ack_is_not_a_storage_error()
    {
        _options.AckBatchSize = 2;
        var freed = new ConcurrentQueue<string>();
        var kept = Running();
        var lost = Running();
        _store.OnAck = (_, _) => Task.FromResult<IReadOnlyList<Guid>>([kept.Job.Id]);
        _logs.OnEntry = e =>
        {
            if (e.EventId.Name == "AckPartialLeaseLost")
                throw new InvalidOperationException("logger down");
        };
        var sink = Sink(freed.Enqueue);

        sink.Complete(kept, new JobOutcome.Ack());
        sink.Complete(lost, new JobOutcome.Ack());
        await Eventually.True(() => _table.Count == 0, "the answer is applied and both records leave the table despite the logger");
        await sink.StopAsync(Ct);

        Assert.Equal(0, Errors(StorageOperations.Ack));
        Assert.Single(_store.CallsOf("Ack"));
        Assert.Equal(2, freed.Count);
        Assert.Equal(1, Dropped(DropReasons.LeaseLost));
    }

    [Fact]
    public async Task Throwing_logger_after_discarded_outcome_is_not_a_storage_error()
    {
        var freed = new ConcurrentQueue<string>();
        _store.OnFail = (_, _, _) => Task.FromResult(FailOutcome.LeaseLost);
        _logs.OnEntry = e =>
        {
            if (e.EventId.Name == "OutcomeDiscardedLeaseLost")
                throw new InvalidOperationException("logger down");
        };
        var sink = Sink(freed.Enqueue);

        sink.Complete(Running(), new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await Eventually.True(() => _table.Count == 0, "the answer is applied and the record leaves the table despite the logger");
        await sink.StopAsync(Ct);

        Assert.Equal(0, Errors(StorageOperations.Outcome));
        Assert.Single(_store.CallsOf("Fail"));
        Assert.Single(freed);
        Assert.Equal(1, Dropped(DropReasons.LeaseLost));
    }

    [Fact]
    public async Task Drop_reason_and_log_agree_when_abort_changes()
    {
        _options.LeaseDuration = TimeSpan.FromSeconds(3);
        _options.HeartbeatInterval = TimeSpan.FromSeconds(1);
        var hung = new TaskCompletionSource<FailOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnFail = (_, _, _) => hung.Task;
        var sink = Sink();
        sink.Complete(Running(), new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await Eventually.True(() => _store.CallsOf("Fail").Any(), "fail is sent");
        using var cts = new CancellationTokenSource();
        var stop = sink.StopAsync(cts.Token);
        // Остановка отменяется синхронно на измерении причины сброса — после выбора причины и до выбора лога:
        // причина и лог должны совпасть.
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter == _instrumentation.Meter && instrument.Name == I.JobOutcomeDropped)
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, _, _, _) => cts.Cancel());
        listener.Start();

        _time.Advance(TimeSpan.FromSeconds(3));
        await stop;

        await Eventually.True(() => Dropped(DropReasons.SendTimeout) == 1, "the budget is exhausted before the stop");
        await Eventually.True(() => _logs.Entries.Any(e => e.EventId.Name is "OutcomeSendBudgetExceeded" or "OutcomeNotSentStopCancelled"),
            "the abandoned send is logged");
        Assert.Contains(_logs.Entries, e => e.EventId.Name == "OutcomeSendBudgetExceeded");
        Assert.DoesNotContain(_logs.Entries, e => e.EventId.Name == "OutcomeNotSentStopCancelled");
        hung.SetResult(FailOutcome.Failed);
    }
}
