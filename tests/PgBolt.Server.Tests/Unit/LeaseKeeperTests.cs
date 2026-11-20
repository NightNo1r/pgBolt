using System.Collections.Concurrent;
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

public sealed class LeaseKeeperTests : IDisposable
{
    private static readonly TimeSpan H = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan L = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);
    private static readonly Guid ServerId = Guid.CreateVersion7();
    private static readonly ServerRegistration Registration = new(ServerId, "s", "h", ["q"], 1);
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly FakeTimeProvider _time = new();
    private readonly MonoClock _clock;
    private readonly FakeServerStore _store = new();
    private readonly ExecutionTable _table = new();
    private readonly ListLogger _logger = new();
    private readonly PgBoltInstrumentation _instrumentation = TestTelemetry.Instrumentation();
    private readonly PgBoltMetrics _metrics;
    private readonly MetricCollector<long> _dropped;
    private readonly MetricCollector<long> _errors;
    private int _recovered;

    public LeaseKeeperTests()
    {
        _clock = new MonoClock(_time);
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

    private long HeartbeatErrors => TestTelemetry.Sum(_errors, Attr.StorageOperation, StorageOperations.Heartbeat);

    /// <summary>Jitter = 0.5 → шаг расписания ровно HeartbeatInterval.</summary>
    private LeaseKeeper Create(TimeSpan? lease = null, TimeSpan? interval = null, double jitter = 0.5) =>
        new(Registration, _store, _table, new PgBoltServerOptions { LeaseDuration = lease ?? L, HeartbeatInterval = interval ?? H },
            _clock, _logger, () => Interlocked.Increment(ref _recovered), _metrics)
        {
            Jitter = () => jitter,
        };

    private long FromNow(TimeSpan duration) => _clock.Add(_clock.Now, duration);

    private void AdvanceTo(long timestamp) => _time.Advance(_clock.Until(timestamp));

    private int HeartbeatCalls => _store.CallsOf("Heartbeat").Count();

    private static Task<IReadOnlyList<HeartbeatLease>> DbDown() =>
        Task.FromException<IReadOnlyList<HeartbeatLease>>(new TimeoutException("db down"));

    private static TaskCompletionSource<IReadOnlyList<HeartbeatLease>> NewCall() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static IReadOnlyList<HeartbeatLease> Rows(params (Execution Execution, bool CancelRequested)[] rows) =>
        [.. rows.Select(r => new HeartbeatLease(r.Execution.Lease.JobId, r.Execution.Lease.LeaseId, r.CancelRequested))];

    private void Respond(IReadOnlyList<HeartbeatLease> rows) =>
        _store.OnHeartbeat = (_, _) => Task.FromResult(rows);

    // ---- регистрация и снимок --------------------------------------------------------------

    [Fact]
    public void Register_adds_running_execution_with_given_valid_until()
    {
        var keeper = Create();
        var job = Leased.Job();
        var validUntil = FromNow(L);

        var e = keeper.Register(job, "q", validUntil);

        Assert.Equal(ExecutionState.Running, e.State);
        Assert.Equal(validUntil, e.ValidUntil);
        Assert.Equal("q", e.Queue);
        Assert.True(_table.TryGet(job.LeaseId, out var found));
        Assert.Same(e, found);
    }

    [Fact]
    public void Register_for_release_adds_pending_release_outcome()
    {
        var keeper = Create();

        var e = keeper.RegisterForRelease(Leased.Job(), "q", FromNow(L));

        Assert.Equal(ExecutionState.OutcomePending, e.State);
        Assert.Equal(new JobOutcome.Release(false), e.Outcome);
        Assert.Equal(1, _table.Count);
    }

    [Fact]
    public async Task Heartbeat_sends_only_leases_of_live_table_entries()
    {
        var keeper = Create();
        var running = keeper.Register(Leased.Job(), "q", FromNow(L));
        var release = keeper.RegisterForRelease(Leased.Job(), "q", FromNow(L));
        var abandoned = keeper.Register(Leased.Job(), "q", FromNow(L));
        Assert.True(abandoned.TryTransition(ExecutionState.Running, ExecutionState.Removed));   // ещё в таблице, но Removed

        await keeper.HeartbeatOnceAsync(Ct);

        var sent = (LeaseRef[])Assert.Single(_store.CallsOf("Heartbeat")).Detail!;
        Assert.Equal(new[] { running.Lease, release.Lease }.OrderBy(l => l.LeaseId), sent.OrderBy(l => l.LeaseId));
    }

    [Fact]
    public async Task Execution_registered_after_snapshot_is_not_treated_as_lost()
    {
        var keeper = Create();
        var before = keeper.Register(Leased.Job(), "q", FromNow(L));
        var call = NewCall();
        _store.OnHeartbeat = (_, _) => call.Task;

        var heartbeat = keeper.HeartbeatOnceAsync(Ct);
        var afterValidUntil = FromNow(L);
        var after = keeper.Register(Leased.Job(), "q", afterValidUntil);   // dequeue закоммичен после снимка
        call.SetResult(Rows((before, false)));
        await heartbeat;

        Assert.Equal([before.Lease], (LeaseRef[])Assert.Single(_store.CallsOf("Heartbeat")).Detail!);
        Assert.Equal(ExecutionState.Running, after.State);
        Assert.Equal(CancelReason.None, after.FirstReason);
        Assert.False(after.Token.IsCancellationRequested);
        Assert.Equal(afterValidUntil, after.ValidUntil);
        Assert.True(_table.TryGet(after.Lease.LeaseId, out _));
    }

    // ---- ответ heartbeat ------------------------------------------------------------------

    [Fact]
    public async Task Missing_running_lease_is_removed_and_cancelled_as_lease_lost()
    {
        var keeper = Create();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        Respond([]);

        await keeper.HeartbeatOnceAsync(Ct);

        Assert.Equal(ExecutionState.Removed, e.State);
        Assert.Equal(CancelReason.LeaseLost, e.FirstReason);
        Assert.True(e.Token.IsCancellationRequested);
        Assert.False(_table.TryGet(e.Lease.LeaseId, out _));
        Assert.Contains(_logger.At(LogLevel.Warning), l => l.Message.Contains(e.Lease.LeaseId.ToString(), StringComparison.Ordinal));
    }

    [Fact]
    public async Task Missing_pending_outcome_is_dropped()
    {
        var keeper = Create();
        var completed = keeper.Register(Leased.Job(), "q", FromNow(L));
        completed.Outcome = new JobOutcome.Ack();
        Assert.True(completed.TryTransition(ExecutionState.Running, ExecutionState.OutcomePending));
        var notStarted = keeper.RegisterForRelease(Leased.Job(), "q", FromNow(L));
        Respond([]);

        await keeper.HeartbeatOnceAsync(Ct);

        foreach (var e in new[] { completed, notStarted })
        {
            Assert.Equal(ExecutionState.Removed, e.State);
            Assert.Equal(CancelReason.None, e.FirstReason);
            Assert.False(_table.TryGet(e.Lease.LeaseId, out _));
        }
        Assert.Equal(0, _table.Count);
    }

    [Fact]
    public async Task Missing_lease_with_outcome_in_flight_is_left_to_the_sink()
    {
        var keeper = Create();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        e.Outcome = new JobOutcome.Ack();
        Assert.True(e.TryTransition(ExecutionState.Running, ExecutionState.OutcomePending));
        Assert.True(e.TryTransition(ExecutionState.OutcomePending, ExecutionState.OutcomeInFlight));
        Respond([]);

        await keeper.HeartbeatOnceAsync(Ct);

        Assert.Equal(ExecutionState.OutcomeInFlight, e.State);
        Assert.Equal(CancelReason.None, e.FirstReason);
        Assert.True(_table.TryGet(e.Lease.LeaseId, out _));
    }

    [Fact]
    public async Task Lease_already_removed_by_the_sink_is_not_reported_again()
    {
        var keeper = Create();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        var call = NewCall();
        _store.OnHeartbeat = (_, _) => call.Task;

        var heartbeat = keeper.HeartbeatOnceAsync(Ct);
        e.Outcome = new JobOutcome.Ack();                    // ack того же сервера удалил строку раньше heartbeat
        Assert.True(e.TryTransition(ExecutionState.Running, ExecutionState.OutcomePending));
        Assert.True(e.TryTransition(ExecutionState.OutcomePending, ExecutionState.OutcomeInFlight));
        Assert.True(e.TryTransition(ExecutionState.OutcomeInFlight, ExecutionState.Removed));
        Assert.True(_table.Remove(e));
        call.SetResult([]);
        await heartbeat;

        Assert.Equal(ExecutionState.Removed, e.State);
        Assert.Equal(CancelReason.None, e.FirstReason);
        Assert.Empty(_logger.At(LogLevel.Warning));
    }

    [Fact]
    public async Task Cancel_request_cancels_the_execution_once()
    {
        var keeper = Create();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        Respond(Rows((e, true)));

        await keeper.HeartbeatOnceAsync(Ct);
        await keeper.HeartbeatOnceAsync(Ct);

        Assert.Equal(CancelReason.CancelRequested, e.FirstReason);
        Assert.True(e.Token.IsCancellationRequested);
        Assert.Equal(ExecutionState.Running, e.State);          // исход задачи сдаст runner
        Assert.True(_table.TryGet(e.Lease.LeaseId, out _));
        Assert.Single(_logger.At(LogLevel.Information));
    }

    [Fact]
    public async Task Valid_until_grows_to_sent_at_plus_lease_only_for_returned_leases()
    {
        var keeper = Create();
        var fetchStartedAt = _clock.Now;
        _time.Advance(TimeSpan.FromSeconds(2));                         // dequeue длился 2 с
        var renewed = keeper.Register(Leased.Job(), "q", _clock.Add(fetchStartedAt, L));
        var farAhead = keeper.Register(Leased.Job(), "q", FromNow(L * 2));
        Assert.Equal(_clock.Add(fetchStartedAt, L), renewed.ValidUntil);

        _time.Advance(TimeSpan.FromSeconds(10));
        var sentAt = _clock.Now;
        var call = NewCall();
        _store.OnHeartbeat = (_, _) => call.Task;
        var heartbeat = keeper.HeartbeatOnceAsync(Ct);
        var lateValidUntil = _clock.Add(sentAt, L - TimeSpan.FromSeconds(1));
        var late = keeper.Register(Leased.Job(), "q", lateValidUntil); // после снимка: в ответе его нет
        _time.Advance(TimeSpan.FromSeconds(1));                         // ответ пришёл через 1 с
        call.SetResult(Rows((renewed, false), (farAhead, false)));
        await heartbeat;

        Assert.Equal(_clock.Add(sentAt, L), renewed.ValidUntil);        // от sentAt, не от ответа
        Assert.Equal(_clock.Add(fetchStartedAt, L * 2 + TimeSpan.FromSeconds(2)), farAhead.ValidUntil);   // не уменьшается
        Assert.Equal(lateValidUntil, late.ValidUntil);
    }

    // ---- таймаут вызова и цикл ------------------------------------------------------------

    [Fact]
    public async Task Heartbeat_call_is_aborted_after_half_the_interval()
    {
        var keeper = Create();
        CancellationToken seen = default;
        _store.OnHeartbeat = (_, ct) =>
        {
            seen = ct;
            var call = NewCall();
            ct.Register(() => call.TrySetCanceled(ct));
            return call.Task;
        };

        var heartbeat = keeper.HeartbeatOnceAsync(Ct);
        _time.Advance(H / 2 - Tick);
        Assert.False(seen.IsCancellationRequested);
        _time.Advance(Tick);
        Assert.True(seen.IsCancellationRequested);
        await heartbeat;                                                 // не бросает

        Assert.Contains(_logger.At(LogLevel.Warning), l => l.Message.Contains("timed out", StringComparison.Ordinal));
        Assert.False(keeper.HeartbeatLost);
    }

    [Fact]
    public async Task Failed_heartbeat_is_logged_and_does_not_throw()
    {
        var keeper = Create();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        var validUntil = e.ValidUntil;
        _store.OnHeartbeat = (_, _) => DbDown();

        await keeper.HeartbeatOnceAsync(Ct);

        Assert.IsType<TimeoutException>(Assert.Single(_logger.At(LogLevel.Warning)).Exception);
        Assert.Equal(ExecutionState.Running, e.State);
        Assert.Equal(validUntil, e.ValidUntil);
    }

    [Fact]
    public async Task Loop_calls_heartbeat_every_interval_and_stop_ends_it()
    {
        var keeper = Create();
        keeper.Start();

        _time.Advance(H - Tick);
        Assert.Equal(0, HeartbeatCalls);
        _time.Advance(Tick);
        await Eventually.True(() => HeartbeatCalls == 1, "first heartbeat one interval after start");
        _time.Advance(H);
        await Eventually.True(() => HeartbeatCalls == 2, "second heartbeat one interval later");

        await keeper.StopAsync();
        _time.Advance(H * 3);
        Assert.Equal(2, HeartbeatCalls);
    }

    [Fact]
    public async Task Stop_cancels_the_call_in_flight_without_warning()
    {
        var keeper = Create();
        CancellationToken seen = default;
        _store.OnHeartbeat = (_, ct) =>
        {
            seen = ct;
            var call = NewCall();
            ct.Register(() => call.TrySetCanceled(ct));
            return call.Task;
        };
        keeper.Start();
        _time.Advance(H);
        await Eventually.True(() => HeartbeatCalls == 1, "heartbeat call started");

        await keeper.StopAsync();

        Assert.True(seen.IsCancellationRequested);
        Assert.Empty(_logger.At(LogLevel.Warning));
        Assert.Empty(_errors.GetMeasurementSnapshot());
    }

    [Fact]
    public void Start_twice_throws()
    {
        var keeper = Create();
        keeper.Start();
        Assert.Throws<InvalidOperationException>(keeper.Start);
    }

    // ---- проверка сроков lease -----------------------------------------------------------

    [Fact]
    public async Task Deadline_check_cancels_exactly_at_valid_until_minus_interval()
    {
        _store.OnHeartbeat = (_, _) => DbDown();
        var keeper = Create();
        keeper.Start();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        var deadline = _clock.Add(e.ValidUntil, -H);

        _time.Advance(_clock.Until(deadline) - Tick);
        Assert.Equal(CancelReason.None, e.FirstReason);
        Assert.False(e.Token.IsCancellationRequested);

        _time.Advance(Tick);
        Assert.Equal(CancelReason.HeartbeatLost, e.FirstReason);
        Assert.True(e.Token.IsCancellationRequested);
        Assert.Equal(ExecutionState.Running, e.State);                  // исход не отбрасывается
        Assert.True(_table.TryGet(e.Lease.LeaseId, out _));

        await keeper.StopAsync();
    }

    [Fact]
    public async Task Deadline_check_fires_while_heartbeat_call_hangs()
    {
        var hung = NewCall();
        _store.OnHeartbeat = (_, _) => hung.Task;                      // игнорирует токен — вызов висит
        var keeper = Create();
        keeper.Start();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        var deadline = _clock.Add(e.ValidUntil, -H);

        _time.Advance(H);
        await Eventually.True(() => HeartbeatCalls == 1, "heartbeat call started and hangs");
        _time.Advance(_clock.Until(deadline) - Tick);
        Assert.Equal(CancelReason.None, e.FirstReason);
        _time.Advance(Tick);
        Assert.Equal(CancelReason.HeartbeatLost, e.FirstReason);
        Assert.Equal(1, HeartbeatCalls);

        hung.SetException(new TimeoutException("released by test"));
        await keeper.StopAsync();
    }

    [Fact]
    public async Task Each_execution_is_cancelled_at_its_own_deadline()
    {
        _store.OnHeartbeat = (_, _) => DbDown();
        var keeper = Create();
        keeper.Start();
        var early = keeper.Register(Leased.Job(), "q", FromNow(L));            // рубеж: +45 с
        _time.Advance(TimeSpan.FromSeconds(10));
        var earlier = keeper.Register(Leased.Job(), "q", FromNow(TimeSpan.FromSeconds(30)));   // рубеж: +25 с — таймер сдвигается раньше

        AdvanceTo(_clock.Add(earlier.ValidUntil, -H));
        Assert.Equal(CancelReason.HeartbeatLost, earlier.FirstReason);
        Assert.Equal(CancelReason.None, early.FirstReason);

        _time.Advance(_clock.Until(_clock.Add(early.ValidUntil, -H)) - Tick);
        Assert.Equal(CancelReason.None, early.FirstReason);
        _time.Advance(Tick);                                             // таймер перевзведён после срабатывания
        Assert.Equal(CancelReason.HeartbeatLost, early.FirstReason);

        await keeper.StopAsync();
    }

    [Fact]
    public async Task Execution_registered_past_its_deadline_is_cancelled_at_once()
    {
        _store.OnHeartbeat = (_, _) => DbDown();
        var keeper = Create();
        keeper.Start();

        var e = keeper.Register(Leased.Job(), "q", FromNow(H - TimeSpan.FromSeconds(1)));

        await Eventually.True(() => e.FirstReason == CancelReason.HeartbeatLost, "deadline already passed at registration");
        await keeper.StopAsync();
    }

    [Fact]
    public async Task Already_cancelled_execution_does_not_get_heartbeat_lost_and_timer_does_not_spin()
    {
        _store.OnHeartbeat = (_, _) => DbDown();
        var keeper = Create();
        keeper.Start();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        e.Cancel(CancelReason.Shutdown);

        _time.Advance(L);                                                // рубеж пройден

        Assert.Equal(CancelReason.Shutdown, e.FirstReason);
        Assert.DoesNotContain(_logger.At(LogLevel.Warning), l => l.Message.Contains("not confirmed", StringComparison.Ordinal));
        await keeper.StopAsync();
    }

    // ---- сценарии сбоя ---------------------------------------------------------------

    [Fact]
    public async Task Job_registered_after_snapshot_of_the_last_success_is_cancelled_by_its_own_valid_until()
    {
        // H = 15 с, L = 60 с. Старт в T, heartbeat в T + 15 с. Dequeue начат в t0 = T + 1 с и длился ≈ H:
        // задача зарегистрирована после снимка успешного heartbeat (sentAt = t0 + 14 с), дальше БД недоступна.
        var keeper = Create();
        var start = _clock.Now;
        var t0 = _clock.Add(start, TimeSpan.FromSeconds(1));
        var first = NewCall();
        var calls = 0;
        _store.OnHeartbeat = (_, _) => Interlocked.Increment(ref calls) == 1 ? first.Task : DbDown();
        keeper.Start();

        _time.Advance(H);
        await Eventually.True(() => HeartbeatCalls == 1, "heartbeat snapshot taken at T + 15 s");
        var sentAt = _clock.Now;
        var e = keeper.Register(Leased.Job(), "q", _clock.Add(t0, L));
        first.SetResult([]);                                             // успех: lastSuccess = T + 15 с
        await Eventually.True(() => keeper.HeartbeatWaits == 2, "success processed, loop waits for the next call");

        var deadline = _clock.Add(t0, L - H);                            // t0 + L − H = T + 46 с
        Assert.True(deadline < _clock.Add(sentAt, L - H));               // серверный watchdog сработал бы только в T + 60 с
        _time.Advance(_clock.Until(deadline) - Tick);
        Assert.Equal(CancelReason.None, e.FirstReason);
        _time.Advance(Tick);
        Assert.Equal(CancelReason.HeartbeatLost, e.FirstReason);

        await keeper.StopAsync();
    }

    [Fact]
    public async Task Heartbeat_lost_starts_at_last_success_plus_lease_minus_interval_and_lifts_on_success()
    {
        var failing = 1;
        _store.OnHeartbeat = (leases, _) =>
            Volatile.Read(ref failing) == 1 ? DbDown() : Task.FromResult(FakeServerStore.AllAlive(leases));
        var keeper = Create();
        var start = _clock.Now;
        keeper.Start();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));

        _time.Advance(H);
        await Eventually.True(() => HeartbeatCalls == 1, "call at T + 15 s");
        _time.Advance(H);
        await Eventually.True(() => HeartbeatCalls == 2, "call at T + 30 s");
        _time.Advance(H - Tick);
        Assert.False(keeper.HeartbeatLost);
        Assert.Equal(CancelReason.None, e.FirstReason);

        _time.Advance(Tick);                                             // T + 45 с = старт + L − H
        Assert.True(keeper.HeartbeatLost);
        Assert.Equal(CancelReason.HeartbeatLost, e.FirstReason);         // рубеж выполнения совпал со входом в состояние
        await Eventually.True(() => HeartbeatCalls == 3, "call at T + 45 s fails too");
        Assert.Equal(0, Volatile.Read(ref _recovered));

        Volatile.Write(ref failing, 0);
        _time.Advance(H);                                                // T + 60 с
        var sentAt = _clock.Now;
        await Eventually.True(() => Volatile.Read(ref _recovered) == 1, "first successful heartbeat lifts HeartbeatLost");
        Assert.False(keeper.HeartbeatLost);
        Assert.Equal(CancelReason.HeartbeatLost, e.FirstReason);         // отменённое не восстанавливается
        Assert.True(e.Token.IsCancellationRequested);
        Assert.Equal(_clock.Add(sentAt, L), e.ValidUntil);               // lease из снимка продлевается снова

        _time.Advance(H);
        var nextSentAt = _clock.Now;
        await Eventually.True(() => e.ValidUntil == _clock.Add(nextSentAt, L), "next heartbeat processed");
        Assert.Equal(1, Volatile.Read(ref _recovered));                  // onRecovered — один раз на восстановление
        Assert.True(start < sentAt);

        await keeper.StopAsync();
    }

    [Fact]
    public async Task Slow_successful_heartbeat_at_minimum_lease_does_not_raise_heartbeat_lost()
    {
        // L = 3H, максимальный разброс расписания (1.1H) и вызовы длиной почти H/2: успех приходит
        // не позже sentAt + 1.6H, раньше рубежа sentAt + 2H. Расписание — от sentAt, не от завершения.
        var h = TimeSpan.FromSeconds(10);
        var l = h * 3;
        var step = h * 1.1;
        var slow = h / 2 - Tick;
        var pending = new ConcurrentQueue<(TaskCompletionSource<IReadOnlyList<HeartbeatLease>> Call, LeaseRef[] Leases)>();
        _store.OnHeartbeat = (leases, _) =>
        {
            var call = NewCall();
            pending.Enqueue((call, leases.ToArray()));
            return call.Task;
        };
        var keeper = Create(l, h, jitter: Math.BitDecrement(1.0));
        keeper.Start();
        var e = keeper.Register(Leased.Job(), "q", FromNow(l));

        for (var i = 1; i <= 5; i++)
        {
            // Ответ приходит асинхронно: время сдвигается только после того, как цикл создал таймер паузы, иначе
            // таймер взвёлся бы на задержку, вычисленную до сдвига, и не сработал бы.
            await Eventually.True(() => keeper.HeartbeatWaits == i, $"loop waits for call {i}");
            _time.Advance(i == 1 ? step : step - slow);
            await Eventually.True(() => pending.Count == 1, $"call {i} starts {step} after the previous sentAt");
            var sentAt = _clock.Now;
            _time.Advance(slow);

            Assert.False(keeper.HeartbeatLost);
            Assert.Equal(CancelReason.None, e.FirstReason);
            Assert.True(pending.TryDequeue(out var current));
            current.Call.SetResult(FakeServerStore.AllAlive(current.Leases));
            await Eventually.True(() => e.ValidUntil == _clock.Add(sentAt, l), $"call {i} processed");
        }

        Assert.Equal(0, Volatile.Read(ref _recovered));
        await keeper.StopAsync();
    }

    // ---- строка servers ------------------------------------------------------

    [Fact]
    public async Task Missing_server_row_is_registered_again()
    {
        var keeper = Create();
        _store.ServerPresent = false;

        await keeper.HeartbeatOnceAsync(Ct);

        Assert.Same(Registration, Assert.Single(_store.CallsOf("RegisterServer")).Detail);
        Assert.Contains(_logger.At(LogLevel.Warning), e => e.Message.Contains("registered again"));
    }

    [Fact]
    public async Task Present_server_row_is_not_registered()
    {
        await Create().HeartbeatOnceAsync(Ct);

        Assert.Empty(_store.CallsOf("RegisterServer"));
    }

    [Fact]
    public async Task Failed_registration_is_logged_and_retried_on_next_heartbeat()
    {
        var keeper = Create();
        _store.ServerPresent = false;
        _store.OnRegisterServer = (_, _) => Task.FromException(new TimeoutException("db down"));

        await keeper.HeartbeatOnceAsync(Ct);
        await keeper.HeartbeatOnceAsync(Ct);

        Assert.Equal(2, _store.CallsOf("RegisterServer").Count());
        Assert.Equal(2, _logger.At(LogLevel.Warning).Count(e => e.Message.Contains("re-registering")));
    }

    [Fact]
    public async Task Registration_ending_after_planned_time_delays_only_next_heartbeat()
    {
        var keeper = Create(jitter: 0);   // шаг расписания 0.9 H
        _store.ServerPresent = false;
        var registration = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnRegisterServer = (_, _) => registration.Task;   // фейк не наблюдает токен: длительность задаёт тест
        keeper.Start();

        _time.Advance(H * 0.9);                                  // первый heartbeat: sentAt = S
        await Eventually.True(() => _store.CallsOf("RegisterServer").Count() == 1, "first heartbeat registers");
        _time.Advance(H);                                        // S + H: плановый S + 0.9 H уже прошёл
        _store.ServerPresent = true;
        registration.SetResult();

        await Eventually.True(() => HeartbeatCalls == 2, "next heartbeat starts right after the registration");
        await Eventually.True(() => keeper.HeartbeatWaits == 3, "loop waits for the third call");   // таймер паузы создан до сдвига
        _time.Advance(H * 0.9 - Tick);
        await Task.Yield();
        Assert.Equal(2, HeartbeatCalls);                         // расписание — от фактического sentAt = S + H
        _time.Advance(Tick);
        await Eventually.True(() => HeartbeatCalls == 3, "third heartbeat at S + H + 0.9 H");
        await keeper.StopAsync();
    }

    [Fact]
    public async Task Stop_during_registration_cancels_and_waits_for_it()
    {
        var keeper = Create(jitter: 0);
        _store.ServerPresent = false;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var observedCancel = false;
        _store.OnRegisterServer = async (_, ct) =>
        {
            started.SetResult();
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
            catch (OperationCanceledException)
            {
                observedCancel = true;
                throw;
            }
        };
        keeper.Start();
        _time.Advance(H * 0.9);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);

        await keeper.StopAsync().WaitAsync(TimeSpan.FromSeconds(5), Ct);

        Assert.True(observedCancel);
        Assert.Single(_store.CallsOf("RegisterServer"));
        Assert.DoesNotContain(_logger.At(LogLevel.Warning), e => e.Message.Contains("re-registering"));
    }

    [Fact]
    public async Task Stop_during_registration_ignores_its_non_cancellation_failure()
    {
        var keeper = Create(jitter: 0);
        _store.ServerPresent = false;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnRegisterServer = async (_, ct) =>
        {
            started.SetResult();
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
            catch (OperationCanceledException)
            {
                // как Npgsql, рвущий соединение при отмене: не OperationCanceledException
                throw new InvalidOperationException("connection broken");
            }
        };
        keeper.Start();
        _time.Advance(H * 0.9);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);

        await keeper.StopAsync().WaitAsync(TimeSpan.FromSeconds(5), Ct);

        Assert.Single(_store.CallsOf("RegisterServer"));
        Assert.Empty(_logger.At(LogLevel.Warning));
        Assert.Empty(_errors.GetMeasurementSnapshot());
    }

    // ---- метрики ------------------------------------------------------------------------------

    [Fact]
    public async Task Lost_running_lease_counts_dropped()
    {
        var keeper = Create();
        keeper.Register(Leased.Job(), "q", FromNow(L));
        Respond([]);

        await keeper.HeartbeatOnceAsync(Ct);

        var m = Assert.Single(_dropped.GetMeasurementSnapshot());
        Assert.Equal((1L, "q", DropReasons.LeaseLost), (m.Value, m.Tags[Attr.Queue], m.Tags[Attr.DropReason]));
    }

    [Fact]
    public async Task Lost_pending_outcome_counts_dropped()
    {
        var keeper = Create();
        keeper.RegisterForRelease(Leased.Job(), "q", FromNow(L));
        Respond([]);

        await keeper.HeartbeatOnceAsync(Ct);
        await keeper.HeartbeatOnceAsync(Ct);   // запись уже снята: повторно не считается

        Assert.Equal(1, Dropped(DropReasons.LeaseLost));
    }

    [Fact]
    public void Unregister_with_reason_counts_dropped_once()
    {
        var keeper = Create();
        var withReason = keeper.Register(Leased.Job(), "q", FromNow(L));
        var withoutReason = keeper.Register(Leased.Job(), "q", FromNow(L));

        Assert.True(keeper.Unregister(withoutReason));
        Assert.Equal(0, Dropped(DropReasons.Abandoned));

        Assert.True(keeper.Unregister(withReason, DropReasons.Abandoned));
        Assert.False(keeper.Unregister(withReason, DropReasons.Abandoned));   // запись уже снята: повторно не считается

        Assert.Equal(1, Dropped(DropReasons.Abandoned));
    }

    [Fact]
    public async Task Failed_heartbeat_counts_storage_error()
    {
        var keeper = Create();
        _store.OnHeartbeat = (_, _) => DbDown();

        await keeper.HeartbeatOnceAsync(Ct);

        var m = Assert.Single(_errors.GetMeasurementSnapshot());
        Assert.Equal((1L, StorageOperations.Heartbeat), (m.Value, m.Tags[Attr.StorageOperation]));
    }

    [Fact]
    public async Task Timed_out_heartbeat_counts_storage_error()
    {
        var keeper = Create();
        _store.OnHeartbeat = (_, ct) =>
        {
            var call = NewCall();
            ct.Register(() => call.TrySetCanceled(ct));
            return call.Task;
        };

        var heartbeat = keeper.HeartbeatOnceAsync(Ct);
        _time.Advance(H / 2);
        await heartbeat;

        Assert.Equal(1, HeartbeatErrors);
    }

    [Fact]
    public async Task Failed_registration_counts_storage_error()
    {
        var keeper = Create();
        _store.ServerPresent = false;
        _store.OnRegisterServer = (_, _) => Task.FromException(new TimeoutException("db down"));

        await keeper.HeartbeatOnceAsync(Ct);

        Assert.Equal(1, HeartbeatErrors);
    }

    [Fact]
    public async Task Successful_registration_is_not_storage_error()
    {
        var keeper = Create();
        _store.ServerPresent = false;

        await keeper.HeartbeatOnceAsync(Ct);

        Assert.Empty(_errors.GetMeasurementSnapshot());
    }

    [Fact]
    public async Task Non_cancellation_exception_during_stop_is_not_a_storage_error()
    {
        var keeper = Create();
        _store.OnHeartbeat = async (_, ct) =>
        {
            await Task.Delay(Timeout.Infinite, ct).ContinueWith(_ => { }, CancellationToken.None);
            throw new IOException("connection reset");
        };
        using var stop = new CancellationTokenSource();

        var heartbeat = keeper.HeartbeatOnceAsync(stop.Token);
        await Eventually.True(() => HeartbeatCalls == 1, "heartbeat is in flight");
        await stop.CancelAsync();
        await heartbeat;

        Assert.Equal(0, HeartbeatErrors);
        Assert.Empty(_logger.Entries);
    }
}
