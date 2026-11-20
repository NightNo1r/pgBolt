using System.Collections.Concurrent;
using System.Collections.Frozen;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;
using static PgBolt.Postgres.Diagnostics.PgBoltTelemetry;
using I = PgBolt.PgBoltDiagnostics.Instruments;

namespace PgBolt.Server.Tests.Unit;

public sealed class DispatcherTests : IAsyncDisposable
{
    private static readonly TimeSpan Ms = TimeSpan.FromMilliseconds(1);
    private static readonly Guid ServerId = Guid.CreateVersion7();

    private readonly FakeTimeProvider _time = new();
    private readonly MonoClock _clock;
    private readonly FakeServerStore _store = new();
    private readonly ExecutionTable _table = new();
    private readonly InProcessWakeupSignal _wakeup = new();
    private readonly ListLogger _logs = new();
    private readonly PgBoltInstrumentation _instrumentation = TestTelemetry.Instrumentation();
    private readonly ConcurrentQueue<Execution> _started = new();
    private readonly ConcurrentQueue<(string Queue, int Max, DateTimeOffset At)> _dequeues = new();
    private readonly PgBoltServerOptions _options = new()
    {
        Workers = 4,
        LeaseDuration = TimeSpan.FromSeconds(60),
        HeartbeatInterval = TimeSpan.FromSeconds(15),
    };
    private SlotAllocator _slots = null!;
    private LeaseKeeper _keeper = null!;
    private CompletionSink _sink = null!;
    private Dispatcher _dispatcher = null!;

    private readonly PgBoltMetrics _metrics;

    public DispatcherTests()
    {
        _clock = new MonoClock(_time);
        _metrics = TestTelemetry.Metrics(_instrumentation);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Ответ dequeue по (очередь, max, ct); по умолчанию пусто.</summary>
    private Func<string, int, CancellationToken, Task<IReadOnlyList<LeasedJob>>> Respond { get; set; } =
        (_, _, _) => Task.FromResult<IReadOnlyList<LeasedJob>>([]);

    /// <summary>Ответ лимитированного dequeue (очередь из Settings.Limits); по умолчанию пусто, лимит не исчерпан.</summary>
    private Func<string, int, CancellationToken, Task<LimitedDequeueResult>> RespondLimited { get; set; } =
        (_, _, _) => Task.FromResult(new LimitedDequeueResult([], false));

    /// <summary>Вызывается вместо запуска JobRunner (по умолчанию выполнение «висит», слот занят).</summary>
    private Action<Execution> OnStart { get; set; } = _ => { };

    /// <summary>Стратегия общего пула для SlotAllocator (по умолчанию — строгий порядок).</summary>
    private ISharedPoolDistribution Distribution { get; set; } = new StrictOrderDistribution();

    /// <summary>Снимок настроек очередей для стратегии выборки (по умолчанию пустой — обычный dequeue).</summary>
    private QueueSettingsSnapshot Settings { get; set; } = QueueSettingsSnapshot.Empty;

    private void Start()
    {
        _store.OnDequeue = (queue, max, ct) =>
        {
            _dequeues.Enqueue((queue, max, _time.GetUtcNow()));
            return Respond(queue, max, ct);
        };
        _store.OnDequeueLimited = (queue, max, ct) =>
        {
            _dequeues.Enqueue((queue, max, _time.GetUtcNow()));
            return RespondLimited(queue, max, ct);
        };
        _slots = new SlotAllocator(_options.Workers, _options.Queues, Distribution);
        _keeper = new LeaseKeeper(new ServerRegistration(ServerId, "s", "h", ["q"], 1), _store, _table, _options, _clock, NullLogger.Instance, onRecovered: () => { }, _metrics);
        _sink = new CompletionSink(ServerId, _store, _table, _options, _clock, NullLogger.Instance, new OutcomeStats(_time), _metrics);
        _dispatcher = new Dispatcher(_options, _slots, new DequeueFetchStrategy(_store, ServerId, _options.LeaseDuration, () => Settings), _keeper, _sink,
            _wakeup, _clock, _logs.CreateLogger("dispatcher"), e =>
            {
                _started.Enqueue(e);
                OnStart(e);
            }, _metrics)
        {
            NextRandom = () => 0.5,   // jitter ×1.0
        };
        _sink.Start();
        _keeper.Start();
        _dispatcher.Start();
    }

    public async ValueTask DisposeAsync()
    {
        _instrumentation.Dispose();
        if (_dispatcher is null)
            return;
        _dispatcher.SignalStop();
        await _dispatcher.Completion;
        await _sink.StopAsync(CancellationToken.None);
        await _keeper.StopAsync();
    }

    private static Task<IReadOnlyList<LeasedJob>> Jobs(int count) =>
        Task.FromResult<IReadOnlyList<LeasedJob>>(Enumerable.Range(0, count).Select(_ => Leased.Job()).ToArray());

    private static Task<LimitedDequeueResult> Limited(int count, bool limitReached) =>
        Task.FromResult(new LimitedDequeueResult(Enumerable.Range(0, count).Select(_ => Leased.Job()).ToArray(), limitReached));

    private void LimitQueue(string queue) =>
        Settings = new QueueSettingsSnapshot(new Dictionary<string, int> { [queue] = 1 }.ToFrozenDictionary(StringComparer.Ordinal), FrozenDictionary<string, int>.Empty);

    private Task Idle(int waits) =>
        Eventually.True(() => _dispatcher.IdleWaits == waits, $"dispatcher enters wait #{waits}");

    private Task Dequeues(int count) =>
        Eventually.True(() => _dequeues.Count == count, $"dequeue #{count} happens");

    private void FreeSlot(Execution execution)
    {
        // Runtime возвращает ёмкость до сигнала диспетчеру.
        _slots.Release(execution.Queue, execution.FromReserved);
        _dispatcher.OnSlotFreed(execution.Queue);
    }

    private void AssertPollTimes(params int[] milliseconds)
    {
        // Отсчитываем от первого запроса той же очереди с точностью виртуальных часов.
        var first = _dequeues.First().At;
        Assert.Equal(milliseconds.Select(ms => first + ms * Ms), _dequeues.Select(d => d.At));
    }

    private async Task AssertPollCountAfterWake(int count, int nextWait)
    {
        // Даём диспетчеру увидеть текущее виртуальное время до проверки числа запросов.
        _dispatcher.Wake();
        await Idle(nextWait);
        Assert.Equal(count, _dequeues.Count);
    }

    [Fact]
    public async Task Queues_are_polled_in_declaration_order_and_unused_shared_slots_move_on()
    {
        _options.Queue("a").Queue("b").Queue("c", q => q.Reserved = 1);
        Respond = (queue, _, _) => queue switch
        {
            "a" => Jobs(1),
            "b" => Jobs(3),
            _ => Jobs(0),
        };
        Start();

        await Idle(1);

        Assert.Equal([("a", 4), ("b", 3), ("c", 1)], _dequeues.Select(d => (d.Queue, d.Max)));
        Assert.Equal(4, _started.Count);
        Assert.Equal((1, 3, 0), (_slots.InUse("a"), _slots.InUse("b"), _slots.InUse("c")));
        Assert.All(_started, e => Assert.Equal(ExecutionState.Running, e.State));
        Assert.Equal(4, _table.Count);
    }

    [Fact]
    public async Task Full_batch_is_followed_by_immediate_poll()
    {
        _options.Workers = 10;
        _options.Queue("q");
        var calls = 0;
        Respond = (_, max, _) => Interlocked.Increment(ref calls) == 1 ? Jobs(max) : Jobs(0);
        OnStart = e => _slots.Release(e.Queue, e.FromReserved);   // слоты свободны сразу
        Start();

        await Idle(2);

        var polls = _dequeues.ToArray();
        Assert.Equal(2, polls.Length);
        Assert.Equal(polls[0].At, polls[1].At);
        Assert.Equal(10, _started.Count);
    }

    [Fact]
    public async Task Empty_queue_doubles_interval_up_to_max()
    {
        _options.Queue("q", q =>
        {
            q.MinPollInterval = TimeSpan.FromMilliseconds(50);
            q.MaxPollInterval = TimeSpan.FromMilliseconds(400);
        });
        Start();

        int[] gapsMs = [100, 200, 400, 400];
        for (var k = 1; k <= gapsMs.Length; k++)
        {
            await Idle(k);
            _time.Advance((gapsMs[k - 1] - 1) * Ms);
            Assert.Equal(k, _dequeues.Count);
            _time.Advance(Ms);
            await Dequeues(k + 1);
        }
    }

    [Fact]
    public async Task Wakeup_signal_resets_next_poll_and_interval()
    {
        _options.Queue("q", q => q.MinPollInterval = TimeSpan.FromMilliseconds(50));
        Start();
        await Idle(1);
        _time.Advance(100 * Ms);
        await Idle(2);                         // второй пустой dequeue: интервал 200 мс

        _wakeup.Signal("q");
        await Dequeues(3);                     // без сдвига времени
        await Idle(3);

        _time.Advance(99 * Ms);                // интервал снова от Min: 100 мс, а не 400
        Assert.Equal(3, _dequeues.Count);
        _time.Advance(Ms);
        await Dequeues(4);
    }

    [Fact]
    public async Task Signal_during_dequeue_is_not_lost()
    {
        _options.Queue("q");
        var calls = 0;
        Respond = (queue, _, _) =>
        {
            if (Interlocked.Increment(ref calls) == 1)
                _wakeup.Signal(queue);         // enqueue пришёл, пока шёл dequeue
            return Jobs(0);
        };
        Start();

        await Dequeues(2);

        var polls = _dequeues.ToArray();
        Assert.Equal(polls[0].At, polls[1].At);
    }

    [Fact]
    public async Task Freed_slot_wakes_dispatcher()
    {
        _options.Workers = 1;
        _options.Queue("q");
        var calls = 0;
        Respond = (_, _, _) => Interlocked.Increment(ref calls) == 1 ? Jobs(1) : Jobs(0);
        Start();
        await Idle(1);                         // слотов нет — ожидание без таймаута

        var execution = Assert.Single(_started);
        _slots.Release(execution.Queue, execution.FromReserved);
        _dispatcher.OnSlotFreed(execution.Queue);

        await Dequeues(2);
        Assert.Equal(_dequeues.First().At, _dequeues.Last().At);
    }

    [Fact]
    public async Task SlotPollKeepsDeadline()
    {
        _options.Queue("q");
        Respond = (_, _, _) => Jobs(_dequeues.Count == 1 ? 2 : 0);
        Start();
        await Idle(1);

        // Каждый из двух слотов освобождается раньше запланированного опроса на 50 мс.
        var waits = 1;
        foreach (var execution in _started)
        {
            _time.Advance(10 * Ms);
            FreeSlot(execution);
            await Idle(++waits);
        }

        _time.Advance(29 * Ms);
        await AssertPollCountAfterWake(3, nextWait: 4);
        _time.Advance(Ms);
        await Idle(5);
        _time.Advance(99 * Ms);
        await AssertPollCountAfterWake(4, nextWait: 6);
        _time.Advance(Ms);
        await Dequeues(5);
        AssertPollTimes(0, 10, 20, 50, 150);
    }

    [Fact]
    public async Task SlotPollAtDeadlineBacksOff()
    {
        _options.Queue("q");
        Respond = (_, _, _) => Jobs(_dequeues.Count == 1 ? 1 : 0);
        OnStart = execution =>
        {
            // Сигнал приходит к самому сроку, пока диспетчер ещё запускает задачу.
            _time.Advance(50 * Ms);
            FreeSlot(execution);
        };
        Start();
        await Idle(2);

        _time.Advance(99 * Ms);
        await AssertPollCountAfterWake(2, nextWait: 3);
        _time.Advance(Ms);
        await Dequeues(3);
        AssertPollTimes(0, 50, 150);
    }

    [Fact]
    public async Task SlowSlotPollKeepsDeadline()
    {
        _options.Queue("q");
        Respond = (_, _, _) =>
        {
            // Второй запрос идёт с 40 до 60 мс и пересекает срок в 50 мс.
            if (_dequeues.Count == 2)
            {
                _time.Advance(20 * Ms);
            }
            return Jobs(_dequeues.Count == 1 ? 1 : 0);
        };
        Start();
        await Idle(1);
        _time.Advance(40 * Ms);
        FreeSlot(Assert.Single(_started));

        await Dequeues(3);
        AssertPollTimes(0, 40, 60);
    }

    [Fact]
    public async Task EnqueueOverridesSlotPoll()
    {
        _options.Queue("q");
        Respond = (_, _, _) => Jobs(_dequeues.Count == 1 ? 1 : 0);
        OnStart = execution =>
        {
            // Оба сигнала накоплены до обработки флагов.
            _time.Advance(10 * Ms);
            FreeSlot(execution);
            _wakeup.Signal(execution.Queue);
        };
        Start();
        await Idle(2);

        _time.Advance(99 * Ms);
        await AssertPollCountAfterWake(2, nextWait: 3);
        _time.Advance(Ms);
        await Dequeues(3);
        AssertPollTimes(0, 10, 110);
    }

    [Fact]
    public async Task LimitOverridesSlotPoll()
    {
        _options.Queue("q");
        LimitQueue("q");
        RespondLimited = (_, _, _) => _dequeues.Count == 1 ? Limited(1, false) : Limited(0, true);
        Start();
        await Idle(1);
        _time.Advance(10 * Ms);
        FreeSlot(Assert.Single(_started));
        await Idle(2);

        // На прежнем сроке слот в кластере всё ещё занят. Место видно после коммита.
        _time.Advance(40 * Ms);
        await AssertPollCountAfterWake(2, nextWait: 3);
        _time.Advance(20 * Ms);
        _dispatcher.OnLeaseFreed("q");
        await Dequeues(3);
        AssertPollTimes(0, 10, 70);
    }

    [Fact]
    public async Task Queue_without_available_slots_is_not_polled()
    {
        _options.Workers = 2;
        _options.Queue("a", q => q.MaxConcurrency = 1).Queue("b");
        Respond = (queue, max, _) => queue == "a" ? Jobs(max) : Jobs(0);
        Start();
        await Idle(1);                         // a взяла полный пакет, слотов у неё нет; ждём только b

        _time.Advance(100 * Ms);
        await Dequeues(3);
        await Idle(2);                         // дождаться, пока взведено следующее ожидание, до сдвига времени
        _time.Advance(200 * Ms);
        await Dequeues(4);

        Assert.Equal(["a", "b", "b", "b"], _dequeues.Select(d => d.Queue));
    }

    [Fact]
    public async Task Hung_dequeue_times_out_after_heartbeat_interval_and_pauses_fetching()
    {
        _options.Queue("q");
        var calls = 0;
        Respond = async (_, _, ct) =>
        {
            switch (Interlocked.Increment(ref calls))
            {
                case 1:
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);   // зависшее соединение
                    return [];
                case 2:
                    throw new IOException("connection refused");
                default:
                    return [];
            }
        };
        Start();
        await Dequeues(1);

        _time.Advance(TimeSpan.FromSeconds(15) - Ms);
        Assert.Equal(0, _dispatcher.IdleWaits);
        _time.Advance(Ms);                     // таймаут dequeue = HeartbeatInterval
        await Idle(1);

        _time.Advance(999 * Ms);               // пауза 1 с
        Assert.Single(_dequeues);
        _time.Advance(Ms);
        await Dequeues(2);
        await Idle(2);

        _time.Advance(1999 * Ms);              // вторая ошибка подряд — пауза 2 с
        Assert.Equal(2, _dequeues.Count);
        _time.Advance(Ms);
        await Dequeues(3);
    }

    [Fact]
    public async Task Jobs_of_dequeue_returning_after_its_timeout_are_released_not_run()
    {
        _options.Queue("q");
        var gate = new TaskCompletionSource<IReadOnlyList<LeasedJob>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        Respond = (_, _, _) => Interlocked.Increment(ref calls) == 1 ? gate.Task : Jobs(0); // №1 не соблюдает токен
        Start();
        await Dequeues(1);

        _time.Advance(TimeSpan.FromSeconds(15) + Ms);   // таймаут HeartbeatInterval истёк, вызов ещё не вернулся
        gate.SetResult(await Jobs(2));

        await Eventually.True(() => _store.CallsOf("Release").Count() == 2, "late jobs are released");
        Assert.All(_store.CallsOf("Release"), c => Assert.Equal((object?)false, c.Detail));
        Assert.Empty(_started);
        Assert.Equal(0, _slots.InUse("q"));
        Assert.False(_keeper.HeartbeatLost);
        await Eventually.True(() => _table.Count == 0, "released executions leave the table");
        await Eventually.True(() => _logs.Entries.Any(e => e.Level == LogLevel.Warning && e.Message.Contains("past its timeout")),
            "late dequeue is logged");
    }

    [Fact]
    public async Task Jobs_of_dequeue_returning_exactly_at_its_timeout_are_started()
    {
        _options.Queue("q");
        var gate = new TaskCompletionSource<IReadOnlyList<LeasedJob>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        Respond = (_, _, _) => Interlocked.Increment(ref calls) == 1 ? gate.Task : Jobs(0);
        Start();
        await Dequeues(1);

        _time.Advance(TimeSpan.FromSeconds(15));        // до рубежа отмены остаётся L − H ≥ 2H
        gate.SetResult(await Jobs(2));

        await Eventually.True(() => _started.Count == 2, "jobs are started");
        Assert.Empty(_store.CallsOf("Release"));
        Assert.All(_started, e => Assert.True(e.ValidUntil >= _clock.Add(_clock.Now, TimeSpan.FromSeconds(45))));
    }

    [Fact]
    public async Task Stop_does_not_cancel_in_flight_dequeue_and_taken_jobs_are_released_not_run()
    {
        _options.Queue("q");
        var gate = new TaskCompletionSource<IReadOnlyList<LeasedJob>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var dequeueToken = CancellationToken.None;
        Respond = (_, _, ct) =>
        {
            dequeueToken = ct;
            return gate.Task;
        };
        Start();
        await Dequeues(1);

        _dispatcher.SignalStop();
        Assert.False(dequeueToken.IsCancellationRequested);
        Assert.False(_dispatcher.Completion.IsCompleted);

        gate.SetResult(await Jobs(2));
        await _dispatcher.Completion;

        await Eventually.True(() => _store.CallsOf("Release").Count() == 2, "taken jobs are released");
        Assert.All(_store.CallsOf("Release"), c => Assert.Equal((object?)false, c.Detail));
        Assert.Empty(_started);
        Assert.Equal(0, _slots.InUse("q"));
        await Eventually.True(() => _table.Count == 0, "released executions leave the table");
    }

    [Fact]
    public async Task Jobs_returned_after_heartbeat_loss_are_released_not_run()
    {
        _options.LeaseDuration = TimeSpan.FromSeconds(3);
        _options.HeartbeatInterval = TimeSpan.FromSeconds(1);
        _options.Queue("q", q =>
        {
            q.MinPollInterval = TimeSpan.FromMilliseconds(1500);
            q.MaxPollInterval = TimeSpan.FromMilliseconds(1500);
        });
        _store.OnHeartbeat = (_, _) => throw new IOException("db unreachable");
        var gate = new TaskCompletionSource<IReadOnlyList<LeasedJob>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        Respond = (_, _, _) => Interlocked.Increment(ref calls) == 1 ? Jobs(0) : gate.Task;
        Start();
        await Idle(1);

        _time.Advance(1500 * Ms);              // dequeue №2 начат до входа в «heartbeat потерян»
        await Dequeues(2);
        _time.Advance(500 * Ms);               // lastSuccess(старт) + L − H = 2 с
        Assert.True(_keeper.HeartbeatLost);

        gate.SetResult(await Jobs(1));

        await Eventually.True(() => _store.CallsOf("Release").Any(), "job is released");
        Assert.Equal((object?)false, Assert.Single(_store.CallsOf("Release")).Detail);
        Assert.Empty(_started);
    }

    [Fact]
    public async Task Heartbeat_loss_pauses_new_dequeues_until_recovery_and_wake()
    {
        _options.LeaseDuration = TimeSpan.FromSeconds(3);
        _options.HeartbeatInterval = TimeSpan.FromSeconds(1);
        _options.Queue("q");
        _store.OnHeartbeat = (_, _) => throw new IOException("db unreachable");
        Start();
        await Idle(1);

        _time.Advance(TimeSpan.FromSeconds(2));   // срок опроса наступил, но heartbeat потерян
        await Idle(2);
        _time.Advance(TimeSpan.FromSeconds(1));   // страховочный таймаут HeartbeatInterval — всё ещё потерян
        await Idle(3);
        Assert.Single(_dequeues);

        _store.OnHeartbeat = (leases, _) => Task.FromResult<IReadOnlyList<HeartbeatLease>>(
            leases.Select(l => new HeartbeatLease(l.JobId, l.LeaseId, false)).ToArray());
        await _keeper.HeartbeatOnceAsync(Ct);
        Assert.False(_keeper.HeartbeatLost);
        _dispatcher.Wake();

        await Dequeues(2);
    }

    [Fact]
    public async Task Outcomes_are_still_sent_while_heartbeat_is_lost()
    {
        _options.LeaseDuration = TimeSpan.FromSeconds(3);
        _options.HeartbeatInterval = TimeSpan.FromSeconds(1);
        _options.Queue("q");
        _store.OnHeartbeat = (_, _) => throw new IOException("db unreachable");
        var calls = 0;
        Respond = (_, _, _) => Interlocked.Increment(ref calls) == 1 ? Jobs(1) : Jobs(0);
        Start();
        await Idle(1);
        var execution = Assert.Single(_started);

        _time.Advance(TimeSpan.FromSeconds(2));   // рубеж выполнения (validUntil − H) и вход в «heartbeat потерян»
        Assert.True(_keeper.HeartbeatLost);
        await Eventually.True(() => execution.FirstReason == CancelReason.HeartbeatLost, "execution is cancelled at its deadline");

        // Runner сдаёт исход остановленной задачи (HeartbeatLost → retry): sink отправляет его и во время сбоя.
        Assert.True(_sink.Complete(execution, new JobOutcome.Retry(TimeSpan.Zero, JobEventKind.Retry, "cancelled: lease renewal not confirmed", null)));

        await Eventually.True(() => _table.Count == 0, "outcome is sent despite heartbeat loss");
        Assert.Equal(execution.Job.Id, Assert.Single(_store.CallsOf("Retry")).JobId!.Value);
        Assert.Single(_dequeues);                 // выборка при этом на паузе
    }

    [Fact]
    public async Task Unexpected_iteration_failure_is_logged_and_fetching_resumes_after_pause()
    {
        _options.Queue("q");
        var calls = 0;
        Respond = (_, _, _) => Interlocked.Increment(ref calls) == 1 ? Jobs(1) : Jobs(0);
        var starts = 0;
        OnStart = _ =>
        {
            if (Interlocked.Increment(ref starts) == 1)
                throw new InvalidOperationException("start failed");
        };
        Start();
        await Idle(1);                            // пауза 1 с после неожиданной ошибки

        Assert.Contains(_logs.At(LogLevel.Critical), e => e.Exception is InvalidOperationException);
        Assert.False(_dispatcher.Completion.IsCompleted);
        _time.Advance(999 * Ms);
        Assert.Single(_dequeues);
        _time.Advance(Ms);
        await Dequeues(2);
    }

    [Fact]
    public async Task Failed_start_rolls_back_registration_and_slot()
    {
        _options.Queue("q");
        var calls = 0;
        Respond = (_, _, _) => Interlocked.Increment(ref calls) == 1 ? Jobs(2) : Jobs(0);
        OnStart = _ => throw new InvalidOperationException("start failed");
        Start();
        await Idle(1);                            // откат, затем пауза после неожиданной ошибки

        var failed = Assert.Single(_started);     // вторая задача пакета не регистрировалась (сирота — её заберёт sweeper)
        Assert.Equal(ExecutionState.Removed, failed.State);
        Assert.Equal(0, _table.Count);            // не Running в таблице: heartbeat не продлевает её lease
        Assert.Equal(0, _slots.InUse("q"));       // слот возвращён
        Assert.Empty(_logs.At(LogLevel.Error));   // без ошибки учёта слотов
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains(failed.Job.Id.ToString()));

        await _keeper.HeartbeatOnceAsync(Ct);
        Assert.All(_store.CallsOf("Heartbeat"), c => Assert.Empty((LeaseRef[])c.Detail!));
    }

    [Fact]
    public async Task Dispatcher_start_failure_counts_abandoned()
    {
        _options.Queue("q");
        using var dropped = new MetricCollector<long>(_instrumentation.Meter, I.JobOutcomeDropped);
        var calls = 0;
        Respond = (_, _, _) => Interlocked.Increment(ref calls) == 1 ? Jobs(1) : Jobs(0);
        OnStart = _ => throw new InvalidOperationException("start failed");
        Start();
        await Idle(1);

        var m = Assert.Single(dropped.GetMeasurementSnapshot());
        Assert.Equal((1L, "q", DropReasons.Abandoned), (m.Value, m.Tags[Attr.Queue], m.Tags[Attr.DropReason]));
    }

    [Fact]
    public async Task Dispatcher_fetch_failure_counts_storage_error()
    {
        _options.Queue("q");
        using var errors = new MetricCollector<long>(_instrumentation.Meter, I.StorageErrors);
        Respond = (_, _, _) => throw new IOException("connection refused");
        Start();
        await Idle(1);

        var m = Assert.Single(errors.GetMeasurementSnapshot());
        Assert.Equal((1L, StorageOperations.Dequeue), (m.Value, m.Tags[Attr.StorageOperation]));
    }

    [Fact]
    public async Task Distribution_declining_a_due_queue_pauses_fetching_instead_of_spinning()
    {
        _options.Queue("q");
        Distribution = new DecliningDistribution();
        Start();
        await Idle(1);                            // план пуст при готовой очереди со свободными слотами — ошибка, пауза 1 с

        Assert.Contains(_logs.At(LogLevel.Critical), e => e.Exception is InvalidOperationException);
        _time.Advance(999 * Ms);
        Assert.Equal(1, _dispatcher.IdleWaits);   // без холостого цикла: до конца паузы новых ожиданий нет
        _time.Advance(Ms);
        await Idle(2);                            // снова отказ стратегии — пауза 2 с

        _time.Advance(1999 * Ms);
        Assert.Equal(2, _dispatcher.IdleWaits);   // и во второй паузе ни новых ожиданий, ни выборки
        Assert.Empty(_dequeues);
        _time.Advance(Ms);
        await Idle(3);                            // третий отказ — пауза 4 с

        // Явная проверка «нет холостого цикла»: цикл стоит на паузе, и за окно реального времени ни счётчик ожиданий,
        // ни число выборок, ни число отказов стратегии (Critical на каждую итерацию) не растут. Холостой цикл
        // (IdleAsync(0) на каждой итерации) увеличивал бы IdleWaits и Critical без Advance.
        await Task.Delay(50, Ct);
        Assert.Equal(3, _dispatcher.IdleWaits);
        Assert.Equal(3, _logs.At(LogLevel.Critical).Count());
        Assert.Empty(_dequeues);
        Assert.False(_dispatcher.Completion.IsCompleted);
    }

    [Fact]
    public async Task Limit_reached_backs_off_even_when_full()
    {
        _options.Queue("q");                   // Min 50 мс
        LimitQueue("q");
        var calls = 0;
        RespondLimited = (_, max, _) => Limited(Interlocked.Increment(ref calls) == 1 ? max : 0, limitReached: true);
        OnStart = e => _slots.Release(e.Queue, e.FromReserved);
        Start();

        await Idle(1);
        Assert.Equal(4, _started.Count);
        _time.Advance(99 * Ms);                // полный пакет у лимита — как пустой: 50 × 2, а не сразу
        Assert.Single(_dequeues);
        _time.Advance(Ms);
        await Dequeues(2);
    }

    [Fact]
    public async Task Signals_and_slot_freed_are_absorbed_while_limit_reached()
    {
        _options.Queue("q");
        LimitQueue("q");
        var calls = 0;
        RespondLimited = (_, _, _) => Limited(Interlocked.Increment(ref calls) == 1 ? 1 : 0, limitReached: true);
        Start();
        await Idle(1);                         // nextPoll = +100 мс

        _wakeup.Signal("q");
        await Idle(2);
        var execution = Assert.Single(_started);
        _slots.Release(execution.Queue, execution.FromReserved);
        _dispatcher.OnSlotFreed(execution.Queue);
        await Idle(3);
        Assert.Single(_dequeues);

        _time.Advance(99 * Ms);                // nextPoll не сдвинут
        Assert.Single(_dequeues);
        _time.Advance(Ms);
        await Dequeues(2);
        await Idle(4);

        _time.Advance(199 * Ms);               // interval не сброшен к Min: 100 × 2
        Assert.Equal(2, _dequeues.Count);
        _time.Advance(Ms);
        await Dequeues(3);
    }

    [Fact]
    public async Task Lease_freed_polls_immediately_while_limit_reached()
    {
        _options.Queue("q");
        LimitQueue("q");
        var calls = 0;
        RespondLimited = (_, _, _) => Limited(Interlocked.Increment(ref calls) == 1 ? 1 : 0, limitReached: true);
        Start();
        await Idle(1);                         // interval 100 мс

        _dispatcher.OnLeaseFreed("q");
        await Dequeues(2);                     // без сдвига времени
        await Idle(2);

        _time.Advance(199 * Ms);               // interval не изменён сигналом: 100 × 2
        Assert.Equal(2, _dequeues.Count);
        _time.Advance(Ms);
        await Dequeues(3);
    }

    [Fact]
    public async Task Lease_freed_is_ignored_without_limit_reached()
    {
        _options.Queue("q");
        LimitQueue("q");
        var calls = 0;
        RespondLimited = (_, _, _) => Limited(0, limitReached: Interlocked.Increment(ref calls) > 1);
        Start();
        await Idle(1);                         // лимит не исчерпан, interval 100 мс

        _dispatcher.OnLeaseFreed("q");
        await Idle(2);
        Assert.Single(_dequeues);

        _time.Advance(100 * Ms);
        await Dequeues(2);                     // теперь LimitReached: сброшенный сигнал не будит сразу
        await Idle(3);
        _time.Advance(199 * Ms);
        Assert.Equal(2, _dequeues.Count);
        _time.Advance(Ms);
        await Dequeues(3);
    }

    [Fact]
    public async Task Lease_freed_during_dequeue_is_not_lost()
    {
        _options.Queue("q");
        LimitQueue("q");
        var calls = 0;
        RespondLimited = (queue, _, _) =>
        {
            if (Interlocked.Increment(ref calls) == 1)
                _dispatcher.OnLeaseFreed(queue);   // исход закоммичен, пока шёл dequeue
            return Limited(0, limitReached: true);
        };
        Start();

        await Dequeues(2);

        var polls = _dequeues.ToArray();
        Assert.Equal(polls[0].At, polls[1].At);
    }

    [Fact]
    public async Task Without_limit_reached_signals_behave_as_before()
    {
        _options.Workers = 1;
        _options.Queue("q");
        LimitQueue("q");
        var calls = 0;
        RespondLimited = (_, _, _) => Limited(Interlocked.Increment(ref calls) == 1 ? 1 : 0, limitReached: false);
        Start();
        await Idle(1);                         // слотов нет — ожидание без таймаута

        var execution = Assert.Single(_started);
        _slots.Release(execution.Queue, execution.FromReserved);
        _dispatcher.OnSlotFreed(execution.Queue);
        await Dequeues(2);                     // slotFreed — сразу
        await Idle(2);                         // пусто: interval 100 мс

        _time.Advance(50 * Ms);
        _wakeup.Signal("q");
        await Dequeues(3);                     // signaled — сразу, interval снова от Min
        await Idle(3);
        _time.Advance(99 * Ms);
        Assert.Equal(3, _dequeues.Count);
        _time.Advance(Ms);
        await Dequeues(4);
    }

    /// <summary>Стратегия, всегда заканчивающая план (нарушает контракт ISharedPoolDistribution).</summary>
    private sealed class DecliningDistribution : ISharedPoolDistribution
    {
        public void BeginPlan(int queueCount, Func<int, bool> candidate) { }

        public (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available,
                                        Func<int, int> freeReserved, int freeShared) => null;

        public void OnTaken(int queue, bool fromReserved) { }
    }
}
