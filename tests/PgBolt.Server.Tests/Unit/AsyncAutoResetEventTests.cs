using Microsoft.Extensions.Time.Testing;
using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Unit;

public sealed class AsyncAutoResetEventTests
{
    private readonly FakeTimeProvider _time = new();
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void Set_without_waiter_is_latched_until_next_wait()
    {
        var wake = new AsyncAutoResetEvent();
        wake.Set();

        Assert.True(wake.WaitAsync(Timeout.InfiniteTimeSpan, _time, Ct).IsCompletedSuccessfully);
        Assert.False(wake.WaitAsync(Timeout.InfiniteTimeSpan, _time, Ct).IsCompleted);   // auto-reset: латч израсходован
    }

    [Fact]
    public void Repeated_sets_without_waiter_collapse_into_one()
    {
        var wake = new AsyncAutoResetEvent();
        wake.Set();
        wake.Set();

        Assert.True(wake.WaitAsync(TimeSpan.FromSeconds(1), _time, Ct).IsCompletedSuccessfully);
        Assert.False(wake.WaitAsync(TimeSpan.FromSeconds(1), _time, Ct).IsCompleted);
    }

    [Fact]
    public async Task Set_wakes_waiter()
    {
        var wake = new AsyncAutoResetEvent();
        var wait = wake.WaitAsync(Timeout.InfiniteTimeSpan, _time, Ct);
        Assert.False(wait.IsCompleted);

        wake.Set();

        await wait;
        Assert.False(wake.WaitAsync(TimeSpan.FromSeconds(1), _time, Ct).IsCompleted);   // сигнал не остался в латче
    }

    [Fact]
    public async Task Timeout_completes_without_exception()
    {
        var wake = new AsyncAutoResetEvent();
        var wait = wake.WaitAsync(TimeSpan.FromMilliseconds(100), _time, Ct);

        _time.Advance(TimeSpan.FromMilliseconds(99));
        Assert.False(wait.IsCompleted);
        _time.Advance(TimeSpan.FromMilliseconds(1));

        await wait;
    }

    [Fact]
    public async Task Set_after_timed_out_wait_is_not_lost()
    {
        var wake = new AsyncAutoResetEvent();
        var wait = wake.WaitAsync(TimeSpan.FromMilliseconds(10), _time, Ct);
        _time.Advance(TimeSpan.FromMilliseconds(10));
        await wait;

        wake.Set();

        Assert.True(wake.WaitAsync(Timeout.InfiniteTimeSpan, _time, Ct).IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Wait_reports_whether_it_was_set_or_timed_out()   // resolver: проверка работы только после таймера
    {
        var wake = new AsyncAutoResetEvent();
        var timed = wake.WaitAsync(TimeSpan.FromMilliseconds(10), _time, Ct);
        _time.Advance(TimeSpan.FromMilliseconds(10));
        Assert.False(await timed);

        var woken = wake.WaitAsync(Timeout.InfiniteTimeSpan, _time, Ct);
        wake.Set();
        Assert.True(await woken);

        wake.Set();
        Assert.True(await wake.WaitAsync(TimeSpan.Zero, _time, Ct));    // латч
        Assert.False(await wake.WaitAsync(TimeSpan.Zero, _time, Ct));   // нулевой таймаут без латча
    }

    [Fact]
    public async Task Set_delivered_after_timeout_fired_reports_true()   // сигнал, снятый Set'ом в момент таймаута, не теряется
    {
        var wake = new AsyncAutoResetEvent();
        var context = new QueueSynchronizationContext();
        var previous = SynchronizationContext.Current;
        Task<bool> wait;
        SynchronizationContext.SetSynchronizationContext(context);   // продолжение WaitAsync — в очередь контекста
        try
        {
            wait = wake.WaitAsync(TimeSpan.FromMilliseconds(10), _time, Ct);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }

        _time.Advance(TimeSpan.FromMilliseconds(10));   // таймаут сработал, catch ещё не выполнен
        Assert.False(wait.IsCompleted);
        wake.Set();                                     // Set снимает ожидающего и завершает его
        context.RunAll();

        Assert.True(await wait);
        Assert.False(await wake.WaitAsync(TimeSpan.Zero, _time, Ct));   // сигнал доставлен один раз, латч пуст
    }

    [Fact]
    public async Task Timed_out_waits_free_the_slot()   // простаивающий диспетчер ждёт с таймаутом каждую итерацию
    {
        var wake = new AsyncAutoResetEvent();
        for (var i = 0; i < 100; i++)
        {
            var wait = wake.WaitAsync(TimeSpan.FromMilliseconds(10), _time, Ct);   // занятая ячейка дала бы InvalidOperationException
            _time.Advance(TimeSpan.FromMilliseconds(10));
            Assert.False(await wait);
        }

        wake.Set();
        Assert.True(await wake.WaitAsync(TimeSpan.Zero, _time, Ct));   // сигнал ушёл в латч, а не снятому ожидающему
    }

    [Fact]
    public async Task Cancelled_wait_frees_the_slot()
    {
        var wake = new AsyncAutoResetEvent();
        using var cts = new CancellationTokenSource();
        var wait = wake.WaitAsync(Timeout.InfiniteTimeSpan, _time, cts.Token);

        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
        var next = wake.WaitAsync(Timeout.InfiniteTimeSpan, _time, Ct);
        wake.Set();
        Assert.True(await next);
    }

    [Fact]
    public async Task Concurrent_second_wait_throws()   // контракт: один ожидающий за раз (цикл диспетчера, цикл resolver'а)
    {
        var wake = new AsyncAutoResetEvent();
        var first = wake.WaitAsync(Timeout.InfiniteTimeSpan, _time, Ct);

        var error = Record.Exception(() => { _ = wake.WaitAsync(Timeout.InfiniteTimeSpan, _time, Ct); });   // синхронно
        Assert.IsType<InvalidOperationException>(error);

        wake.Set();
        Assert.True(await first);
    }

    [Fact]
    public async Task Concurrent_sets_never_lose_a_wakeup()
    {
        var wake = new AsyncAutoResetEvent();
        for (var i = 0; i < 10_000; i++)
        {
            var wait = wake.WaitAsync(Timeout.InfiniteTimeSpan, TimeProvider.System, Ct);
            var sets = Task.WhenAll(Task.Run(wake.Set, Ct), Task.Run(wake.Set, Ct));
            Assert.True(await wait.WaitAsync(TimeSpan.FromSeconds(10), Ct));
            await sets;
            await wake.WaitAsync(TimeSpan.Zero, TimeProvider.System, Ct);   // второй Set мог уйти в латч — сбросить
        }
    }

    [Fact]
    public async Task Set_racing_timeout_or_cancel_is_delivered_exactly_once()
    {
        var wake = new AsyncAutoResetEvent();
        for (var i = 0; i < 5_000; i++)
        {
            using var cts = new CancellationTokenSource();
            var wait = wake.WaitAsync(i % 2 == 0 ? TimeSpan.FromTicks(1) : Timeout.InfiniteTimeSpan, TimeProvider.System, cts.Token);
            var set = Task.Run(wake.Set, Ct);
            if (i % 2 == 1)
                await cts.CancelAsync();
            bool delivered;
            try
            {
                delivered = await wait;
            }
            catch (OperationCanceledException)
            {
                delivered = false;
            }
            await set;

            // Сигнал либо получен ожидающим, либо остался в латче — ровно один раз.
            Assert.Equal(!delivered, await wake.WaitAsync(TimeSpan.Zero, TimeProvider.System, Ct));
        }
    }

    [Fact]
    public async Task Set_delivered_to_cancelled_waiter_is_passed_on()   // сигнал, снятый Set'ом в момент отмены, не теряется
    {
        var wake = new AsyncAutoResetEvent();
        var context = new QueueSynchronizationContext();
        var previous = SynchronizationContext.Current;
        using var cts = new CancellationTokenSource();
        Task<bool> wait;
        SynchronizationContext.SetSynchronizationContext(context);   // продолжение WaitAsync — в очередь контекста
        try
        {
            wait = wake.WaitAsync(Timeout.InfiniteTimeSpan, _time, cts.Token);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }

        await cts.CancelAsync();   // отмена сработала, catch ещё не выполнен
        Assert.False(wait.IsCompleted);
        wake.Set();                // Set снимает ожидающего и завершает его
        context.RunAll();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
        Assert.True(await wake.WaitAsync(TimeSpan.Zero, _time, Ct));   // сигнал передан в латч
    }

    private sealed class QueueSynchronizationContext : SynchronizationContext
    {
        private readonly Queue<(SendOrPostCallback Callback, object? State)> _posted = new();

        public override void Post(SendOrPostCallback d, object? state)
        {
            lock (_posted)
                _posted.Enqueue((d, state));
        }

        public void RunAll()
        {
            while (true)
            {
                (SendOrPostCallback Callback, object? State) next;
                lock (_posted)
                {
                    if (!_posted.TryDequeue(out next))
                        return;
                }
                next.Callback(next.State);
            }
        }
    }

    [Fact]
    public void Zero_timeout_returns_immediately()
    {
        var wake = new AsyncAutoResetEvent();
        Assert.True(wake.WaitAsync(TimeSpan.Zero, _time, Ct).IsCompletedSuccessfully);
    }
}
