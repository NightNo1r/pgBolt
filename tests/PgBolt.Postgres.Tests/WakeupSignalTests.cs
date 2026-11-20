using System.Collections.Concurrent;
using PgBolt.Postgres.Client;

namespace PgBolt.Postgres.Tests;

public sealed class WakeupSignalTests
{
    [Fact]
    public void Signal_invokes_callback_of_that_queue_synchronously()
    {
        var signal = new InProcessWakeupSignal();
        var a = 0;
        var b = 0;
        using var subA = signal.Subscribe("a", () => a++);
        using var subB = signal.Subscribe("b", () => b++);

        signal.Signal("b");

        Assert.Equal((0, 1), (a, b));   // проверка сразу после Signal: вызов синхронный
    }

    [Fact]
    public void Other_queue_does_not_invoke_callback()
    {
        var signal = new InProcessWakeupSignal();
        var calls = 0;
        using var sub = signal.Subscribe("a", () => calls++);

        signal.Signal("z");
        signal.Signal("A");   // имена очередей сравниваются ordinal

        Assert.Equal(0, calls);
    }

    [Fact]
    public void Signal_without_subscribers_is_noop()
    {
        var signal = new InProcessWakeupSignal();
        Assert.Null(Record.Exception(() => signal.Signal("nobody")));
    }

    [Fact]
    public void Dispose_unsubscribes_and_is_idempotent()
    {
        var signal = new InProcessWakeupSignal();
        var removed = 0;
        var kept = 0;
        var sub = signal.Subscribe("q", () => removed++);
        using var other = signal.Subscribe("q", () => kept++);

        sub.Dispose();
        sub.Dispose();
        signal.Signal("q");

        Assert.Equal((0, 1), (removed, kept));
    }

    [Fact]
    public void Resubscribe_after_last_dispose_works()
    {
        var signal = new InProcessWakeupSignal();
        var calls = 0;
        signal.Subscribe("q", () => calls++).Dispose();
        using var again = signal.Subscribe("q", () => calls++);

        signal.Signal("q");

        Assert.Equal(1, calls);
    }

    [Fact]
    public void Callback_exception_does_not_reach_Signal_and_other_subscribers_still_run()
    {
        var signal = new InProcessWakeupSignal();
        var calls = 0;
        using var bad = signal.Subscribe("q", () => throw new InvalidOperationException("subscriber bug"));
        using var good = signal.Subscribe("q", () => calls++);

        var e = Record.Exception(() => signal.Signal("q"));

        Assert.Null(e);
        Assert.Equal(1, calls);
    }

    [Fact]
    public void All_subscribers_of_one_queue_are_invoked_in_subscription_order()
    {
        var signal = new InProcessWakeupSignal();
        var order = new List<int>();
        using var s1 = signal.Subscribe("q", () => order.Add(1));
        var s2 = signal.Subscribe("q", () => order.Add(2));
        using var s3 = signal.Subscribe("q", () => order.Add(3));

        signal.Signal("q");
        s2.Dispose();
        signal.Signal("q");

        Assert.Equal(new[] { 1, 2, 3, 1, 3 }, order);
    }

    [Fact]
    public void Same_delegate_subscribed_twice_is_removed_once_per_dispose()
    {
        var signal = new InProcessWakeupSignal();
        var calls = 0;
        Action callback = () => calls++;
        var first = signal.Subscribe("q", callback);
        using var second = signal.Subscribe("q", callback);

        first.Dispose();
        signal.Signal("q");

        Assert.Equal(1, calls);
    }

    [Fact]
    public void Concurrent_subscribe_and_dispose_keep_subscriber_set_consistent()
    {
        var signal = new InProcessWakeupSignal();
        var calls = 0;
        var subs = new ConcurrentBag<IDisposable>();

        Parallel.For(0, 200, _ => subs.Add(signal.Subscribe("q", () => Interlocked.Increment(ref calls))));
        signal.Signal("q");
        Assert.Equal(200, Volatile.Read(ref calls));

        Parallel.ForEach(subs, s => s.Dispose());
        signal.Signal("q");
        Assert.Equal(200, Volatile.Read(ref calls));
    }
}
