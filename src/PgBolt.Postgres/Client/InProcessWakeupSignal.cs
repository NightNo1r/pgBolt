using System.Collections.Concurrent;

namespace PgBolt.Postgres.Client;

/// <summary>
/// Signals local workers through per-queue copy-on-write subscriber arrays (Part 3 §8.2). Signal takes no locks and allocates no memory.
/// Subscribing and unsubscribing use compare-and-swap on the dictionary. Subscribers provide their own latch. Signals without subscribers are
/// lost.
/// </summary>
public sealed class InProcessWakeupSignal : IWakeupSignal
{
    private readonly ConcurrentDictionary<string, Action[]> _subscribers = new(StringComparer.Ordinal);

    public void Signal(string queue)
    {
        if (_subscribers.TryGetValue(queue, out var callbacks))
            SignalSubscribers.InvokeAll(callbacks);
    }

    public IDisposable Subscribe(string queue, Action callback)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(callback);
        _subscribers.AddOrUpdate(queue,
            static (_, cb) => [cb],
            static (_, existing, cb) => [.. existing, cb],
            callback);
        return new SignalSubscribers.Subscription(() => Unsubscribe(queue, callback));
    }

    private void Unsubscribe(string queue, Action callback)
    {
        while (_subscribers.TryGetValue(queue, out var existing))
        {
            var index = Array.IndexOf(existing, callback);
            if (index < 0)
                return;
            if (existing.Length == 1)
            {
                if (_subscribers.TryRemove(new KeyValuePair<string, Action[]>(queue, existing)))
                    return;
                continue;
            }
            var next = new Action[existing.Length - 1];
            Array.Copy(existing, 0, next, 0, index);
            Array.Copy(existing, index + 1, next, index, existing.Length - index - 1);
            if (_subscribers.TryUpdate(queue, next, existing))
                return;
        }
    }
}
