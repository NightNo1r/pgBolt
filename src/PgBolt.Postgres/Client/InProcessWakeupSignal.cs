using System.Collections.Concurrent;

namespace PgBolt.Postgres.Client;

/// <summary>
/// In-process сигнал: подписчики по очереди в copy-on-write массиве (часть 3 §8.2). Signal не аллоцирует и не берёт
/// блокировок; подписка/отписка — CAS по словарю. Сигнал без подписчиков теряется: latch обеспечивает подписчик.
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
