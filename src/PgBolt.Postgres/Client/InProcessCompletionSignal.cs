namespace PgBolt.Postgres.Client;

/// <summary>In-process сигнал resolver'у: подписчики в copy-on-write массиве, Signal без блокировок и аллокаций.</summary>
public sealed class InProcessCompletionSignal : ICompletionSignal
{
    private readonly Lock _gate = new();
    private Action[] _subscribers = [];

    public void Signal() => SignalSubscribers.InvokeAll(Volatile.Read(ref _subscribers));

    public IDisposable Subscribe(Action callback)
    {
        ArgumentNullException.ThrowIfNull(callback);
        lock (_gate)
            Volatile.Write(ref _subscribers, [.. _subscribers, callback]);
        return new SignalSubscribers.Subscription(() => Unsubscribe(callback));
    }

    private void Unsubscribe(Action callback)
    {
        lock (_gate)
        {
            var index = Array.IndexOf(_subscribers, callback);
            if (index >= 0)
                Volatile.Write(ref _subscribers, [.. _subscribers.AsSpan(0, index), .. _subscribers.AsSpan(index + 1)]);
        }
    }
}
