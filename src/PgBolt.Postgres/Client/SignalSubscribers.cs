namespace PgBolt.Postgres.Client;

/// <summary>Subscriber invocation and idempotent unsubscription for local IWakeupSignal and ICompletionSignal implementations.</summary>
internal static class SignalSubscribers
{
    /// <summary>Invokes all subscribers without allocations. Subscriber failures neither escape to the caller nor prevent other callbacks.</summary>
    public static void InvokeAll(Action[] callbacks)
    {
        foreach (var callback in callbacks)
        {
            try
            {
                callback();
            }
            catch (Exception)
            {
                // Подписчик не должен ломать вызывающую операцию (enqueue §8.2 части 3, финализация §6.3 части 5).
            }
        }
    }

    /// <summary>The first Dispose unsubscribes. Subsequent calls have no effect.</summary>
    public sealed class Subscription(Action unsubscribe) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
                unsubscribe();
        }
    }
}
