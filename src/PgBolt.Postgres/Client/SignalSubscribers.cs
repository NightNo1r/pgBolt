namespace PgBolt.Postgres.Client;

/// <summary>Общее для in-process сигналов (IWakeupSignal, ICompletionSignal): вызов подписчиков и подписка с идемпотентной отпиской.</summary>
internal static class SignalSubscribers
{
    /// <summary>Вызывает всех; ошибка подписчика не доходит до вызывающего и не мешает остальным. Без аллокаций.</summary>
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

    /// <summary>Подписка: первый Dispose вызывает unsubscribe, повторные — ничего.</summary>
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
