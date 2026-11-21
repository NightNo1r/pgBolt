namespace PgBolt.Postgres.Client;

/// <summary>
/// Wakes workers after enqueue, trigger, or promotion (§5.10, Part 3 §8.2). Workers also poll in case a signal is missed. Implementations can
/// signal other pods, for example through Redis pub/sub.
/// </summary>
public interface IWakeupSignal
{
    void Signal(string queue);

    /// <summary>Signal invokes the callback synchronously. Keep it lightweight and nonblocking.</summary>
    /// <returns>A subscription. Dispose unsubscribes and is idempotent.</returns>
    IDisposable Subscribe(string queue, Action callback);
}
