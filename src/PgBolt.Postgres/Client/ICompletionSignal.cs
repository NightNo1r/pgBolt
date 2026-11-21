namespace PgBolt.Postgres.Client;

/// <summary>
/// Wakes the local resolver after completion signals are written by finalization with subscribers, CancelBatchAsync, or SubmitAsync for
/// synthetic signals (Part 5 §6.4). Periodic polling covers missed signals.
/// </summary>
public interface ICompletionSignal
{
    void Signal();

    /// <summary>Signal invokes the callback synchronously. Keep it lightweight and nonblocking.</summary>
    /// <returns>A subscription. Dispose unsubscribes and is idempotent.</returns>
    IDisposable Subscribe(Action callback);
}
