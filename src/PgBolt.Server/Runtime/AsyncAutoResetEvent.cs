namespace PgBolt.Server.Runtime;

/// <summary>
/// A lock-free, latched auto-reset event (§4.3) for a single waiter (the dispatcher or resolver loop) and any number of
/// Set callers. With no waiter, Set stores a signal for the next WaitAsync. Repeated signals coalesce. Otherwise Set
/// wakes exactly one waiter. WaitAsync returns true for a signal, including a latched one, and false on timeout. A second
/// concurrent WaitAsync throws InvalidOperationException.
/// </summary>
/// <remarks>
/// One slot holds the entire state: <c>null</c> (empty), <see cref="Signaled"/> (latched), or the waiter's
/// TaskCompletionSource. Transitions use CAS. Exactly one caller removes the waiter: Set, which completes it, or the
/// waiter itself on timeout or cancellation. A signal at the timeout or cancellation boundary is delivered at most once
/// and cannot be lost. Removed waiters do not accumulate.
/// </remarks>
internal sealed class AsyncAutoResetEvent
{
    private static readonly object Signaled = new();
    private static readonly Task<bool> True = Task.FromResult(true);
    private static readonly Task<bool> False = Task.FromResult(false);

    private object? _state;

    public void Set()
    {
        var state = Volatile.Read(ref _state);
        while (!ReferenceEquals(state, Signaled))
        {
            // Пусто → латч; ожидающий → пусто, и его завершает победитель CAS.
            var seen = Interlocked.CompareExchange(ref _state, state is null ? Signaled : null, state);
            if (ReferenceEquals(seen, state))
            {
                (state as TaskCompletionSource)?.TrySetResult();
                return;
            }
            state = seen;
        }
    }

    public Task<bool> WaitAsync(TimeSpan timeout, TimeProvider time, CancellationToken ct)
    {
        if (TryConsumeLatch())
            return True;
        if (timeout == TimeSpan.Zero)
            return False;

        var waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        while (true)
        {
            var seen = Interlocked.CompareExchange(ref _state, waiter, null);
            if (seen is null)
                return WaitCoreAsync(waiter, timeout, time, ct);
            if (!ReferenceEquals(seen, Signaled))
                throw new InvalidOperationException("AsyncAutoResetEvent supports a single waiter at a time.");
            if (TryConsumeLatch())
                return True;
        }
    }

    /// <summary>
    /// SuppressThrowing avoids an exception on timeout. An idle dispatcher can time out tens of times per second.
    /// </summary>
    private async Task<bool> WaitCoreAsync(TaskCompletionSource waiter, TimeSpan timeout, TimeProvider time, CancellationToken ct)
    {
        var wait = waiter.Task.WaitAsync(timeout, time, ct);
        await wait.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext | ConfigureAwaitOptions.SuppressThrowing);
        if (wait.IsCompletedSuccessfully)
            return true;
        if (wait.IsCanceled)
        {
            // Сигнал, доставленный в момент отмены, уходит дальше — в латч.
            if (!Retire(waiter))
                Set();
            await wait;   // OperationCanceledException
        }
        _ = wait.Exception;   // TimeoutException (waiter не завершается ошибкой) — наблюдена, без UnobservedTaskException
        // Set забрал ожидающего уже после таймаута — сигнал доставлен.
        return !Retire(waiter);
    }

    private bool TryConsumeLatch() => ReferenceEquals(Interlocked.CompareExchange(ref _state, null, Signaled), Signaled);

    /// <summary>Returns true if this call removed the waiter, or false if Set already claimed it.</summary>
    private bool Retire(TaskCompletionSource waiter) => ReferenceEquals(Interlocked.CompareExchange(ref _state, null, waiter), waiter);
}
