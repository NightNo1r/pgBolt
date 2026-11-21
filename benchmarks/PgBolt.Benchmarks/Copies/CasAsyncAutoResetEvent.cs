// Копия: src/PgBolt.Server/Runtime/AsyncAutoResetEvent.cs @ 52b4a8f (lock-free CAS, текущая).
// Изменены только пространство имён и имя типа; код синхронизации — дословно.

namespace PgBolt.Benchmarks.Copies;

/// <summary>
/// Lock-free latched auto-reset event (§4.3) for one waiter at a time (dispatcher or resolver loop) and any number of Set callers. With no
/// waiter, Set latches a signal for the next WaitAsync and repeated calls coalesce. Otherwise it wakes one waiter. WaitAsync returns true on a
/// signal, including a latched one, and false on timeout. A second concurrent WaitAsync throws InvalidOperationException.
/// </summary>
/// <remarks>
/// One cell holds <c>null</c> (empty), <see cref="Signaled"/> (latched), or the waiter's TaskCompletionSource. State changes use CAS. Exactly
/// one CAS winner removes the waiter, either Set to complete it or the waiter itself on timeout or cancellation. A signal at that boundary is
/// neither lost nor delivered twice, and removed waiters cannot accumulate.
/// </remarks>
internal sealed class CasAsyncAutoResetEvent
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

    private async Task<bool> WaitCoreAsync(TaskCompletionSource waiter, TimeSpan timeout, TimeProvider time, CancellationToken ct)
    {
        try
        {
            await waiter.Task.WaitAsync(timeout, time, ct);
            return true;
        }
        catch (TimeoutException)
        {
            // Set забрал ожидающего уже после таймаута — сигнал доставлен.
            return !Retire(waiter);
        }
        catch (OperationCanceledException)
        {
            // Сигнал, доставленный в момент отмены, уходит дальше — в латч.
            if (!Retire(waiter))
                Set();
            throw;
        }
    }

    private bool TryConsumeLatch() => ReferenceEquals(Interlocked.CompareExchange(ref _state, null, Signaled), Signaled);

    /// <summary>Returns true if this call removed the waiter, or false if Set already claimed it.</summary>
    private bool Retire(TaskCompletionSource waiter) => ReferenceEquals(Interlocked.CompareExchange(ref _state, null, waiter), waiter);
}
