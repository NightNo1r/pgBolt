namespace PgBolt.Server.Runtime;

/// <summary>
/// Lock-free latched auto-reset событие (§4.3) для одного ожидающего за раз (цикл диспетчера, цикл resolver'а) и любого
/// числа вызывающих Set. Set без ожидающего сохраняется до следующего WaitAsync (повторные схлопываются); Set будит
/// ровно одного ожидающего. WaitAsync не бросает на таймаут: true — событие установлено (в том числе латч), false —
/// истёк таймаут. Второй одновременный WaitAsync — InvalidOperationException.
/// </summary>
/// <remarks>
/// Всё состояние — одна ячейка: <c>null</c> (пусто), <see cref="Signaled"/> (латч) или TaskCompletionSource ожидающего;
/// переходы — CAS. Ожидающего из ячейки снимает ровно один победитель CAS: Set (и завершает его) или сам ожидающий
/// по таймауту/отмене. Поэтому сигнал на границе таймаута или отмены не теряется и не доставляется дважды, а снятые
/// ожидающие не накапливаются.
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

    /// <summary>Таймаут — без исключения (SuppressThrowing): у простаивающего диспетчера он штатный, до десятков раз в секунду.</summary>
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

    /// <summary>true — ожидающий снят этим вызовом; false — его уже забрал Set.</summary>
    private bool Retire(TaskCompletionSource waiter) => ReferenceEquals(Interlocked.CompareExchange(ref _state, null, waiter), waiter);
}
