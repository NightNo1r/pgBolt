// Копия: src/PgBolt.Server/Runtime/AsyncAutoResetEvent.cs @ aaf1738 (версия с lock).
// Изменены только пространство имён и имя типа; код синхронизации — дословно.

namespace PgBolt.Benchmarks.Copies;

/// <summary>
/// Latched auto-reset событие (§4.3): Set без ожидающего сохраняется до следующего WaitAsync; Set будит ровно одного
/// ожидающего. WaitAsync не бросает на таймаут: true — событие установлено (в том числе латч), false — истёк таймаут.
/// Ожидающий, снятый по таймауту или отмене, удаляется из очереди: простаивающий цикл не копит их.
/// </summary>
internal sealed class LockAsyncAutoResetEvent
{
    private readonly Lock _lock = new();
    private readonly LinkedList<TaskCompletionSource> _waiters = new();
    private bool _signaled;

    /// <summary>Число ожидающих в очереди (тесты).</summary>
    internal int WaiterCount
    {
        get
        {
            lock (_lock)
                return _waiters.Count;
        }
    }

    public void Set()
    {
        lock (_lock)
            SetLocked();
    }

    public async Task<bool> WaitAsync(TimeSpan timeout, TimeProvider time, CancellationToken ct)
    {
        LinkedListNode<TaskCompletionSource> node;
        lock (_lock)
        {
            if (_signaled)
            {
                _signaled = false;
                return true;
            }
            if (timeout == TimeSpan.Zero)
                return false;
            node = _waiters.AddLast(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        }
        try
        {
            await node.Value.Task.WaitAsync(timeout, time, ct);
            return true;
        }
        catch (TimeoutException)
        {
            // Set мог снять ожидающего и завершить его уже после таймаута: тогда сигнал доставлен — true.
            return !Retire(node, passOn: false);
        }
        catch (OperationCanceledException)
        {
            // Сигнал, доставленный ожидающему в момент отмены, уходит следующему или в латч.
            Retire(node, passOn: true);
            throw;
        }
    }

    // Завершение под lock'ом безопасно (RunContinuationsAsynchronously). Ожидающий снимается из очереди и завершается
    // атомарно — либо Set'ом, либо Retire, — поэтому снятый из очереди всегда завершается успешно.
    private void SetLocked()
    {
        while (_waiters.First is { } first)
        {
            _waiters.RemoveFirst();
            if (first.Value.TrySetResult())
                return;
        }
        _signaled = true;
    }

    /// <summary>true — ожидающий снят этим вызовом; false — его уже завершил Set (сигнал доставлен).</summary>
    private bool Retire(LinkedListNode<TaskCompletionSource> node, bool passOn)
    {
        lock (_lock)
        {
            if (node.List is not null)
            {
                _waiters.Remove(node);
                node.Value.TrySetCanceled(CancellationToken.None);
                return true;
            }
            if (passOn)
                SetLocked();
            return false;
        }
    }
}
