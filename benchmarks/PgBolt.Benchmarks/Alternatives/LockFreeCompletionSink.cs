using System.Collections.Concurrent;
using PgBolt.Benchmarks.Copies;

namespace PgBolt.Benchmarks.Alternatives;

/// <summary>
/// Variant (c), lock-free acknowledgments using ConcurrentQueue, a counter, a single drainer elected by CAS, and a Dekker-style stop protocol.
/// The prototype supports only acknowledgments. Other production outcomes are sent individually and throw NotSupportedException here.
/// </summary>
/// <remarks>
/// <para>
/// Complete publishes via Enqueue followed by Interlocked.Increment. At AckBatchSize it tries to claim _draining by CAS from 0 to 1. If that
/// fails, the holder releases with Interlocked.Exchange (a full barrier), rereads the count, and drains a full batch. Either the CAS sees 0 or
/// the holder sees the increment.
/// </para>
/// <para>
/// The timer is never manually disarmed. After publishing, Complete arms it by changing _timerArmed from 0 to 1 with CAS. When the timer fires,
/// it clears the flag with Exchange and drains all entries. A later entry is either visible to this drain or its Complete sees 0 and arms the
/// timer. If the drainer is busy, the timer is armed again. Unlike production, the window starts at the first entry after the last timer firing,
/// so the tail may flush earlier but never later than the window.
/// </para>
/// <para>
/// As in production, stop flushes and waits while sends remain, sets _stopped to 1 with Interlocked.Exchange, acquires the drainer flag
/// permanently, drains once more, and waits for all sends. After publishing, Complete rereads _stopped after the increment's full barrier. On 1
/// it tries to reclaim its entry (OutcomePending to Removed). If reclaiming fails, the final drain already owns it and Complete returns true. If
/// it sees 0, the final drain is guaranteed to see the entry.
/// </para>
/// </remarks>
internal sealed class LockFreeCompletionSink : ICompletionSink
{
    private readonly ExecutionTable _table;
    private readonly AckSendStub _sender;
    private readonly int _ackBatchSize;
    private readonly TimeSpan _ackFlushInterval;
    private readonly MonoClock _clock;
    private readonly bool _exactTrigger;
    private readonly ConcurrentQueue<Execution> _acks = new();
    private readonly ConcurrentDictionary<Task, byte> _sends = new();
    private readonly CancellationTokenSource _abort = new();
    private ITimer? _ackTimer;
    private int _count;       // опубликовано и ещё не забрано дренажёром; ≤ длины очереди (инкремент — после Enqueue)
    private int _draining;    // 1 — есть дренажёр; после остановки — навсегда 1
    private int _stopped;
    private int _timerArmed;

    /// <param name="exactTrigger">
    /// If false, every Complete that sees count ≥ AckBatchSize tries to drain, generating failed CAS operations on one cache line while the drainer
    /// is busy. If true, only the call incrementing the count to exactly AckBatchSize tries. The count rises one at a time and must pass that value.
    /// The flag holder rechecks for a full batch after releasing the flag.
    /// </param>
    public LockFreeCompletionSink(ExecutionTable table, AckSendStub sender, int ackBatchSize, TimeSpan ackFlushInterval, MonoClock clock,
        bool exactTrigger = false)
    {
        _exactTrigger = exactTrigger;
        _table = table;
        _sender = sender;
        _ackBatchSize = ackBatchSize;
        _ackFlushInterval = ackFlushInterval;
        _clock = clock;
    }

    public void Start()
    {
        var timer = _clock.Time.CreateTimer(static s => ((LockFreeCompletionSink)s!).OnAckTimer(), this,
            Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        if (Interlocked.CompareExchange(ref _ackTimer, timer, null) is not null)
        {
            timer.Dispose();
            return;
        }
        // Записи, принятые до Start, могли взвести флаг без таймера — один безусловный взвод их покрывает.
        Interlocked.Exchange(ref _timerArmed, 1);
        timer.Change(_ackFlushInterval, Timeout.InfiniteTimeSpan);
    }

    public bool Complete(Execution execution, JobOutcome outcome)
    {
        if (outcome is not JobOutcome.Ack)
            throw new NotSupportedException("The lock-free prototype covers the ack path only.");
        execution.Outcome = outcome;
        if (Volatile.Read(ref _stopped) != 0)
        {
            if (execution.TryTransition(ExecutionState.Running, ExecutionState.Removed))
                _table.Remove(execution);
            return false;
        }
        if (!execution.TryTransition(ExecutionState.Running, ExecutionState.OutcomePending))
            return false;

        _acks.Enqueue(execution);
        var count = Interlocked.Increment(ref _count);     // полный барьер: публикация → чтение _stopped/_timerArmed
        if (Volatile.Read(ref _stopped) != 0)
        {
            if (!execution.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed))
                return true;                                 // забрана финальным дренажом — будет отправлена
            _table.Remove(execution);
            return false;
        }
        if (_exactTrigger ? count == _ackBatchSize : count >= _ackBatchSize)
            Drain(all: false);
        if (Volatile.Read(ref _timerArmed) == 0)
            ArmTimer();
        return true;
    }

    public async Task StopAsync(CancellationToken ct)
    {
        using var abort = ct.Register(static s => ((CancellationTokenSource)s!).Cancel(), _abort);
        try
        {
            while (true)
            {
                FlushAll();
                Task[] pending = [.. _sends.Keys];
                if (pending.Length == 0)
                    break;
                await Task.WhenAll(pending).WaitAsync(ct);
            }

            Interlocked.Exchange(ref _stopped, 1);
            AcquireDrainerForever();
            while (TakeBatch(all: true) is { } batch)
                StartSend(batch);
            while (_sends.Keys is { Count: > 0 } last)
                await Task.WhenAll(last).WaitAsync(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            await _abort.CancelAsync();
            Interlocked.Exchange(ref _stopped, 1);
            AcquireDrainerForever();
            while (_acks.TryDequeue(out var execution))
                if (execution.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed))
                    _table.Remove(execution);
        }
        _ackTimer?.Dispose();
    }

    private void OnAckTimer()
    {
        Interlocked.Exchange(ref _timerArmed, 0);          // полный барьер: сброс флага → чтение очереди
        if (Volatile.Read(ref _stopped) != 0)
            return;
        if (!Drain(all: true))
            ArmTimer();                                      // дренажёр занят: хвост остаётся под таймером
    }

    private void ArmTimer()
    {
        if (Interlocked.CompareExchange(ref _timerArmed, 1, 0) == 0)
            Volatile.Read(ref _ackTimer)?.Change(_ackFlushInterval, Timeout.InfiniteTimeSpan);
    }

    /// <returns>False if the drainer flag was busy on the first attempt. Its holder will recheck the count.</returns>
    private bool Drain(bool all)
    {
        var acquired = false;
        while (true)
        {
            if (Interlocked.CompareExchange(ref _draining, 1, 0) != 0)
                return acquired;
            acquired = true;
            try
            {
                while (TakeBatch(all) is { } batch)
                    StartSend(batch);
            }
            finally
            {
                Interlocked.Exchange(ref _draining, 0);      // полный барьер: освобождение → перечитывание счётчика
            }
            all = false;
            if (Volatile.Read(ref _count) < _ackBatchSize)
                return true;
        }
    }

    /// <summary>During shutdown, waits for the current drainer and acquires its flag, like a blocking Drain(all).</summary>
    private void FlushAll()
    {
        var spin = new SpinWait();
        while (Interlocked.CompareExchange(ref _draining, 1, 0) != 0)
            spin.SpinOnce();
        try
        {
            while (TakeBatch(all: true) is { } batch)
                StartSend(batch);
        }
        finally
        {
            Interlocked.Exchange(ref _draining, 0);
        }
        if (Volatile.Read(ref _count) >= _ackBatchSize)
            Drain(all: false);
    }

    private void AcquireDrainerForever()
    {
        var spin = new SpinWait();
        while (Interlocked.CompareExchange(ref _draining, 1, 0) != 0)
            spin.SpinOnce();
    }

    /// <summary>Only the flag holder can drain. With all = false, drains only full batches according to the count.</summary>
    private List<Execution>? TakeBatch(bool all)
    {
        if (!all && Volatile.Read(ref _count) < _ackBatchSize)
            return null;
        List<Execution>? batch = null;
        while (_acks.TryDequeue(out var execution))
        {
            (batch ??= new List<Execution>(Math.Min(_ackBatchSize, 1024))).Add(execution);
            if (batch.Count == _ackBatchSize)
                break;
        }
        if (batch is null)
            return null;
        Interlocked.Add(ref _count, -batch.Count);
        return batch;
    }

    private void StartSend(List<Execution> batch)
    {
        var task = Task.Run(() => _sender.SendAckBatchAsync(batch));
        _sends.TryAdd(task, 0);
        _ = task.ContinueWith(static (t, s) => ((ConcurrentDictionary<Task, byte>)s!).TryRemove(t, out _), _sends,
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }
}
