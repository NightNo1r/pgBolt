using System.Collections.Concurrent;

namespace PgBolt.Benchmarks.Copies;

// Копия ядра синхронизации: src/PgBolt.Server/Runtime/CompletionSink.cs @ 52b4a8f.
// Дословно: Start, Complete, StopAsync, EnqueueLocked, OnAckTimer, FlushAckLocked, DiscardAckBufferLocked, StartSend.
// Заменено: SendAckBatchAsync/SendSingleAsync → AckSendStub (без БД и backoff); логирование опущено (холодные ветки);
// PgBoltServerOptions → (ackBatchSize, ackFlushInterval); EnqueueRegistered не нужен бенчмарку.

/// <summary>Вариант (а): текущий — всё под одним lock, включая Task.Run и ContinueWith в StartSend.</summary>
internal sealed class LockCompletionSink : ICompletionSink
{
    private readonly ExecutionTable _table;
    private readonly AckSendStub _sender;
    private readonly int _ackBatchSize;
    private readonly TimeSpan _ackFlushInterval;
    private readonly MonoClock _clock;
    private readonly Lock _lock = new();
    private readonly ConcurrentDictionary<Task, byte> _sends = new();
    private readonly CancellationTokenSource _abort = new();
    private List<Execution> _ackBuffer = [];
    private ITimer? _ackTimer;
    private bool _stopped;

    public LockCompletionSink(ExecutionTable table, AckSendStub sender, int ackBatchSize, TimeSpan ackFlushInterval, MonoClock clock)
    {
        _table = table;
        _sender = sender;
        _ackBatchSize = ackBatchSize;
        _ackFlushInterval = ackFlushInterval;
        _clock = clock;
    }

    public void Start()
    {
        lock (_lock)
        {
            _ackTimer ??= _clock.Time.CreateTimer(static s => ((LockCompletionSink)s!).OnAckTimer(), this,
                Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            if (_ackBuffer.Count > 0)
                _ackTimer.Change(_ackFlushInterval, Timeout.InfiniteTimeSpan);
        }
    }

    public bool Complete(Execution execution, JobOutcome outcome)
    {
        execution.Outcome = outcome;
        lock (_lock)
        {
            if (_stopped)
            {
                if (execution.TryTransition(ExecutionState.Running, ExecutionState.Removed))
                    _table.Remove(execution);
                return false;
            }
            if (!execution.TryTransition(ExecutionState.Running, ExecutionState.OutcomePending))
                return false;
            EnqueueLocked(execution);
            return true;
        }
    }

    public async Task StopAsync(CancellationToken ct)
    {
        using var abort = ct.Register(static s => ((CancellationTokenSource)s!).Cancel(), _abort);
        while (true)
        {
            Task[] pending;
            lock (_lock)
            {
                FlushAckLocked();
                pending = [.. _sends.Keys];
                if (pending.Length == 0)
                {
                    _stopped = true;
                    _ackTimer?.Dispose();
                    return;
                }
            }
            try
            {
                await Task.WhenAll(pending).WaitAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                await _abort.CancelAsync();
                lock (_lock)
                {
                    DiscardAckBufferLocked();
                    _stopped = true;
                    _ackTimer?.Dispose();
                }
                return;
            }
        }
    }

    private void EnqueueLocked(Execution execution)
    {
        if (execution.Outcome is JobOutcome.Ack)
        {
            _ackBuffer.Add(execution);
            if (_ackBuffer.Count >= _ackBatchSize)
                FlushAckLocked();
            else if (_ackBuffer.Count == 1)
                _ackTimer?.Change(_ackFlushInterval, Timeout.InfiniteTimeSpan);
            return;
        }
        StartSend(() => _sender.SendSingleAsync(execution));
    }

    private void OnAckTimer()
    {
        lock (_lock)
            FlushAckLocked();
    }

    private void FlushAckLocked()
    {
        if (_ackBuffer.Count == 0)
            return;
        var batch = _ackBuffer;
        _ackBuffer = new List<Execution>(Math.Min(_ackBatchSize, 1024));
        _ackTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        StartSend(() => _sender.SendAckBatchAsync(batch));
    }

    private void DiscardAckBufferLocked()
    {
        foreach (var execution in _ackBuffer)
        {
            if (!execution.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed))
                continue;
            _table.Remove(execution);
        }
        _ackBuffer.Clear();
        _ackTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    private void StartSend(Func<Task> send)
    {
        var task = Task.Run(send);
        _sends.TryAdd(task, 0);
        _ = task.ContinueWith(static (t, s) => ((ConcurrentDictionary<Task, byte>)s!).TryRemove(t, out _), _sends,
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }
}
