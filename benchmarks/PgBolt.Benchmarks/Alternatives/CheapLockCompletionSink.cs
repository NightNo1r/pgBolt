using System.Collections.Concurrent;
using PgBolt.Benchmarks.Copies;

namespace PgBolt.Benchmarks.Alternatives;

/// <summary>
/// Вариант (б) «дешёвый lock»: та же логика, что <see cref="LockCompletionSink"/>, но под lock только создание
/// незапущенной задачи отправки и её регистрация в _sends (StopAsync видит её в снимке и дожидается); Start() и
/// ContinueWith (снятие из _sends) — после выхода из lock.
/// </summary>
/// <remarks>
/// Риск: задача, зарегистрированная под lock, запускается вызывающим потоком после lock. Если он не дойдёт до Start
/// (исключение между ними — здесь его неоткуда взять), StopAsync будет ждать её вечно.
/// </remarks>
internal sealed class CheapLockCompletionSink : ICompletionSink
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

    public CheapLockCompletionSink(ExecutionTable table, AckSendStub sender, int ackBatchSize, TimeSpan ackFlushInterval, MonoClock clock)
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
            _ackTimer ??= _clock.Time.CreateTimer(static s => ((CheapLockCompletionSink)s!).OnAckTimer(), this,
                Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            if (_ackBuffer.Count > 0)
                _ackTimer.Change(_ackFlushInterval, Timeout.InfiniteTimeSpan);
        }
    }

    public bool Complete(Execution execution, JobOutcome outcome)
    {
        execution.Outcome = outcome;
        PendingSend send;
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
            send = EnqueueLocked(execution);
        }
        send.Start(_sends);
        return true;
    }

    public async Task StopAsync(CancellationToken ct)
    {
        using var abort = ct.Register(static s => ((CancellationTokenSource)s!).Cancel(), _abort);
        while (true)
        {
            Task[] pending;
            PendingSend send;
            lock (_lock)
            {
                send = FlushAckLocked();
                pending = [.. _sends.Keys];
                if (pending.Length == 0)
                {
                    _stopped = true;
                    _ackTimer?.Dispose();
                    return;
                }
            }
            send.Start(_sends);
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

    private PendingSend EnqueueLocked(Execution execution)
    {
        if (execution.Outcome is JobOutcome.Ack)
        {
            _ackBuffer.Add(execution);
            if (_ackBuffer.Count >= _ackBatchSize)
                return FlushAckLocked();
            if (_ackBuffer.Count == 1)
                _ackTimer?.Change(_ackFlushInterval, Timeout.InfiniteTimeSpan);
            return default;
        }
        return CreateSendLocked(() => _sender.SendSingleAsync(execution));
    }

    private void OnAckTimer()
    {
        PendingSend send;
        lock (_lock)
            send = FlushAckLocked();
        send.Start(_sends);
    }

    private PendingSend FlushAckLocked()
    {
        if (_ackBuffer.Count == 0)
            return default;
        var batch = _ackBuffer;
        _ackBuffer = new List<Execution>(Math.Min(_ackBatchSize, 1024));
        _ackTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        return CreateSendLocked(() => _sender.SendAckBatchAsync(batch));
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

    /// <summary>Под lock: незапущенная задача и её прокси (Unwrap) в _sends — StopAsync, снявший снимок, её дождётся.</summary>
    private PendingSend CreateSendLocked(Func<Task> send)
    {
        var start = new Task<Task>(send);
        var task = start.Unwrap();
        _sends.TryAdd(task, 0);
        return new PendingSend(start, task);
    }

    private readonly record struct PendingSend(Task<Task>? Outer, Task? Task)
    {
        /// <summary>Вне lock: запуск и снятие из _sends по завершении.</summary>
        public void Start(ConcurrentDictionary<Task, byte> sends)
        {
            if (Outer is null)
                return;
            Outer.Start(TaskScheduler.Default);
            _ = Task!.ContinueWith(static (t, s) => ((ConcurrentDictionary<Task, byte>)s!).TryRemove(t, out _), sends,
                CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }
    }
}
