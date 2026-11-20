using System.Collections.Concurrent;
using PgBolt.Benchmarks.Copies;

namespace PgBolt.Benchmarks.Alternatives;

/// <summary>
/// Вариант (в) lock-free ack-путь: ConcurrentQueue + счётчик + единственный дренажёр (флаг через CAS) + остановка по Деккеру.
/// Прототип поддерживает только ack (остальные исходы в продукте уходят поштучно — здесь NotSupportedException).
/// </summary>
/// <remarks>
/// <para>
/// Пакет: Complete публикует запись (Enqueue, затем Interlocked.Increment счётчика). Достигнут AckBatchSize — пытается
/// стать дренажёром (CAS 0→1 на _draining); не вышло — держатель после освобождения (Interlocked.Exchange — полный
/// барьер) перечитает счётчик и заберёт полный пакет (Деккер: либо CAS увидит 0, либо держатель увидит инкремент).
/// </para>
/// <para>
/// Окно: таймер вручную не снимается. Флаг _timerArmed: Complete после публикации взводит таймер, если флаг 0 (CAS 0→1);
/// срабатывание сначала сбрасывает флаг (Exchange), затем забирает всё. Деккер: запись, опубликованная после сброса,
/// либо видна этому дренажу, либо её Complete видит флаг 0 и взводит таймер — хвост не остаётся без таймера. Не удалось
/// стать дренажёром — таймер взводится снова. Отличие от продукта: окно отсчитывается от первого элемента после
/// последнего срабатывания таймера, а не после последнего сброса, — хвост может уйти раньше, но не позже окна.
/// </para>
/// <para>
/// Остановка: как в продукте, пока есть отправки — сброс и ожидание; затем _stopped = 1 (Interlocked.Exchange), захват
/// флага дренажёра навсегда, финальный дренаж и ожидание всех отправок. Complete после публикации перечитывает _stopped
/// (полный барьер инкремента): видит 1 — пытается вернуть запись (OutcomePending → Removed); не вышло — её уже забрал
/// финальный дренаж, исход будет отправлен (true). Не видит 1 — финальный дренаж гарантированно видит запись.
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
    /// false — дренаж пробует каждый Complete, увидевший count ≥ AckBatchSize (пока дренажёр занят, это шторм неудачных
    /// CAS на одной кэш-линии); true — только тот, чей инкремент дал ровно AckBatchSize. Достаточно: счётчик растёт
    /// только инкрементами по одному и всегда проходит через это значение, а состояния count ≥ AckBatchSize, возникшие
    /// при занятом флаге, перепроверяет держатель после освобождения.
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

    /// <returns>false — флаг дренажёра занят с первой попытки (его держатель перепроверит счётчик).</returns>
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

    /// <summary>Остановка: ждёт текущего держателя и берёт флаг (как Drain(all) с ожиданием).</summary>
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

    /// <summary>Только держатель флага. all = false — только полный пакет (по счётчику).</summary>
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
