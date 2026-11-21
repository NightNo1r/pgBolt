using System.Collections.Concurrent;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Load;
using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Slo;

namespace PgBolt.LoadTest.Run;

/// <summary>
/// Общее состояние прогона одного режима: его пишут обработчик задачи (из DI хостов) и продюсер, читают окна,
/// срез и сверка корректности.
/// </summary>
/// <param name="capacity">Верхняя оценка числа задач прогона: размер внешнего массива счётчика выполнений.</param>
internal sealed class RunContext(long capacity, QueueProfile queues = QueueProfile.Standard)
{
    private long _executed;

    public FetchRecorder? Fetches { get; init; }
    public QueueProfile Queues { get; } = queues;
    public ExecutionCounter Counter { get; } = new(capacity);

    /// <summary>Выполнений всего, включая падения первой попытки.</summary>
    public long Executed => Interlocked.Read(ref _executed);

    public LatencyRecorder Immediate { get; } = new();
    public LatencyRecorder Delayed { get; } = new();

    /// <summary><c>Seq</c> задач из вызовов <c>EnqueueManyAsync</c>, завершившихся исключением.</summary>
    public ConcurrentBag<long> FailedCallSeqs { get; } = [];

    /// <summary><c>Seq</c> задач, падающих на первой попытке.</summary>
    public ConcurrentBag<long> FailFirstSeqs { get; } = [];

    public void CountExecution(long seq)
    {
        Counter.Increment(seq);
        Interlocked.Increment(ref _executed);
    }

    /// <summary>
    /// Лишние выполнения среди первых <paramref name="issued"/> <c>Seq</c> для прогона без дренажа: выполнений больше
    /// ожидаемого — дубль и без дренажа, а нехватку не отличить от задачи, которая ещё не дошла. Вызывать после
    /// остановки хостов.
    /// </summary>
    public long DuplicatedExecutions(long issued) =>
        ExecutionReconciler.Reconcile(Counter, issued, FailFirstSeqs.ToHashSet(), FailedCallSeqs.ToHashSet()).Duplicated;
}
