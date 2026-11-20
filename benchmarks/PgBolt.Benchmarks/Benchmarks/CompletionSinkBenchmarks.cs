using BenchmarkDotNet.Attributes;
using PgBolt.Benchmarks.Alternatives;
using PgBolt.Benchmarks.Copies;
using PgBolt.Benchmarks.Infrastructure;

namespace PgBolt.Benchmarks.Benchmarks;

/// <summary>
/// Пропускная способность CompletionSink.Complete(ack) из N рабочих потоков; AckBatchSize = 100 и AckFlushInterval = 20 мс
/// (умолчания PgBoltServerOptions), настоящий таймер TimeProvider.System, отправка пакета — заглушка (Task.Run + переходы
/// состояний). 120 000 Complete на вызов поровну между потоками; результат — время на один Complete (агрегированно).
/// </summary>
/// <remarks>
/// Записи переиспользуются: 4 поколения по 120 000, каждый поток перед своими Complete возвращает свою долю поколения в
/// Running (≈1 нс на запись, одинаково для всех вариантов). Отправки предыдущего использования поколения к этому моменту
/// давно завершены (три вызова назад).
/// </remarks>
[Config(typeof(BenchConfig))]
public class CompletionSinkBenchmarks
{
    private const int TotalOps = 120_000;
    private const int Generations = 4;
    private const int AckBatchSize = 100;
    private static readonly TimeSpan AckFlushInterval = TimeSpan.FromMilliseconds(20);
    private static readonly JobOutcome Ack = new JobOutcome.Ack();

    private WorkerPool _pool = null!;
    private Execution[][] _executions = null!;
    private ICompletionSink _lock = null!, _cheap = null!, _lockFree = null!, _lockFreeExact = null!;
    private Action<int> _runLock = null!, _runCheap = null!, _runLockFree = null!, _runLockFreeExact = null!;
    private int _generation;

    [Params(1, 4, 8, 12)]
    public int Threads { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _pool = new WorkerPool(Threads, "completer");
        _executions = new Execution[Generations][];
        for (var g = 0; g < Generations; g++)
        {
            _executions[g] = new Execution[TotalOps];
            for (var i = 0; i < TotalOps; i++)
                _executions[g][i] = new Execution(Guid.NewGuid(), Guid.NewGuid(), "default", 0, ExecutionState.Running);
        }
        var clock = new MonoClock(TimeProvider.System);
        _lock = new LockCompletionSink(new ExecutionTable(), new AckSendStub(new ExecutionTable()), AckBatchSize, AckFlushInterval, clock);
        _cheap = new CheapLockCompletionSink(new ExecutionTable(), new AckSendStub(new ExecutionTable()), AckBatchSize, AckFlushInterval, clock);
        _lockFree = new LockFreeCompletionSink(new ExecutionTable(), new AckSendStub(new ExecutionTable()), AckBatchSize, AckFlushInterval, clock);
        _lockFreeExact = new LockFreeCompletionSink(new ExecutionTable(), new AckSendStub(new ExecutionTable()), AckBatchSize, AckFlushInterval, clock,
            exactTrigger: true);
        _lock.Start();
        _cheap.Start();
        _lockFree.Start();
        _lockFreeExact.Start();
        _runLock = CreateWork(_lock);
        _runCheap = CreateWork(_cheap);
        _runLockFree = CreateWork(_lockFree);
        _runLockFreeExact = CreateWork(_lockFreeExact);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        Task.WaitAll(_lock.StopAsync(CancellationToken.None), _cheap.StopAsync(CancellationToken.None),
            _lockFree.StopAsync(CancellationToken.None), _lockFreeExact.StopAsync(CancellationToken.None));
        _pool.Dispose();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = TotalOps)]
    public void Complete_Lock() => Run(_runLock);

    [Benchmark(OperationsPerInvoke = TotalOps)]
    public void Complete_CheapLock() => Run(_runCheap);

    [Benchmark(OperationsPerInvoke = TotalOps)]
    public void Complete_LockFree() => Run(_runLockFree);

    /// <summary>Lock-free с дренажом только на инкременте, давшем ровно AckBatchSize (без шторма CAS на флаге дренажёра).</summary>
    [Benchmark(OperationsPerInvoke = TotalOps)]
    public void Complete_LockFreeExactTrigger() => Run(_runLockFreeExact);

    private void Run(Action<int> work)
    {
        _pool.Run(work);
        _generation = (_generation + 1) % Generations;
    }

    private Action<int> CreateWork(ICompletionSink sink)
    {
        var perThread = TotalOps / Threads;
        return index =>
        {
            var executions = _executions[_generation];
            var from = index * perThread;
            var to = from + perThread;
            for (var i = from; i < to; i++)
                executions[i].ResetForReuse();
            for (var i = from; i < to; i++)
                sink.Complete(executions[i], Ack);
        };
    }
}
