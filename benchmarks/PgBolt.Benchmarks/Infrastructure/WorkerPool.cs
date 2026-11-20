using System.Runtime.ExceptionServices;

namespace PgBolt.Benchmarks.Infrastructure;

/// <summary>
/// N выделенных потоков для конкурентных бенчмарков: <see cref="Run"/> одновременно стартует работу на всех (Barrier) и
/// ждёт, пока все закончат. Потоки создаются один раз (GlobalSetup), чтобы не мерить их создание.
/// </summary>
internal sealed class WorkerPool : IDisposable
{
    private readonly Thread[] _threads;
    private readonly Barrier _barrier;
    private Action<int>? _work;
    private volatile bool _stop;
    private ExceptionDispatchInfo? _error;

    public WorkerPool(int count, string name)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);
        _barrier = new Barrier(count + 1);
        _threads = new Thread[count];
        for (var i = 0; i < count; i++)
        {
            _threads[i] = new Thread(Loop) { IsBackground = true, Name = $"{name}-{i}" };
            _threads[i].Start(i);
        }
    }

    public int Count => _threads.Length;

    /// <summary>Выполняет work(i) на каждом потоке i, одновременный старт; возвращается, когда закончили все.</summary>
    public void Run(Action<int> work)
    {
        _work = work;
        _barrier.SignalAndWait();
        _barrier.SignalAndWait();
        if (Interlocked.Exchange(ref _error, null) is { } error)
            error.Throw();
    }

    public void Dispose()
    {
        _stop = true;
        _barrier.SignalAndWait();
        foreach (var thread in _threads)
            thread.Join();
        _barrier.Dispose();
    }

    private void Loop(object? state)
    {
        var index = (int)state!;
        while (true)
        {
            _barrier.SignalAndWait();
            if (_stop)
                return;
            try
            {
                _work!(index);
            }
            catch (Exception e)
            {
                Interlocked.CompareExchange(ref _error, ExceptionDispatchInfo.Capture(e), null);
            }
            _barrier.SignalAndWait();
        }
    }
}
