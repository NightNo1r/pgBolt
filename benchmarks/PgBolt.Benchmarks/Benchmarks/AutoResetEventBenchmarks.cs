using BenchmarkDotNet.Attributes;
using PgBolt.Benchmarks.Copies;
using PgBolt.Benchmarks.Infrastructure;

namespace PgBolt.Benchmarks.Benchmarks;

/// <summary>
/// N threads call Set while one waiter loops, as in the dispatcher with timed WaitAsync and worker Set calls. Splits 120,000 Set calls equally
/// among threads per invocation. Reports aggregate time per Set.
/// </summary>
[Config(typeof(BenchConfig))]
public class AutoResetEventSetBenchmarks
{
    private const int TotalOps = 120_000;
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(1);

    private WorkerPool _pool = null!;
    private LockAsyncAutoResetEvent _lockEvent = null!;
    private CasAsyncAutoResetEvent _casEvent = null!;
    private Task _lockWaiter = Task.CompletedTask;
    private Task _casWaiter = Task.CompletedTask;
    private Action<int> _setLock = null!;
    private Action<int> _setCas = null!;
    private volatile bool _stop;

    [Params(1, 4, 8, 12)]
    public int Threads { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _pool = new WorkerPool(Threads, "setter");
        _lockEvent = new LockAsyncAutoResetEvent();
        _casEvent = new CasAsyncAutoResetEvent();
        _lockWaiter = Task.Run(async () =>
        {
            while (!_stop)
                await _lockEvent.WaitAsync(WaitTimeout, TimeProvider.System, CancellationToken.None);
        });
        _casWaiter = Task.Run(async () =>
        {
            while (!_stop)
                await _casEvent.WaitAsync(WaitTimeout, TimeProvider.System, CancellationToken.None);
        });
        var perThread = TotalOps / Threads;
        _setLock = _ =>
        {
            var e = _lockEvent;
            for (var i = 0; i < perThread; i++)
                e.Set();
        };
        _setCas = _ =>
        {
            var e = _casEvent;
            for (var i = 0; i < perThread; i++)
                e.Set();
        };
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _stop = true;
        _lockEvent.Set();
        _casEvent.Set();
        Task.WaitAll(_lockWaiter, _casWaiter);
        _pool.Dispose();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = TotalOps)]
    public void Set_Lock() => _pool.Run(_setLock);

    [Benchmark(OperationsPerInvoke = TotalOps)]
    public void Set_Cas() => _pool.Run(_setCas);
}

/// <summary>
/// Ping-pong pairs Set(ping) and WaitAsync(pong) with an echo loop waiting on ping and setting pong. Measures one round trip, including two
/// thread-pool wakeups with RunContinuationsAsynchronously. The --latency mode reports individual round-trip percentiles.
/// </summary>
[Config(typeof(BenchConfig))]
public class AutoResetEventPingPongBenchmarks
{
    private const int RoundTrips = 1_000;
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    private LockAsyncAutoResetEvent _lockPing = null!, _lockPong = null!;
    private CasAsyncAutoResetEvent _casPing = null!, _casPong = null!;
    private Task _lockEcho = Task.CompletedTask, _casEcho = Task.CompletedTask;
    private volatile bool _stop;

    [GlobalSetup]
    public void Setup()
    {
        (_lockPing, _lockPong) = (new LockAsyncAutoResetEvent(), new LockAsyncAutoResetEvent());
        (_casPing, _casPong) = (new CasAsyncAutoResetEvent(), new CasAsyncAutoResetEvent());
        _lockEcho = Task.Run(async () =>
        {
            while (!_stop)
                if (await _lockPing.WaitAsync(WaitTimeout, TimeProvider.System, CancellationToken.None))
                    _lockPong.Set();
        });
        _casEcho = Task.Run(async () =>
        {
            while (!_stop)
                if (await _casPing.WaitAsync(WaitTimeout, TimeProvider.System, CancellationToken.None))
                    _casPong.Set();
        });
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _stop = true;
        _lockPing.Set();
        _casPing.Set();
        Task.WaitAll(_lockEcho, _casEcho);
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = RoundTrips)]
    public async Task<int> PingPong_Lock()
    {
        var delivered = 0;
        for (var i = 0; i < RoundTrips; i++)
        {
            _lockPing.Set();
            if (await _lockPong.WaitAsync(WaitTimeout, TimeProvider.System, CancellationToken.None))
                delivered++;
        }
        return delivered;
    }

    [Benchmark(OperationsPerInvoke = RoundTrips)]
    public async Task<int> PingPong_Cas()
    {
        var delivered = 0;
        for (var i = 0; i < RoundTrips; i++)
        {
            _casPing.Set();
            if (await _casPong.WaitAsync(WaitTimeout, TimeProvider.System, CancellationToken.None))
                delivered++;
        }
        return delivered;
    }
}

/// <summary>
/// Measures time and allocations for an idle WaitAsync without Set. A zero timeout returns false synchronously without a latch. A 1 ms timeout
/// creates a waiter and timer, handles TimeoutException internally, and removes the waiter.
/// </summary>
[Config(typeof(BenchConfig))]
public class AutoResetEventIdleWaitBenchmarks
{
    private readonly LockAsyncAutoResetEvent _lockEvent = new();
    private readonly CasAsyncAutoResetEvent _casEvent = new();
    private TimeSpan _timeout;

    [Params(0, 1)]
    public int TimeoutMs { get; set; }

    [GlobalSetup]
    public void Setup() => _timeout = TimeSpan.FromMilliseconds(TimeoutMs);

    [Benchmark(Baseline = true)]
    public Task<bool> IdleWait_Lock() => _lockEvent.WaitAsync(_timeout, TimeProvider.System, CancellationToken.None);

    [Benchmark]
    public Task<bool> IdleWait_Cas() => _casEvent.WaitAsync(_timeout, TimeProvider.System, CancellationToken.None);
}
