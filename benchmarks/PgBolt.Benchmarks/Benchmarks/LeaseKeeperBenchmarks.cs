using BenchmarkDotNet.Attributes;
using PgBolt.Benchmarks.Alternatives;
using PgBolt.Benchmarks.Copies;
using PgBolt.Benchmarks.Infrastructure;

namespace PgBolt.Benchmarks.Benchmarks;

/// <summary>Background deadline timer rearming.</summary>
public enum RearmMode
{
    /// <summary>No background rearming.</summary>
    None,

    /// <summary>Rearms about once per millisecond with Thread.Sleep(1).</summary>
    Periodic1ms,

    /// <summary>
    /// Rearms about every 10 µs using a spin wait. With 100 entries, Rearm takes several microseconds, so the lock is often held. Without a pause,
    /// repeated acquisition of the unfair lock starves registration and measures a pathological case (see README).
    /// </summary>
    Periodic10us,
}

/// <summary>Ordering of deadlines for new registrations.</summary>
public enum DeadlineOrder
{
    /// <summary>
    /// Each new deadline is later than the armed one, matching increasing validUntil = fetchStartedAt + LeaseDuration in production. ArmAtMost
    /// leaves the timer alone.
    /// </summary>
    Increasing,

    /// <summary>Each new deadline is earlier than all others, forcing ArmAtMost to lower it and call Change every time (worst case).</summary>
    Decreasing,
}

/// <summary>
/// Measures LeaseKeeper.Register (new Execution, Add, ArmAtMost) and rollback via Unregister (Running to Removed, then Remove) on the dispatcher
/// benchmark thread. Uses 100 background entries, a separate Rearm thread, and a stub timer. Reports time per pair across 10,000
/// Register/Unregister pairs per invocation.
/// </summary>
[Config(typeof(BenchConfig))]
public class LeaseKeeperBenchmarks
{
    private const int Ops = 10_000;
    private const int BackgroundEntries = 100;
    private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromTicks(1_000);

    private ILeaseDeadlines _lock = null!, _epoch = null!;
    private ExecutionTable _lockTable = null!, _epochTable = null!;
    private Thread? _rearmer;
    private volatile bool _stop;
    private long _next;
    private (Guid JobId, Guid LeaseId)[] _ids = null!;   // id готовы заранее, как у взятой из БД задачи

    [Params(RearmMode.None, RearmMode.Periodic1ms, RearmMode.Periodic10us)]
    public RearmMode Rearm { get; set; }

    [Params(DeadlineOrder.Increasing, DeadlineOrder.Decreasing)]
    public DeadlineOrder Order { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        (_lock, _lockTable) = Create(static (t, c, i) => new LockLeaseDeadlines(t, c, i));
        (_epoch, _epochTable) = Create(static (t, c, i) => new EpochLeaseDeadlines(t, c, i));
        // Фон — рубежи около 1e9; растущие начинаются позже всех, убывающие — раньше всех.
        _next = Order == DeadlineOrder.Increasing ? 3_000_000_000 : 900_000_000;
        _ids = Enumerable.Range(0, Ops).Select(_ => (Guid.NewGuid(), Guid.NewGuid())).ToArray();
        if (Rearm == RearmMode.None)
            return;
        _rearmer = new Thread(() =>
        {
            while (!_stop)
            {
                _lock.Rearm();
                _epoch.Rearm();
                if (Rearm == RearmMode.Periodic1ms)
                    Thread.Sleep(1);
                else
                    SpinFor(TimeSpan.FromMicroseconds(10));
            }
        }) { IsBackground = true, Name = "rearmer" };
        _rearmer.Start();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _stop = true;
        _rearmer?.Join();
        _lock.Stop();
        _epoch.Stop();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Ops)]
    public int Register_Lock() => Run(_lock, _lockTable);

    [Benchmark(OperationsPerInvoke = Ops)]
    public int Register_Epoch() => Run(_epoch, _epochTable);

    private int Run(ILeaseDeadlines deadlines, ExecutionTable table)
    {
        var step = Order == DeadlineOrder.Increasing ? 1 : -1;
        var removed = 0;
        for (var i = 0; i < Ops; i++)
        {
            _next += step;
            var (jobId, leaseId) = _ids[i];
            var execution = deadlines.Register(jobId, leaseId, "default", _next);
            if (execution.TryTransition(ExecutionState.Running, ExecutionState.Removed) && table.Remove(execution))
                removed++;
        }
        return removed;
    }

    private static void SpinFor(TimeSpan duration)
    {
        var until = System.Diagnostics.Stopwatch.GetTimestamp() + (long)(duration.TotalSeconds * System.Diagnostics.Stopwatch.Frequency);
        while (System.Diagnostics.Stopwatch.GetTimestamp() < until)
            Thread.SpinWait(10);
    }

    private static (ILeaseDeadlines, ExecutionTable) Create(Func<ExecutionTable, MonoClock, TimeSpan, ILeaseDeadlines> factory)
    {
        var table = new ExecutionTable();
        for (var i = 0; i < BackgroundEntries; i++)
            table.Add(TableLoad.NewExecution(1_000_000_000 + i * 1_000));
        var deadlines = factory(table, new MonoClock(new StubTimeProvider()), HeartbeatInterval);
        deadlines.Start();
        return (deadlines, table);
    }
}
