using BenchmarkDotNet.Attributes;
using PgBolt.Benchmarks.Copies;
using PgBolt.Benchmarks.Infrastructure;

namespace PgBolt.Benchmarks.Benchmarks;

/// <summary>Фон для бенчмарков таблицы: таблица на Entries записей и потоки, непрерывно делающие Add/Remove своих записей.</summary>
internal sealed class TableLoad : IDisposable
{
    private readonly Thread[] _writers;
    private volatile bool _stop;

    public TableLoad(int entries, int backgroundWriters)
    {
        Table = new ExecutionTable();
        for (var i = 0; i < entries; i++)
            Table.Add(NewExecution(1_000_000_000 + i));
        _writers = new Thread[backgroundWriters];
        for (var w = 0; w < backgroundWriters; w++)
        {
            var own = Enumerable.Range(0, 16).Select(i => NewExecution(2_000_000_000 + i)).ToArray();
            _writers[w] = new Thread(() =>
            {
                var i = 0;
                while (!_stop)
                {
                    var e = own[i++ & 15];
                    Table.Add(e);
                    Table.Remove(e);
                }
            }) { IsBackground = true, Name = $"table-writer-{w}" };
            _writers[w].Start();
        }
    }

    public ExecutionTable Table { get; }

    public static Execution NewExecution(long validUntil) =>
        new(Guid.NewGuid(), Guid.NewGuid(), "default", validUntil, ExecutionState.Running);

    public void Dispose()
    {
        _stop = true;
        foreach (var writer in _writers)
            writer.Join();
    }
}

/// <summary>Время Snapshot при Entries записях и фоне из Writers потоков Add/Remove: текущий (List(Count)) против без Count.</summary>
[Config(typeof(BenchConfig))]
public class ExecutionTableSnapshotBenchmarks
{
    private TableLoad _load = null!;

    [Params(100, 10_000)]
    public int Entries { get; set; }

    [Params(0, 4)]
    public int Writers { get; set; }

    [GlobalSetup]
    public void Setup() => _load = new TableLoad(Entries, Writers);

    [GlobalCleanup]
    public void Cleanup() => _load.Dispose();

    [Benchmark(Baseline = true)]
    public int Snapshot_Current() => _load.Table.Snapshot().Count;

    [Benchmark]
    public int Snapshot_WithoutCount() => _load.Table.SnapshotWithoutCount().Count;
}

/// <summary>
/// Вычисление ближайшего рубежа, как в LeaseKeeper.RearmDeadlineTimer: Snapshot() + цикл (текущий) против минимума прямо
/// по перечислению без List.
/// </summary>
[Config(typeof(BenchConfig))]
public class ExecutionTableMinDeadlineBenchmarks
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(10);
    private readonly MonoClock _clock = new(TimeProvider.System);
    private TableLoad _load = null!;

    [Params(100, 10_000)]
    public int Entries { get; set; }

    [Params(0, 4)]
    public int Writers { get; set; }

    [GlobalSetup]
    public void Setup() => _load = new TableLoad(Entries, Writers);

    [GlobalCleanup]
    public void Cleanup() => _load.Dispose();

    [Benchmark(Baseline = true)]
    public long MinDeadline_ViaSnapshot()
    {
        var next = long.MaxValue;
        foreach (var execution in _load.Table.Snapshot())
            if (execution.State == ExecutionState.Running && execution.FirstReason == CancelReason.None)
                next = Math.Min(next, _clock.Add(execution.ValidUntil, -Interval));
        return next;
    }

    [Benchmark]
    public long MinDeadline_Enumerate() => _load.Table.MinWatchedDeadline(_clock, Interval);
}

/// <summary>
/// Пропускная способность Add+Remove из N потоков при Entries записях, пока отдельный поток непрерывно (без пауз —
/// худший случай «периодического» снимка) делает: ничего / Snapshot / Snapshot без Count / минимум по перечислению.
/// 120 000 пар Add+Remove на вызов; результат — время на пару (агрегированно).
/// </summary>
[Config(typeof(BenchConfig))]
public class ExecutionTableWriteBenchmarks
{
    private const int TotalOps = 120_000;
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(10);

    private readonly MonoClock _clock = new(TimeProvider.System);
    private TableLoad _load = null!;
    private WorkerPool _pool = null!;
    private Thread _snapshotter = null!;
    private readonly ManualResetEventSlim _snapshotGo = new();
    private readonly ManualResetEventSlim _snapshotIdle = new(true);
    private volatile int _mode;     // 0 — нет, 1 — Snapshot, 2 — без Count, 3 — минимум по перечислению, -1 — выход
    private Action<int> _write = null!;
    private long _sink;

    [Params(1, 4, 8, 12)]
    public int Threads { get; set; }

    [Params(100, 10_000)]
    public int Entries { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _load = new TableLoad(Entries, backgroundWriters: 0);
        _pool = new WorkerPool(Threads, "table-writer");
        var own = Enumerable.Range(0, Threads)
            .Select(_ => Enumerable.Range(0, 16).Select(i => TableLoad.NewExecution(2_000_000_000 + i)).ToArray())
            .ToArray();
        var perThread = TotalOps / Threads;
        var table = _load.Table;
        _write = index =>
        {
            var mine = own[index];
            for (var i = 0; i < perThread; i++)
            {
                var e = mine[i & 15];
                table.Add(e);
                table.Remove(e);
            }
        };
        _snapshotter = new Thread(SnapshotLoop) { IsBackground = true, Name = "snapshotter" };
        _snapshotter.Start();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _mode = -1;
        _snapshotGo.Set();
        _snapshotter.Join();
        _pool.Dispose();
        _load.Dispose();
    }

    [Benchmark(OperationsPerInvoke = TotalOps)]
    public void Writes_NoSnapshot() => Run(0);

    [Benchmark(Baseline = true, OperationsPerInvoke = TotalOps)]
    public void Writes_SnapshotCurrent() => Run(1);

    [Benchmark(OperationsPerInvoke = TotalOps)]
    public void Writes_SnapshotWithoutCount() => Run(2);

    [Benchmark(OperationsPerInvoke = TotalOps)]
    public void Writes_MinDeadlineEnumerate() => Run(3);

    private void Run(int mode)
    {
        if (mode != 0)
        {
            _mode = mode;
            _snapshotIdle.Reset();
            _snapshotGo.Set();
        }
        _pool.Run(_write);
        if (mode != 0)
        {
            _snapshotGo.Reset();
            _mode = 0;
            _snapshotIdle.Wait();
        }
    }

    private void SnapshotLoop()
    {
        var table = _load.Table;
        while (true)
        {
            _snapshotGo.Wait();
            var mode = _mode;
            if (mode == -1)
                return;
            while ((mode = _mode) > 0)
            {
                _sink += mode switch
                {
                    1 => table.Snapshot().Count,
                    2 => table.SnapshotWithoutCount().Count,
                    _ => table.MinWatchedDeadline(_clock, Interval),
                };
            }
            _snapshotIdle.Set();
            if (mode == -1)
                return;
        }
    }
}
