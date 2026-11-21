using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using PgBolt.Benchmarks.Alternatives;
using PgBolt.Benchmarks.Copies;
using PgBolt.Benchmarks.Infrastructure;

namespace PgBolt.Benchmarks.Benchmarks;

/// <summary>
/// One dispatcher calls Available and Take as in the fetch loop, while N threads release slots as jobs finish. SPSC rings transfer slots without
/// Interlocked in both variants. Queues q0 and q1 each reserve 16 slots. q1 also has MaxConcurrency 128. The shared pool has 256 workers.
/// Reports aggregate time per pair across 200,000 Take/Release pairs per invocation.
/// </summary>
[Config(typeof(BenchConfig))]
public class SlotAllocatorBenchmarks
{
    private const int TotalOps = 200_000;
    private const int RingSize = 256;
    private static readonly string[] QueueNames = ["q0", "q1"];
    private static readonly SlotQueue[] Queues = [new("q0", 16, null), new("q1", 16, 128)];

    private WorkerPool _pool = null!;
    private ISlotAllocator _lock = null!, _interlocked = null!;
    private Ring[] _rings = null!;
    private Action<int> _runLock = null!, _runInterlocked = null!;
    private volatile bool _dispatchDone;

    /// <summary>Number of release threads. The dispatcher adds one more, for totals of 2, 4, 8, or 12.</summary>
    [Params(1, 3, 7, 11)]
    public int Releasers { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _pool = new WorkerPool(Releasers + 1, "slots");
        _lock = new LockSlotAllocator(256, Queues);
        _interlocked = new InterlockedSlotAllocator(256, Queues);
        _rings = new Ring[Releasers];
        for (var i = 0; i < Releasers; i++)
            _rings[i] = new Ring();
        _runLock = CreateWork(_lock);
        _runInterlocked = CreateWork(_interlocked);
    }

    [GlobalCleanup]
    public void Cleanup() => _pool.Dispose();

    [Benchmark(Baseline = true, OperationsPerInvoke = TotalOps)]
    public void TakeRelease_Lock() => Run(_runLock);

    [Benchmark(OperationsPerInvoke = TotalOps)]
    public void TakeRelease_Interlocked() => Run(_runInterlocked);

    private void Run(Action<int> work)
    {
        _dispatchDone = false;
        _pool.Run(work);
    }

    private Action<int> CreateWork(ISlotAllocator allocator) => index =>
    {
        if (index == 0)
            Dispatch(allocator);
        else
            ReleaseLoop(allocator, _rings[index - 1]);
    };

    private void Dispatch(ISlotAllocator allocator)
    {
        var rings = _rings;
        var r = 0;
        var spin = new SpinWait();
        for (var done = 0; done < TotalOps;)
        {
            var q = done & 1;
            var queue = QueueNames[q];
            var ring = rings[r];
            if (ring.Head.Value - Volatile.Read(ref ring.Tail.Value) >= RingSize || allocator.Available(queue) == 0)
            {
                r = r + 1 == rings.Length ? 0 : r + 1;
                spin.SpinOnce(sleep1Threshold: -1);
                continue;
            }
            spin.Reset();
            var fromReserved = allocator.Take(queue);
            var head = ring.Head.Value;
            ring.Items[head & (RingSize - 1)] = (byte)(q * 2 + (fromReserved ? 1 : 0));
            Volatile.Write(ref ring.Head.Value, head + 1);
            r = r + 1 == rings.Length ? 0 : r + 1;
            done++;
        }
        _dispatchDone = true;
    }

    private void ReleaseLoop(ISlotAllocator allocator, Ring ring)
    {
        var tail = ring.Tail.Value;
        var spin = new SpinWait();
        while (true)
        {
            if (tail < Volatile.Read(ref ring.Head.Value))
            {
                var item = ring.Items[tail & (RingSize - 1)];
                allocator.Release(QueueNames[item >> 1], (item & 1) != 0);
                Volatile.Write(ref ring.Tail.Value, ++tail);
                spin.Reset();
                continue;
            }
            if (_dispatchDone && tail == Volatile.Read(ref ring.Head.Value))
                return;
            spin.SpinOnce(sleep1Threshold: -1);
        }
    }

    private sealed class Ring
    {
        public readonly byte[] Items = new byte[RingSize];
        public readonly PaddedLong Head = new();   // пишет диспетчер
        public readonly PaddedLong Tail = new();   // пишет освобождающий
    }
}

/// <summary>A long in the middle of a 256-byte object avoids false sharing (128-byte cache lines on Apple M).</summary>
[StructLayout(LayoutKind.Explicit, Size = 256)]
internal sealed class PaddedLong
{
    [FieldOffset(128)]
    public long Value;
}
