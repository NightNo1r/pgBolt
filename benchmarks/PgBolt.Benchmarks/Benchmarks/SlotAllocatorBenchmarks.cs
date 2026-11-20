using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using PgBolt.Benchmarks.Alternatives;
using PgBolt.Benchmarks.Copies;
using PgBolt.Benchmarks.Infrastructure;

namespace PgBolt.Benchmarks.Benchmarks;

/// <summary>
/// 1 поток диспетчера (Available + Take, как цикл выборки) и N потоков, освобождающих слоты (Release, как завершение
/// задач). Слоты передаются освобождающим через SPSC-кольца (без Interlocked, одинаково для обоих вариантов).
/// Очереди: q0 (Reserved 16), q1 (Reserved 16, MaxConcurrency 128); общий пул Workers = 256. 200 000 пар Take/Release
/// на вызов; результат — время на пару (агрегированно).
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

    /// <summary>Число освобождающих потоков (всего потоков — на один больше: 2, 4, 8, 12).</summary>
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

/// <summary>long посередине 256-байтного объекта — без ложного разделения кэш-линий (128 байт на Apple M).</summary>
[StructLayout(LayoutKind.Explicit, Size = 256)]
internal sealed class PaddedLong
{
    [FieldOffset(128)]
    public long Value;
}
