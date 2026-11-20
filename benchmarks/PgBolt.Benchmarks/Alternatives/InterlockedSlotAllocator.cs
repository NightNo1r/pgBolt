using PgBolt.Benchmarks.Copies;

namespace PgBolt.Benchmarks.Alternatives;

/// <summary>
/// Lock-free аллокатор: счётчики — Interlocked. Take и Available вызывает только поток диспетчера (единственный, кто
/// увеличивает счётчики); Release — любые потоки: Decrement счётчика очереди, затем общего.
/// </summary>
/// <remarks>
/// Почему не превышаются лимиты: счётчики растут только в Take (один поток), Release лишь уменьшает их — любое
/// прочитанное диспетчером значение не меньше истинного, свободных слотов он видит не больше, чем есть. Промежуточное
/// состояние Release (очередь уже освобождена, общий счётчик ещё нет) занижает свободный общий пул (консервативно),
/// а ограничение MaxConcurrency считается по счётчикам очереди. Отрицательный счётчик после Decrement — двойной Release:
/// откат и InvalidOperationException, как в продукте. План выборки (Plan) читал бы счётчики так же — без атомарного
/// снимка, только консервативно.
/// </remarks>
internal sealed class InterlockedSlotAllocator : ISlotAllocator
{
    private readonly int _workers;
    private readonly Dictionary<string, QueueSlots> _queues;
    private int _sharedInUse;

    public InterlockedSlotAllocator(int workers, IReadOnlyList<SlotQueue> queues)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(workers);
        ArgumentNullException.ThrowIfNull(queues);
        _workers = workers;
        _queues = new Dictionary<string, QueueSlots>(queues.Count, StringComparer.Ordinal);
        foreach (var q in queues)
            _queues.Add(q.Name, new QueueSlots(q.Reserved, q.MaxConcurrency));
    }

    /// <summary>Только поток диспетчера.</summary>
    public int Available(string queue) => AvailableCore(Get(queue));

    /// <summary>Только поток диспетчера.</summary>
    public bool Take(string queue)
    {
        var slots = Get(queue);
        if (AvailableCore(slots) == 0)
            throw new InvalidOperationException($"No free slot for queue '{queue}'.");
        if (Volatile.Read(ref slots.ReservedInUse) < slots.Reserved)
        {
            Interlocked.Increment(ref slots.ReservedInUse);
            return true;
        }
        Interlocked.Increment(ref slots.SharedInUse);
        Interlocked.Increment(ref _sharedInUse);
        return false;
    }

    public void Release(string queue, bool fromReserved)
    {
        var slots = Get(queue);
        if (fromReserved)
        {
            if (Interlocked.Decrement(ref slots.ReservedInUse) < 0)
            {
                Interlocked.Increment(ref slots.ReservedInUse);
                throw new InvalidOperationException($"No reserved slot of queue '{queue}' is taken.");
            }
        }
        else
        {
            if (Interlocked.Decrement(ref slots.SharedInUse) < 0)
            {
                Interlocked.Increment(ref slots.SharedInUse);
                throw new InvalidOperationException($"No shared slot of queue '{queue}' is taken.");
            }
            Interlocked.Decrement(ref _sharedInUse);
        }
    }

    public int InUse(string queue)
    {
        var slots = Get(queue);
        return Volatile.Read(ref slots.ReservedInUse) + Volatile.Read(ref slots.SharedInUse);
    }

    public (int ReservedInUse, int SharedInUse, int TotalShared) Counters(string queue)
    {
        var slots = Get(queue);
        return (Volatile.Read(ref slots.ReservedInUse), Volatile.Read(ref slots.SharedInUse), Volatile.Read(ref _sharedInUse));
    }

    private int AvailableCore(QueueSlots slots)
    {
        var reservedInUse = Volatile.Read(ref slots.ReservedInUse);
        var sharedOfQueue = Volatile.Read(ref slots.SharedInUse);
        var free = slots.Reserved - reservedInUse + (_workers - Volatile.Read(ref _sharedInUse));
        if (slots.MaxConcurrency is int max)
            free = Math.Min(free, max - reservedInUse - sharedOfQueue);
        return Math.Max(0, free);
    }

    private QueueSlots Get(string queue) =>
        _queues.TryGetValue(queue, out var slots)
            ? slots
            : throw new ArgumentException($"Queue '{queue}' is not declared on this server.", nameof(queue));

    private sealed class QueueSlots(int reserved, int? maxConcurrency)
    {
        public readonly int Reserved = reserved;
        public readonly int? MaxConcurrency = maxConcurrency;
        public int ReservedInUse;
        public int SharedInUse;
    }
}
