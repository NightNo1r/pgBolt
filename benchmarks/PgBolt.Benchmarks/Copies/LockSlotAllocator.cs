namespace PgBolt.Benchmarks.Copies;

// Копия ядра синхронизации: src/PgBolt.Server/Runtime/SlotAllocator.cs @ 52b4a8f.
// Дословно: Available, Take, Release, InUse, AvailableCore, Get, QueueSlots. Опущено: Plan/NextEntry и
// ISharedPoolDistribution (план выборки — не горячий путь Take/Release). QueueOptions → SlotQueue (имя, Reserved, MaxConcurrency).

/// <summary>Queue configuration for the allocator, standing in for QueueOptions.</summary>
internal sealed record SlotQueue(string Name, int Reserved, int? MaxConcurrency);

/// <summary>Shared contract for allocator variants.</summary>
internal interface ISlotAllocator
{
    int Available(string queue);

    bool Take(string queue);

    void Release(string queue, bool fromReserved);

    int InUse(string queue);

    /// <summary>For verification, reports the queue's (ReservedInUse, SharedInUse) and total SharedInUse.</summary>
    (int ReservedInUse, int SharedInUse, int TotalShared) Counters(string queue);
}

/// <summary>Current implementation. One lock protects all counters.</summary>
internal sealed class LockSlotAllocator : ISlotAllocator
{
    private readonly Lock _lock = new();
    private readonly int _workers;
    private readonly Dictionary<string, QueueSlots> _queues;
    private int _sharedInUse;

    public LockSlotAllocator(int workers, IReadOnlyList<SlotQueue> queues)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(workers);
        ArgumentNullException.ThrowIfNull(queues);
        _workers = workers;
        _queues = new Dictionary<string, QueueSlots>(queues.Count, StringComparer.Ordinal);
        foreach (var q in queues)
            _queues.Add(q.Name, new QueueSlots(q.Name, q.Reserved, q.MaxConcurrency));
    }

    public int Available(string queue)
    {
        lock (_lock)
            return AvailableCore(Get(queue));
    }

    public bool Take(string queue)
    {
        lock (_lock)
        {
            var slots = Get(queue);
            if (AvailableCore(slots) == 0)
                throw new InvalidOperationException($"No free slot for queue '{queue}'.");
            if (slots.ReservedInUse < slots.Reserved)
            {
                slots.ReservedInUse++;
                return true;
            }
            slots.SharedInUse++;
            _sharedInUse++;
            return false;
        }
    }

    public void Release(string queue, bool fromReserved)
    {
        lock (_lock)
        {
            var slots = Get(queue);
            if (fromReserved)
            {
                if (slots.ReservedInUse == 0)
                    throw new InvalidOperationException($"No reserved slot of queue '{queue}' is taken.");
                slots.ReservedInUse--;
            }
            else
            {
                if (slots.SharedInUse == 0)
                    throw new InvalidOperationException($"No shared slot of queue '{queue}' is taken.");
                slots.SharedInUse--;
                _sharedInUse--;
            }
        }
    }

    public int InUse(string queue)
    {
        lock (_lock)
        {
            var slots = Get(queue);
            return slots.ReservedInUse + slots.SharedInUse;
        }
    }

    public (int ReservedInUse, int SharedInUse, int TotalShared) Counters(string queue)
    {
        lock (_lock)
        {
            var slots = Get(queue);
            return (slots.ReservedInUse, slots.SharedInUse, _sharedInUse);
        }
    }

    private int AvailableCore(QueueSlots slots)
    {
        var free = slots.Reserved - slots.ReservedInUse + (_workers - _sharedInUse);
        if (slots.MaxConcurrency is int max)
            free = Math.Min(free, max - slots.ReservedInUse - slots.SharedInUse);
        return Math.Max(0, free);
    }

    private QueueSlots Get(string queue) =>
        _queues.TryGetValue(queue, out var slots)
            ? slots
            : throw new ArgumentException($"Queue '{queue}' is not declared on this server.", nameof(queue));

    private sealed class QueueSlots(string name, int reserved, int? maxConcurrency)
    {
        public string Name { get; } = name;
        public int Reserved { get; } = reserved;
        public int? MaxConcurrency { get; } = maxConcurrency;
        public int ReservedInUse { get; set; }
        public int SharedInUse { get; set; }
    }
}
