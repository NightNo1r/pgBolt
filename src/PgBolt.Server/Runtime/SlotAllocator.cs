using System.Collections.Frozen;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Internal <see cref="SlotAllocator"/> strategy for distributing shared slots among queues. Each dispatcher iteration
/// calls <see cref="BeginPlan"/>, then <see cref="Next"/>, dequeues jobs, calls <see cref="OnTaken"/> for each acquired
/// slot, and repeats <see cref="Next"/> until null. Shutdown or a dequeue error can end a plan without a final
/// <see cref="Next"/>. All calls hold the allocator lock.
/// </summary>
internal interface ISharedPoolDistribution
{
    /// <summary>
    /// Marks a plan boundary before the first <see cref="Next"/>, including for empty plans. Uses the same candidates:
    /// isDue has been evaluated once and no queue has yet appeared in the plan.
    /// </summary>
    void BeginPlan(int queueCount, Func<int, bool> candidate);

    /// <summary>Gets the next fetch plan entry.</summary>
    /// <param name="queueCount">Number of queues. Indexes follow declaration order.</param>
    /// <param name="candidate">True for a queue whose nextPoll is due and which has not appeared in this plan.</param>
    /// <param name="available">
    /// min(free Reserved slots for the queue + freeShared, MaxConcurrency − occupied), at least zero.
    /// </param>
    /// <param name="freeReserved">Free Reserved slots for the queue.</param>
    /// <param name="freeShared">
    /// Shared slots remaining in the plan: slots free at the start minus slots taken during the plan. Excludes slots
    /// freed mid-plan.
    /// </param>
    /// <returns>
    /// Queue index and n ≥ 1, or null when the plan is exhausted. Returning null while any candidate has available ≥ 1
    /// causes SlotAllocator to throw InvalidOperationException.
    /// </returns>
    (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available,
                             Func<int, int> freeReserved, int freeShared);

    /// <summary>
    /// Reports a slot acquired by <see cref="SlotAllocator.Take"/> for this queue. fromReserved is the value returned by
    /// Take.
    /// </summary>
    void OnTaken(int queue, bool fromReserved);
}

/// <summary>Strict declaration order. The first candidate with a free slot receives all slots available to it.</summary>
internal sealed class StrictOrderDistribution : ISharedPoolDistribution
{
    public void BeginPlan(int queueCount, Func<int, bool> candidate) { }

    public void OnTaken(int queue, bool fromReserved) { }

    public (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available,
                                    Func<int, int> freeReserved, int freeShared)
    {
        for (var i = 0; i < queueCount; i++)
        {
            if (!candidate(i))
                continue;
            var n = available(i);
            if (n >= 1)
                return (i, n);
        }
        return null;
    }
}

/// <summary>
/// Tracks slots and builds the fetch plan for a virtual server. A queue's Reserved slots belong only to that queue.
/// Workers sets the shared pool size, and MaxConcurrency caps total occupancy per queue.
/// <see cref="ISharedPoolDistribution"/> sets queue order and shares: strict declaration order with no weights, or
/// <see cref="WeightedDistribution"/> when any queue has a weight. Unweighted queues then use weight 1. Thread-safe.
/// </summary>
internal sealed class SlotAllocator
{
    private readonly Lock _lock = new();
    private readonly int _workers;
    private readonly Func<FrozenDictionary<string, int>>? _weights;
    private readonly QueueSlots[] _order;
    private readonly Dictionary<string, QueueSlots> _queues;
    private ISharedPoolDistribution _distribution;
    private FrozenDictionary<string, int>? _appliedSnapshot;
    private int[] _appliedVector;
    private int _sharedInUse;
    /// <summary>
    /// Shared slots left in the current plan: free at its start minus those taken since. Only read during the plan. Never
    /// exceeds FreeShared because Take decrements both values and Release increases only FreeShared.
    /// </summary>
    private int _planBudget;

    /// <param name="workers">Number of slots in the shared pool.</param>
    /// <param name="queues">Queue settings in declaration order.</param>
    /// <param name="weights">
    /// Current queue weights using ordinal comparison. Read under the lock at plan boundaries, so this lookup must be
    /// cheap.
    /// </param>
    public SlotAllocator(int workers, IReadOnlyList<QueueOptions> queues, Func<FrozenDictionary<string, int>>? weights = null)
        : this(workers, queues, new StrictOrderDistribution())
    {
        _weights = weights;
    }

    /// <summary>Uses a fixed strategy without reading weights.</summary>
    internal SlotAllocator(int workers, IReadOnlyList<QueueOptions> queues, ISharedPoolDistribution distribution)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(workers);
        ArgumentNullException.ThrowIfNull(queues);
        ArgumentNullException.ThrowIfNull(distribution);
        _workers = workers;
        _distribution = distribution;
        _order = [.. queues.Select((q, i) => new QueueSlots(i, q.Name, q.Reserved, q.MaxConcurrency))];
        _queues = new Dictionary<string, QueueSlots>(_order.Length, StringComparer.Ordinal);
        foreach (var slots in _order)
            _queues.Add(slots.Name, slots);
        _appliedVector = new int[_order.Length];
    }

    /// <summary>Current strategy, exposed for tests.</summary>
    internal ISharedPoolDistribution Distribution
    {
        get
        {
            lock (_lock)
                return _distribution;
        }
    }

    /// <summary>
    /// Lazy iteration plan of (queue, n) entries. MoveNext computes the next entry after Take on the previous one, so
    /// unused shared slots pass to later queues in the same iteration. Each queue appears at most once, only when isDue
    /// and n ≥ 1. Shared capacity is fixed at plan start. Slots freed mid-plan wait for the next plan, so a burst of
    /// completions cannot give later entries slots outside the strategy's queue order and weights. Reserved slots are
    /// exempt. A single dispatcher builds plans for an allocator, so plans never overlap.
    /// </summary>
    /// <param name="isDue">Whether the queue's nextPoll is due. Called under the lock and must be cheap.</param>
    public IEnumerable<(string Queue, int N)> Plan(Func<string, bool> isDue)
    {
        ArgumentNullException.ThrowIfNull(isDue);
        return PlanIterator(isDue);
    }

    /// <summary>min(free Reserved slots for the queue + free shared slots, MaxConcurrency − occupied), at least zero.</summary>
    public int Available(string queue)
    {
        lock (_lock)
            return AvailableCore(Get(queue), FreeShared);
    }

    /// <summary>Takes one slot, preferring a Reserved slot for this queue before shared capacity.</summary>
    /// <returns>True for a Reserved slot, false for a shared slot.</returns>
    /// <exception cref="InvalidOperationException">No slot is available for this queue.</exception>
    public bool Take(string queue)
    {
        lock (_lock)
        {
            var slots = Get(queue);
            if (AvailableCore(slots, FreeShared) == 0)
                throw new InvalidOperationException($"No free slot for queue '{queue}'.");
            var fromReserved = slots.ReservedInUse < slots.Reserved;
            if (fromReserved)
                slots.ReservedInUse++;
            else
            {
                slots.SharedInUse++;
                _sharedInUse++;
                _planBudget--;
            }
            _distribution.OnTaken(slots.Index, fromReserved);
            return fromReserved;
        }
    }

    /// <summary>Returns a slot of the same kind as <see cref="Take"/> allocated.</summary>
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

    /// <summary>Occupied slots for the queue (reserved + shared).</summary>
    public int InUse(string queue)
    {
        lock (_lock)
        {
            var slots = Get(queue);
            return slots.ReservedInUse + slots.SharedInUse;
        }
    }

    private IEnumerable<(string Queue, int N)> PlanIterator(Func<string, bool> isDue)
    {
        var planned = new bool[_order.Length];
        var candidates = new bool[_order.Length];
        for (var begin = true; NextEntry(planned, candidates, isDue, begin) is { } entry; begin = false)
        {
            planned[entry.Queue] = true;
            yield return (_order[entry.Queue].Name, entry.N);
        }
    }

    /// <summary>
    /// Builds the next entry using current counters under the lock, outside the iterator because yield under a lock is
    /// disallowed here. Evaluates isDue once per entry into candidates so the strategy and its result check see the same
    /// queues. A queue becoming due between those operations would otherwise trigger a false contract violation. The
    /// first entry (<paramref name="beginPlan"/>) tells the strategy where the plan starts.
    /// </summary>
    private (int Queue, int N)? NextEntry(bool[] planned, bool[] candidates, Func<string, bool> isDue, bool beginPlan)
    {
        lock (_lock)
        {
            for (var i = 0; i < _order.Length; i++)
                candidates[i] = !planned[i] && isDue(_order[i].Name);
            if (beginPlan)
            {
                ApplyWeights();
                _planBudget = FreeShared;
                _distribution.BeginPlan(_order.Length, i => candidates[i]);
            }
            var freeShared = Math.Max(_planBudget, 0);
            var next = _distribution.Next(_order.Length, i => candidates[i], i => AvailableCore(_order[i], freeShared),
                i => _order[i].Reserved - _order[i].ReservedInUse, freeShared);
            if (next is not { } entry)
            {
                // Контракт стратегии: план кончается, только когда нет готовых очередей со свободным слотом в пределах
                // бюджета плана. Иначе диспетчер (NextPollDelay = 0 для такой очереди) крутился бы без ожидания — лучше
                // явная ошибка и пауза. Слоты, освободившиеся посреди плана, раздаёт следующий план: диспетчер начинает его
                // сразу по освобождению слота.
                for (var i = 0; i < _order.Length; i++)
                {
                    if (candidates[i] && AvailableCore(_order[i], freeShared) >= 1)
                        throw new InvalidOperationException(
                            $"Shared pool distribution ended the plan while queue '{_order[i].Name}' is due and has free slots.");
                }
                return null;
            }
            if (entry.Queue < 0 || entry.Queue >= _order.Length || !candidates[entry.Queue]
                || entry.N < 1 || entry.N > AvailableCore(_order[entry.Queue], freeShared))
                throw new InvalidOperationException(
                    $"Shared pool distribution returned an invalid plan entry ({entry.Queue}, {entry.N}).");
            return entry;
        }
    }

    /// <summary>
    /// Applies the weight snapshot under the lock at the plan boundary so every Next and OnTaken within a plan reaches
    /// the same strategy instance. Replaces the strategy only if this server's weight vector changes (w[q], 0 for no
    /// weight). A new snapshot with the same vector preserves state. A fresh WeightedDistribution starts all queues at
    /// zero pass, discarding their accumulated lead or lag under the old weights without a burst.
    /// </summary>
    private void ApplyWeights()
    {
        if (_weights?.Invoke() is not { } snapshot || ReferenceEquals(snapshot, _appliedSnapshot))
            return;
        _appliedSnapshot = snapshot;
        int[] vector = [.. _order.Select(q => snapshot.GetValueOrDefault(q.Name))];
        if (vector.AsSpan().SequenceEqual(_appliedVector))
            return;
        _appliedVector = vector;
        _distribution = vector.All(w => w == 0)
            ? new StrictOrderDistribution()
            : new WeightedDistribution([.. vector.Select(w => w == 0 ? 1 : w)]);
    }

    private int FreeShared => _workers - _sharedInUse;

    private int AvailableCore(QueueSlots slots, int freeShared)
    {
        var free = slots.Reserved - slots.ReservedInUse + freeShared;
        if (slots.MaxConcurrency is int max)
            free = Math.Min(free, max - slots.ReservedInUse - slots.SharedInUse);
        return Math.Max(0, free);
    }

    private QueueSlots Get(string queue) =>
        _queues.TryGetValue(queue, out var slots)
            ? slots
            : throw new ArgumentException($"Queue '{queue}' is not declared on this server.", nameof(queue));

    private sealed class QueueSlots(int index, string name, int reserved, int? maxConcurrency)
    {
        public int Index { get; } = index;
        public string Name { get; } = name;
        public int Reserved { get; } = reserved;
        public int? MaxConcurrency { get; } = maxConcurrency;
        public int ReservedInUse { get; set; }
        public int SharedInUse { get; set; }
    }
}
