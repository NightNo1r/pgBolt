using System.Collections.Frozen;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Распределение общего пула между очередями — внутренняя стратегия <see cref="SlotAllocator"/>.
/// Порядок вызовов за итерацию диспетчера: <see cref="BeginPlan"/> → <see cref="Next"/> → dequeue → <see cref="OnTaken"/>
/// на каждый занятый слот → <see cref="Next"/> → … → null. План может оборваться без финального <see cref="Next"/>
/// (остановка, ошибка dequeue). Все вызовы идут под lock allocator'а.
/// </summary>
internal interface ISharedPoolDistribution
{
    /// <summary>
    /// Граница плана: вызывается перед первым <see cref="Next"/> плана, в том числе для плана без записей, с тем же
    /// набором кандидаток (isDue вычислен один раз, ни одна очередь ещё не выдана).
    /// </summary>
    void BeginPlan(int queueCount, Func<int, bool> candidate);

    /// <summary>Следующая запись плана выборки.</summary>
    /// <param name="queueCount">Число очередей; индексы — порядок объявления.</param>
    /// <param name="candidate">Очередь ещё не выдавалась в этом плане и её nextPoll наступил.</param>
    /// <param name="available">min(свободные Reserved очереди + freeShared, MaxConcurrency − занято), ≥ 0.</param>
    /// <param name="freeReserved">Свободные Reserved-слоты очереди.</param>
    /// <param name="freeShared">
    /// Остаток общего пула плана: свободные общие слоты на начало плана минус общие, взятые в этом плане; освободившиеся
    /// посреди плана не входят.
    /// </param>
    /// <returns>
    /// Индекс очереди и n ≥ 1; null — план исчерпан. null допустим, только если ни у одной кандидатки нет available ≥ 1
    /// (иначе SlotAllocator бросает InvalidOperationException).
    /// </returns>
    (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available,
                             Func<int, int> freeReserved, int freeShared);

    /// <summary><see cref="SlotAllocator.Take"/> занял слот очереди; fromReserved — то, что вернул Take.</summary>
    void OnTaken(int queue, bool fromReserved);
}

/// <summary>Строгий порядок объявления: первая кандидатка со свободным слотом получает всё, что ей доступно.</summary>
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
/// Счётчики слотов виртуального сервера и план выборки: Reserved очереди доступны только ей, общий
/// пул — Workers; MaxConcurrency ограничивает очередь целиком. Порядок и доли общего пула задаёт
/// <see cref="ISharedPoolDistribution"/>: без весов — строгий порядок объявления, с весом хотя бы у одной очереди —
/// <see cref="WeightedDistribution"/> (очереди без веса — вес 1). Потокобезопасен.
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
    /// Остаток общего пула текущего плана: свободные общие на его начало минус взятые с тех пор; вне плана не читается.
    /// Внутри плана не больше FreeShared: Take уменьшает оба, Release увеличивает только FreeShared.
    /// </summary>
    private int _planBudget;

    /// <param name="weights">Текущие веса очередей (ordinal); читается под lock на границе плана, должен быть дешёвым.</param>
    public SlotAllocator(int workers, IReadOnlyList<QueueOptions> queues, Func<FrozenDictionary<string, int>>? weights = null)
        : this(workers, queues, new StrictOrderDistribution())
    {
        _weights = weights;
    }

    /// <summary>Фиксированная стратегия, веса не читаются.</summary>
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

    /// <summary>Текущая стратегия; для тестов.</summary>
    internal ISharedPoolDistribution Distribution
    {
        get
        {
            lock (_lock)
                return _distribution;
        }
    }

    /// <summary>
    /// План итерации [(queue, n)]. Ленивый: следующая запись вычисляется при MoveNext, то есть после
    /// Take по предыдущей, поэтому недобранные общие слоты переходят к следующим очередям той же итерации.
    /// Очередь — не больше раза за план; только isDue и n ≥ 1.
    /// Общий пул плана — свободные общие слоты на его начало: слоты, освободившиеся посреди плана, ждут следующего.
    /// Иначе после пачки одновременно закончившихся задач план, начатый по первому освобождению, отдавал бы остальные
    /// слоты следующим записям мимо порядка и весов стратегии. Reserved-слоты этим не ограничиваются. Планы одного
    /// allocator'а не перекрываются (их строит один диспетчер).
    /// </summary>
    /// <param name="isDue">nextPoll очереди наступил; вызывается под lock, должен быть дешёвым.</param>
    public IEnumerable<(string Queue, int N)> Plan(Func<string, bool> isDue)
    {
        ArgumentNullException.ThrowIfNull(isDue);
        return PlanIterator(isDue);
    }

    /// <summary>min(свободные Reserved очереди + свободные общие, MaxConcurrency − занято), ≥ 0.</summary>
    public int Available(string queue)
    {
        lock (_lock)
            return AvailableCore(Get(queue), FreeShared);
    }

    /// <summary>Занимает один слот: сначала Reserved очереди, затем общий.</summary>
    /// <returns>true — слот из Reserved, false — общий.</returns>
    /// <exception cref="InvalidOperationException">Свободных слотов для очереди нет.</exception>
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

    /// <summary>Возвращает слот того же вида, что выдал <see cref="Take"/>.</summary>
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

    /// <summary>Занято слотов очереди (reserved + общие).</summary>
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
    /// Следующая запись по текущим счётчикам; под lock (вне итератора: yield под lock запрещён). isDue вычисляется один
    /// раз на запись (в candidates), чтобы стратегия и проверка её ответа видели один и тот же набор кандидаток: иначе
    /// очередь, ставшая готовой между ними по ходу времени, дала бы ложное нарушение контракта. Первая запись плана
    /// (<paramref name="beginPlan"/>) сообщает стратегии границу плана.
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
    /// Применяет снимок весов на границе плана, под lock: внутри плана все Next и OnTaken приходят в один экземпляр
    /// стратегии. Новая стратегия создаётся, только если изменился вектор весов этого сервера (w[q], 0 — без веса): новый
    /// снимок с тем же вектором сохраняет состояние. Новая WeightedDistribution начинает с нулевого pass у всех очередей,
    /// поэтому отставание и опережение, накопленные при старых весах, забываются без залпа.
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
