namespace PgBolt.Server.Runtime;

/// <summary>
/// Взвешенный round-robin общего пула на уровне слотов (stride scheduling). У очереди виртуальное время <c>pass</c> в «кругах»:
/// каждый взятый общий слот прибавляет 1 / вес, поэтому за круг, в котором очереди с backlog взяли слоты по весам, pass каждой
/// растёт на 1. Учёт — по факту взятия (<see cref="OnTaken"/>), а не по выданным записям: доли не зависят от числа свободных
/// слотов и от того, как план разбит на записи. Reserved-слоты весами не распределяются и pass не меняют.
/// </summary>
internal sealed class WeightedDistribution : ISharedPoolDistribution
{
    private readonly double[] _stride;
    private readonly bool[] _short;
    private readonly int[] _n;
    private readonly int[] _share;
    private readonly int[] _took;
    private readonly bool[] _capped;
    private readonly bool[] _wasCandidate;
    private readonly double[] _copy;
    private readonly int[] _drawn;
    private readonly int[] _sharedCap;

    public WeightedDistribution(IReadOnlyList<int> weights)
    {
        ArgumentNullException.ThrowIfNull(weights);
        _stride = [.. weights.Select(w => 1.0 / w)];
        Pass = new double[weights.Count];
        _short = new bool[weights.Count];
        _n = new int[weights.Count];
        _share = new int[weights.Count];
        _took = new int[weights.Count];
        _capped = new bool[weights.Count];
        _wasCandidate = new bool[weights.Count];
        _copy = new double[weights.Count];
        _drawn = new int[weights.Count];
        _sharedCap = new int[weights.Count];
    }

    /// <summary>Виртуальное время очередей; для тестов.</summary>
    internal double[] Pass { get; }

    public void OnTaken(int queue, bool fromReserved)
    {
        _took[queue]++;
        if (!fromReserved)
            Pass[queue] += _stride[queue];
    }

    /// <summary>
    /// Итог прошлого плана. Очереди, взявшие общую долю целиком и не упёршиеся в MaxConcurrency (F), взяли бы ещё — их pass
    /// и есть текущее виртуальное время V. Недобравшая, упёршаяся или неготовая очередь подтягивается к V, чтобы простой
    /// и недобор не копили преимущества (иначе после появления задач она получила бы залп); готовая очередь без доли
    /// отстаёт не больше чем на круг, а набравшая остатки плана опережает не больше чем на два. Затем pass нормируется к V.
    /// Неготовой считается и очередь, не бывшая кандидаткой в подводимом плане: иначе очередь, ставшая готовой после простоя,
    /// считалась бы готовой без доли и отставала бы на круг — залп до веса слотов. Без F (все недобирают) текущего времени
    /// нет, pass не трогается.
    /// </summary>
    public void BeginPlan(int queueCount, Func<int, bool> candidate)
    {
        var v = double.PositiveInfinity;
        for (var q = 0; q < queueCount; q++)
        {
            if (_share[q] >= 1)
            {
                _short[q] = _took[q] < _n[q];
                if (!_short[q] && !_capped[q])
                    v = Math.Min(v, Pass[q]);
            }
        }

        if (!double.IsPositiveInfinity(v))
        {
            for (var q = 0; q < queueCount; q++)
            {
                var inF = _share[q] >= 1 && !_short[q] && !_capped[q];
                if (!inF)
                {
                    var floor = _took[q] < _n[q] || _capped[q] || !_wasCandidate[q] || !candidate(q) ? v : v - 1;
                    Pass[q] = Math.Max(Pass[q], floor);
                }
                Pass[q] = Math.Min(Pass[q], v + 2) - v;
            }
        }

        for (var q = 0; q < queueCount; q++)
            _wasCandidate[q] = candidate(q);
        Array.Clear(_n);
        Array.Clear(_share);
        Array.Clear(_took);
        Array.Clear(_capped);
    }

    /// <summary>
    /// Разыгрывает свободные общие слоты на копии pass среди кандидаток (розыгрыш свой в каждом вызове: уже взятое учтено
    /// в pass) и выдаёт запись одной из выигравших: сначала недобравшей в прошлый раз — вероятный недобор раньше, чтобы
    /// остаток достался очередям с backlog, — затем с меньшим pass, затем по порядку объявления. Без выигравших — первой
    /// очереди со свободными Reserved.
    /// </summary>
    public (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available,
                                    Func<int, int> freeReserved, int freeShared)
    {
        var any = false;
        for (var q = 0; q < queueCount; q++)
        {
            _copy[q] = Pass[q];
            _drawn[q] = 0;
            any |= candidate(q) && available(q) >= 1;
            // Предел общих слотов очереди для розыгрыша — один раз, а не на каждый слот; -1 — не кандидатка.
            _sharedCap[q] = candidate(q) ? available(q) - freeReserved(q) : -1;
        }
        if (!any)
            return null;

        for (var slot = 0; slot < freeShared; slot++)
        {
            var winner = -1;
            for (var q = 0; q < queueCount; q++)
            {
                if (_drawn[q] < _sharedCap[q] && (winner < 0 || _copy[q] < _copy[winner]))
                    winner = q;
            }
            if (winner < 0)
                break;
            _copy[winner] += _stride[winner];
            _drawn[winner]++;
        }

        var p = -1;
        for (var q = 0; q < queueCount; q++)
        {
            if (_drawn[q] >= 1 && (p < 0 || (_short[q] && !_short[p]) || (_short[q] == _short[p] && Pass[q] < Pass[p])))
                p = q;
        }
        if (p >= 0)
        {
            var sharedCap = available(p) - freeReserved(p);
            _share[p] = _drawn[p];
            _n[p] = freeReserved(p) + _drawn[p];
            _capped[p] = _drawn[p] == sharedCap && sharedCap < freeShared;
            return (p, _n[p]);
        }

        for (var q = 0; q < queueCount; q++)
        {
            if (candidate(q) && available(q) >= 1 && freeReserved(q) >= 1)
            {
                _n[q] = freeReserved(q);
                return (q, _n[q]);
            }
        }
        return null;
    }
}
