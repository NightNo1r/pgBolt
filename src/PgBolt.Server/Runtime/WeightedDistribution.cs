namespace PgBolt.Server.Runtime;

/// <summary>
/// Distributes shared slots by weighted round-robin (stride scheduling). Each queue tracks virtual time <c>pass</c> in
/// rounds. Each shared slot taken adds 1 / weight, so serving backlogged queues in proportion to their weights advances
/// each by one per round. Accounting happens when a slot is taken (<see cref="OnTaken"/>), independently of how many
/// slots are free or how plan entries are grouped. Reserved slots are outside this distribution and never change pass.
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

    /// <summary>Queue virtual times, exposed for tests.</summary>
    internal double[] Pass { get; }

    public void OnTaken(int queue, bool fromReserved)
    {
        _took[queue]++;
        if (!fromReserved)
            Pass[queue] += _stride[queue];
    }

    /// <summary>
    /// Closes the previous plan. Queues that took their full share and stayed below MaxConcurrency (F) would have taken
    /// more. Their pass defines current virtual time V. A queue that underfilled, hit its limit, or was ineligible
    /// catches up to V, preventing a burst of accumulated credit when jobs arrive. A ready queue with no share trails by
    /// at most one round; a queue that absorbed leftovers leads by at most two. Then pass is normalized to V. A queue
    /// outside the previous plan's candidates also counts as ineligible; otherwise becoming ready after an idle period
    /// could create a one-round lag and a burst of up to its weight in slots. Without F (all queues underfilled), there
    /// is no current time and pass stays unchanged.
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
    /// Simulates shared-slot allocation among candidates on a copy of pass. Repeats the simulation each call because pass
    /// already reflects slots taken. Returns one winning queue, preferring a queue that underfilled last time so any
    /// leftover slots can reach backlogged queues. Breaks further ties by lowest pass, then declaration order. With no
    /// winners, returns the first queue with free Reserved slots.
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
