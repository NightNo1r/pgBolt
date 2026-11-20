using System.Collections.Frozen;
using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Unit;

/// <summary>Веса очередей из подменяемого источника: выбор стратегии и её смена на границе плана.</summary>
public sealed class SlotAllocatorWeightsTests
{
    private const double Tolerance = 1.25;
    private static readonly Func<string, bool> AllDue = _ => true;

    private FrozenDictionary<string, int> _weights = FrozenDictionary<string, int>.Empty;

    private static FrozenDictionary<string, int> Weights(params (string Queue, int Weight)[] weights) =>
        weights.ToFrozenDictionary(w => w.Queue, w => w.Weight, StringComparer.Ordinal);

    /// <summary>Вектор весов по очередям q0, q1, …; пустой — без весов.</summary>
    private static FrozenDictionary<string, int> Vector(int[] weights) =>
        Weights([.. weights.Select((w, i) => ($"q{i}", w))]);

    private SlotAllocator Create(int workers, int queues, Action<int, QueueOptions>? configure = null)
    {
        var o = new PgBoltServerOptions { Workers = workers };
        for (var i = 0; i < queues; i++)
        {
            var index = i;
            o.Queue($"q{i}", q => configure?.Invoke(index, q));
        }
        return new SlotAllocator(o.Workers, o.Queues, () => _weights);
    }

    /// <summary>Пустой план: граница плана без записей.</summary>
    private static void EmptyPlan(SlotAllocator slots) => _ = slots.Plan(_ => false).ToList();

    private static void AssertShare(long[] taken, int[] weights, int[] queues, int queue, string context)
    {
        var total = queues.Sum(q => taken[q]);
        var deviation = Math.Abs(taken[queue] - (double)total * weights[queue] / queues.Sum(q => weights[q]));
        Assert.True(deviation <= Tolerance,
            $"{context}: queue {queue} took {taken[queue]} of {total}, deviation {deviation:F3} > {Tolerance}");
    }

    private static long[] Since(long[] now, long[] baseline) => [.. now.Select((v, i) => v - baseline[i])];

    [Fact]
    public void No_weights_uses_strict_order()
    {
        var slots = Create(4, 2);

        EmptyPlan(slots);

        Assert.IsType<StrictOrderDistribution>(slots.Distribution);
        Assert.Equal([("q0", 4), ("q1", 4)], slots.Plan(AllDue));
    }

    [Fact]
    public void Any_weight_switches_to_weighted_with_default_one()
    {
        var slots = Create(4, 3);
        _weights = Weights(("q0", 4));

        EmptyPlan(slots);
        slots.Take("q0");
        slots.Take("q1");
        slots.Take("q2");

        var weighted = Assert.IsType<WeightedDistribution>(slots.Distribution);
        Assert.Equal([0.25, 1, 1], weighted.Pass);       // шаг pass — 1 / вес: [4, 1, 1]
    }

    [Fact]
    public void Foreign_queue_weights_are_ignored()
    {
        var slots = Create(4, 2);
        var strict = slots.Distribution;
        _weights = Weights(("other", 5));

        EmptyPlan(slots);

        Assert.Same(strict, slots.Distribution);
    }

    [Fact]
    public void Snapshot_applies_only_at_plan_boundary()
    {
        var slots = Create(4, 2);
        _weights = Weights(("q0", 3));

        using (var plan = slots.Plan(AllDue).GetEnumerator())
        {
            Assert.True(plan.MoveNext());
            var applied = Assert.IsType<WeightedDistribution>(slots.Distribution);
            _weights = FrozenDictionary<string, int>.Empty;
            slots.Take(plan.Current.Queue);
            Assert.True(plan.MoveNext());
            Assert.Same(applied, slots.Distribution);
        }

        EmptyPlan(slots);
        Assert.IsType<StrictOrderDistribution>(slots.Distribution);
    }

    [Fact]
    public void Changed_vector_resets_state()
    {
        var slots = Create(4, 2);
        var seen = new List<ISharedPoolDistribution> { slots.Distribution };

        foreach (var weights in new[] { Weights(("q0", 3)), Weights(("q0", 1), ("q1", 2)), FrozenDictionary<string, int>.Empty })
        {
            _weights = weights;
            EmptyPlan(slots);
            Assert.DoesNotContain(slots.Distribution, seen);
            if (slots.Distribution is WeightedDistribution weighted)
                Assert.Equal([0, 0], weighted.Pass);
            seen.Add(slots.Distribution);
            slots.Take("q0");                              // состояние, которое следующая смена должна сбросить
            slots.Release("q0", fromReserved: false);
        }
        Assert.IsType<StrictOrderDistribution>(slots.Distribution);
    }

    [Fact]
    public void Same_vector_in_new_snapshot_keeps_state()
    {
        var slots = Create(4, 2);
        _weights = Weights(("q0", 2), ("other", 7));
        EmptyPlan(slots);
        var weighted = Assert.IsType<WeightedDistribution>(slots.Distribution);
        slots.Take("q1");

        _weights = Weights(("q0", 2), ("other", 9));    // новый снимок: изменилась только чужая очередь
        EmptyPlan(slots);

        Assert.Same(weighted, slots.Distribution);
        Assert.NotEqual(0, weighted.Pass.Sum());
    }

    public static TheoryData<int[], int[], int, int> Switches()
    {
        var data = new TheoryData<int[], int[], int, int>();
        foreach (var (from, to) in new (int[], int[])[]
                 {
                     ([3, 1], [1, 3]), ([5, 2, 1], [1, 2, 5]), ([1, 2, 3, 4], [4, 3, 2, 1]), ([], [1, 2, 3]),
                 })
        foreach (var slots in new[] { 1, 2, 3, 4, 8 })
        foreach (var duration in new[] { 1, 3 })
            data.Add(from, to, slots, duration);
        return data;
    }

    [Theory]
    [MemberData(nameof(Switches))]
    public void Shares_after_switch(int[] from, int[] to, int workers, int duration)
    {
        foreach (var switchAt in new[] { 101, 258 })
        {
            _weights = Vector(from);
            var sim = new PoolSimulation(Create(workers, to.Length), [.. to.Select((_, i) => $"q{i}")], k =>
            {
                if (k == switchAt)
                    _weights = Vector(to);
            }) { Duration = duration };
            int[] all = [.. Enumerable.Range(0, to.Length)];

            while (sim.Iteration < switchAt - 1)
                sim.Step();
            var baseline = sim.SharedTaken.ToArray();
            for (var i = 0; i < 2000; i++)
            {
                sim.Step();
                foreach (var q in all)
                    AssertShare(Since(sim.SharedTaken, baseline), to, all, q,
                        $"switch at {switchAt}, duration {duration}, iteration {sim.Iteration}");
            }
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void Queue_idle_before_switch_gets_no_burst(bool switchMidIdle, bool neighborCapped)
    {
        int[] from = [2, 1, 3];
        int[] to = [1, 3, 2];
        const int idle = 2;
        _weights = Vector(from);
        var slots = Create(4, 3, (i, q) => { if (neighborCapped && i == 0) q.MaxConcurrency = 1; });
        var sim = new PoolSimulation(slots, ["q0", "q1", "q2"], k =>
        {
            if (k == (switchMidIdle ? 1501 : 3001))
                _weights = Vector(to);
        }) { Jobs = (q, n) => q == idle ? 0 : n };

        for (var i = 0; i < 3000; i++)
            sim.Step();

        sim.Jobs = (_, n) => n;
        var baseline = sim.SharedTaken.ToArray();
        // Сосед с MaxConcurrency = 1 берёт свой слот, остальное делят по весам очереди без потолка.
        int[] shared = neighborCapped ? [1, 2] : [0, 1, 2];
        for (var i = 0; i < 20; i++)
        {
            sim.Step();
            AssertShare(Since(sim.SharedTaken, baseline), to, shared, idle, $"backlog, iteration {i + 1}");
        }
    }
}
