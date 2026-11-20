using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Unit;

public sealed class WeightedDistributionTests
{
    private const double Tolerance = 1.25;
    private static readonly Func<string, bool> AllDue = _ => true;

    public static TheoryData<int[], int> WeightsAndSlots()
    {
        var data = new TheoryData<int[], int>();
        foreach (var weights in new[] { new[] { 1, 1, 1 }, [3, 1], [5, 2, 1] })
        foreach (var slots in new[] { 1, 2, 3, 4, 8 })
            data.Add(weights, slots);
        return data;
    }

    private static Action<PgBoltServerOptions> Queues(int count, Action<int, QueueOptions>? configure = null) => o =>
    {
        for (var i = 0; i < count; i++)
        {
            var index = i;
            o.Queue($"q{i}", q => configure?.Invoke(index, q));
        }
    };

    /// <summary>Общие слоты, взятые очередями <paramref name="queues"/>, отличаются от долей по весам не больше чем на 1,25.</summary>
    private static void AssertShares(long[] taken, int[] weights, int[] queues, string context)
    {
        foreach (var q in queues)
            AssertShare(taken, weights, queues, q, context);
    }

    /// <summary>Общие слоты очереди <paramref name="queue"/> отличаются от её доли среди <paramref name="queues"/> не больше чем на 1,25.</summary>
    private static void AssertShare(long[] taken, int[] weights, int[] queues, int queue, string context)
    {
        var total = queues.Sum(q => taken[q]);
        var deviation = Math.Abs(taken[queue] - (double)total * weights[queue] / queues.Sum(q => weights[q]));
        Assert.True(deviation <= Tolerance,
            $"{context}: queue {queue} took {taken[queue]} of {total}, deviation {deviation:F3} > {Tolerance}");
    }

    private static long[] Since(long[] now, long[] baseline) => [.. now.Select((v, i) => v - baseline[i])];

    [Theory]
    [MemberData(nameof(WeightsAndSlots))]
    public void Shares_follow_weights(int[] weights, int slots)
    {
        var sim = new PoolSimulation(slots, Queues(weights.Length), new WeightedDistribution(weights));
        int[] all = [.. Enumerable.Range(0, weights.Length)];

        for (var i = 0; i < 20_000; i++)
        {
            sim.Step();
            AssertShares(sim.SharedTaken, weights, all, $"iteration {sim.Iteration}");
        }
    }

    [Theory]
    [InlineData(new[] { 3, 1 }, 2, 1)]
    [InlineData(new[] { 3, 1 }, 4, 1)]
    [InlineData(new[] { 3, 1 }, 4, 3)]
    [InlineData(new[] { 3, 1 }, 8, 2)]
    [InlineData(new[] { 5, 2, 1 }, 3, 1)]
    [InlineData(new[] { 5, 2, 1 }, 4, 2)]
    [InlineData(new[] { 5, 2, 1 }, 8, 1)]
    [InlineData(new[] { 5, 2, 1 }, 8, 3)]
    public void Shares_follow_weights_with_batched_release(int[] weights, int slots, int duration)
    {
        // Задачи, стартовавшие вместе, заканчиваются вместе: план начинается с первого освободившегося слота, остальные
        // освобождаются посреди плана и разыгрываются в следующем.
        var sim = new PoolSimulation(slots, Queues(weights.Length), new WeightedDistribution(weights))
        {
            ReleaseMidPlan = true,
            Duration = duration,
        };
        int[] all = [.. Enumerable.Range(0, weights.Length)];

        for (var i = 0; i < 5000; i++)
        {
            sim.Step();
            AssertShares(sim.SharedTaken, weights, all, $"iteration {sim.Iteration}");
        }
    }

    [Fact]
    public void Example_sequence()
    {
        // critical: 3, default: 1, один свободный общий слот на итерацию.
        var sim = new PoolSimulation(1, Queues(2), new WeightedDistribution([3, 1]));

        for (var i = 0; i < 8; i++)
            sim.Step();

        Assert.Equal([0, 1, 0, 0, 0, 1, 0, 0], sim.SharedOrder);
    }

    [Fact]
    public void Next_draw_is_local_per_entry()
    {
        // Розыгрыш 4 слотов: 2, 1, 1. После того как первая взяла 2, розыгрыш оставшихся 2 начинается заново и даёт 1 и 1.
        var sim = new PoolSimulation(4, Queues(3), new WeightedDistribution([1, 1, 1]));

        sim.Step();

        Assert.Equal([(0, 2), (1, 1), (2, 1)], sim.LastPlan);
    }

    [Theory]
    [InlineData("empty")]
    [InlineData("not_due")]
    [InlineData("neighbor_capped")]
    public void Idle_queue_gets_no_burst(string idleKind)
    {
        int[] weights = [2, 1, 3];
        const int idle = 2;
        var capped = idleKind == "neighbor_capped";
        var sim = new PoolSimulation(4, Queues(3, (i, q) => { if (capped && i == 0) q.MaxConcurrency = 1; }),
            new WeightedDistribution(weights));
        if (idleKind == "not_due")
            sim.Due = q => q != idle;
        else
            sim.Jobs = (q, n) => q == idle ? 0 : n;

        for (var i = 0; i < 3000; i++)
        {
            var before = sim.SharedTaken.ToArray();
            sim.Step();
            Assert.Equal(0, sim.SharedTaken[idle]);
            // С соседом у потолка: в первом плане пустая очередь последняя и её доля пропадает, дальше она недобирает и
            // идёт первой — каждый план a берёт свой слот, остаток пула достаётся b.
            if (!capped)
                AssertShares(sim.SharedTaken, weights, [0, 1], $"idle, iteration {sim.Iteration}");
            else if (sim.Iteration > 1)
                Assert.Equal([1, 3, 0], Since(sim.SharedTaken, before));
        }

        sim.Due = _ => true;
        sim.Jobs = (_, n) => n;
        var baseline = sim.SharedTaken.ToArray();
        // Сосед с MaxConcurrency = 1 берёт свой слот, остальное делят по весам очереди без потолка.
        int[] shared = capped ? [1, 2] : [0, 1, 2];
        for (var i = 0; i < 20; i++)
        {
            sim.Step();
            AssertShare(Since(sim.SharedTaken, baseline), weights, shared, idle, $"backlog, iteration {i + 1}");
        }
    }

    [Fact]
    public void Empty_F_resets_plan_counters()
    {
        int[] weights = [1, 1, 1];
        var d = new WeightedDistribution(weights);
        var sim = new PoolSimulation(4, Queues(3), d) { Jobs = (_, n) => n / 2 };

        // Все недобирают: итог плана не сдвигает pass (только взятые слоты), счётчики не копятся между планами.
        for (var i = 0; i < 50; i++)
        {
            sim.Step();
            for (var q = 0; q < weights.Length; q++)
                Assert.Equal((double)sim.SharedTaken[q] / weights[q], d.Pass[q], 9);
        }

        sim.Jobs = (_, n) => n;
        var baseline = sim.SharedTaken.ToArray();
        for (var i = 0; i < 200; i++)
        {
            sim.Step();
            AssertShares(Since(sim.SharedTaken, baseline), weights, [0, 1, 2], $"backlog, iteration {i + 1}");
        }
    }

    [Fact]
    public void Reserved_freed_during_dequeue_is_not_short()
    {
        var d = new WeightedDistribution([1, 1]);
        var slots = new SlotAllocator(2, new PgBoltServerOptions { Workers = 2 }.Queue("a", q => q.Reserved = 1).Queue("b").Queues, d);
        slots.Take("a");                                 // Reserved-слот a занят

        using (var plan = slots.Plan(AllDue).GetEnumerator())
        {
            Assert.True(plan.MoveNext());
            Assert.Equal(("a", 1), plan.Current);        // доля 1 из общего пула
            slots.Release("a", fromReserved: true);       // во время dequeue освободился Reserved-слот a
            Assert.True(slots.Take("a"));                 // Take занял его вместо общего
            Assert.True(plan.MoveNext());
            Assert.Equal(("b", 2), plan.Current);
            slots.Take("b");
            slots.Take("b");
            Assert.False(plan.MoveNext());
        }
        Assert.Equal([0, 2], d.Pass);                      // общим зачтено только взятое

        _ = slots.Plan(AllDue).ToList();

        // a взяла запись целиком: она в F вместе с b, V = 0. Недобор a дал бы F = {b}, V = 2 и pass [0, 0].
        Assert.Equal([0, 2], d.Pass);
    }

    [Fact]
    public void Shared_slot_freed_mid_plan_goes_to_next_plan()
    {
        var o = new PgBoltServerOptions { Workers = 2 }.Queue("a").Queue("b").Queue("c");
        var slots = new SlotAllocator(o.Workers, o.Queues, new WeightedDistribution([1, 1, 1]));
        slots.Take("c");
        var plan = new List<(string, int)>();

        foreach (var (queue, n) in slots.Plan(AllDue))
        {
            plan.Add((queue, n));
            slots.Take(queue);
            if (queue == "a")
                slots.Release("c", fromReserved: false);   // общий слот c освободился посреди плана
        }

        Assert.Equal([("a", 1)], plan);
        Assert.Equal(("b", 1), slots.Plan(AllDue).First());   // слот разыгран в следующем плане: b отстаёт по pass
    }

    [Fact]
    public void Queue_ready_mid_plan_gets_remainder()
    {
        var o = new PgBoltServerOptions { Workers = 4 }.Queue("a").Queue("b");
        var slots = new SlotAllocator(o.Workers, o.Queues, new WeightedDistribution([1, 1]));
        var bReady = false;
        var plan = new List<(string, int)>();

        foreach (var (queue, n) in slots.Plan(q => q == "a" || bReady))
        {
            plan.Add((queue, n));
            slots.Take(queue);                               // недобор: 1 из n
            bReady = true;
        }

        Assert.Equal([("a", 4), ("b", 3)], plan);
    }

    [Fact]
    public void Interrupted_plan_is_settled_by_next_BeginPlan()
    {
        var o = new PgBoltServerOptions { Workers = 2 }.Queue("a").Queue("b");
        var slots = new SlotAllocator(o.Workers, o.Queues, new WeightedDistribution([1, 1]));

        foreach (var (queue, _) in slots.Plan(AllDue))
        {
            if (queue == "a")
                slots.Take("a");
            else
                break;                                       // b недобрала, план оборвался без финального Next
        }
        slots.Release("a", fromReserved: false);

        // b недобрала в прошлом плане — идёт первой при равных pass.
        Assert.Equal(("b", 1), slots.Plan(AllDue).First());
    }

    [Fact]
    public void Reserved_does_not_change_pass()
    {
        var d = new WeightedDistribution([1, 1]);
        var slots = new SlotAllocator(2, new PgBoltServerOptions { Workers = 2 }.Queue("a", q => q.Reserved = 2).Queue("b").Queues, d);

        slots.Take("a");
        slots.Take("a");
        Assert.Equal([0, 0], d.Pass);

        slots.Take("a");
        Assert.Equal([1, 0], d.Pass);
    }

    [Fact]
    public void MaxConcurrency_caps_share()
    {
        var sim = new PoolSimulation(4, Queues(2, (i, q) => { if (i == 0) q.MaxConcurrency = 1; }),
            new WeightedDistribution([3, 1]));

        for (var i = 1; i <= 20_000; i++)
        {
            sim.Step();
            Assert.Equal([i, 3L * i], sim.SharedTaken);   // a — не больше потолка, остаток пула — b
        }
    }

    [Fact]
    public void Ties_follow_declaration_order()
    {
        var sim = new PoolSimulation(1, Queues(3), new WeightedDistribution([1, 1, 1]));

        for (var i = 0; i < 6; i++)
            sim.Step();

        Assert.Equal([0, 1, 2, 0, 1, 2], sim.SharedOrder);
    }

    [Theory]
    [InlineData(new[] { 5, 2, 1 }, 1)]
    [InlineData(new[] { 5, 2, 1 }, 2)]
    [InlineData(new[] { 1, 1, 1 }, 2)]
    [InlineData(new[] { 7, 1 }, 3)]
    public void Pass_is_normalized(int[] weights, int slots)
    {
        var d = new WeightedDistribution(weights);
        var probe = new PassAtBeginPlan(d);
        var sim = new PoolSimulation(slots, Queues(weights.Length), probe);

        for (var i = 0; i < 20_000; i++)
        {
            // Backlog, без Reserved и MaxConcurrency: F — очереди с записью в прошлом плане.
            int[] f = [.. sim.LastPlan.Select(e => e.Queue)];
            sim.Step();
            if (f.Length == 0)
                continue;
            Assert.Equal(0, f.Min(q => probe.Pass[q]));
            Assert.All(probe.Pass, p => Assert.InRange(p, -1, 2));
        }
    }

    /// <summary>Снимок pass сразу после границы плана.</summary>
    private sealed class PassAtBeginPlan(WeightedDistribution inner) : ISharedPoolDistribution
    {
        public double[] Pass { get; private set; } = [];

        public void BeginPlan(int queueCount, Func<int, bool> candidate)
        {
            inner.BeginPlan(queueCount, candidate);
            Pass = [.. inner.Pass];
        }

        public (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available,
                                        Func<int, int> freeReserved, int freeShared) =>
            inner.Next(queueCount, candidate, available, freeReserved, freeShared);

        public void OnTaken(int queue, bool fromReserved) => inner.OnTaken(queue, fromReserved);
    }
}
