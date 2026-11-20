using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Unit;

public sealed class SlotAllocatorTests
{
    private static readonly Func<string, bool> AllDue = _ => true;

    private static SlotAllocator Create(int workers, Action<PgBoltServerOptions> queues)
    {
        var o = new PgBoltServerOptions { Workers = workers };
        queues(o);
        return new SlotAllocator(o.Workers, o.Queues);
    }

    private static void TakeMany(SlotAllocator slots, string queue, int count)
    {
        for (var i = 0; i < count; i++)
            slots.Take(queue);
    }

    [Fact]
    public void First_queue_sees_its_reserved_plus_all_shared()
    {
        var slots = Create(10, o => o.Queue("critical", q => q.Reserved = 5).Queue("default"));

        Assert.Equal(15, slots.Available("critical"));
        Assert.Equal(10, slots.Available("default"));
    }

    [Fact]
    public void Plan_follows_declaration_order_and_yields_each_queue_once()
    {
        var slots = Create(4, o => o.Queue("a").Queue("b").Queue("c", q => q.Reserved = 1));

        // Ничего не занято: каждая очередь видит весь общий пул (c — ещё и свой Reserved), но попадает в план один раз.
        Assert.Equal([("a", 4), ("b", 4), ("c", 5)], slots.Plan(AllDue));
    }

    [Fact]
    public void Plan_is_recomputed_after_each_take_so_unused_shared_slots_move_on()
    {
        var slots = Create(10, o => o.Queue("a").Queue("b").Queue("c"));
        var plan = new List<(string Queue, int N)>();

        foreach (var (queue, n) in slots.Plan(AllDue))
        {
            plan.Add((queue, n));
            TakeMany(slots, queue, queue == "a" ? 7 : n);   // a добрала 7 из 10, b — всё, что ей досталось
        }

        Assert.Equal([("a", 10), ("b", 3)], plan);          // недобор a перешёл к b; c осталась без слотов — не в плане
    }

    [Fact]
    public void Shared_slot_freed_mid_plan_waits_for_next_plan()
    {
        var slots = Create(2, o => o.Queue("a").Queue("b").Queue("c"));
        slots.Take("c");
        var plan = new List<(string Queue, int N)>();

        foreach (var (queue, n) in slots.Plan(AllDue))
        {
            plan.Add((queue, n));
            slots.Take(queue);
            if (queue == "a")
                slots.Release("c", fromReserved: false);   // общий слот освободился между записями плана
        }

        Assert.Equal([("a", 1)], plan);
        Assert.Equal(1, slots.Available("b"));              // вне плана свободные слоты фактические
        Assert.Equal([("b", 1)], slots.Plan(q => q == "b"));
    }

    [Fact]
    public void Reserved_slot_freed_mid_plan_is_available_to_its_queue()
    {
        var slots = Create(1, o => o.Queue("b").Queue("a", q => q.Reserved = 1));
        slots.Take("a");                                    // Reserved-слот a занят
        var plan = new List<(string Queue, int N)>();

        foreach (var (queue, n) in slots.Plan(AllDue))
        {
            plan.Add((queue, n));
            slots.Take(queue);
            if (queue == "b")
                slots.Release("a", fromReserved: true);
        }

        Assert.Equal([("b", 1), ("a", 1)], plan);
    }

    [Fact]
    public void Reserved_slots_are_not_given_to_other_queues()
    {
        var slots = Create(2, o => o.Queue("critical", q => q.Reserved = 3).Queue("default"));

        TakeMany(slots, "default", 2);

        Assert.Equal([("critical", 3)], slots.Plan(AllDue));   // default: общих нет, Reserved чужой
        Assert.Equal(0, slots.Available("default"));
        Assert.Throws<InvalidOperationException>(() => slots.Take("default"));
    }

    [Fact]
    public void Queue_with_no_free_slots_is_not_in_plan()
    {
        var slots = Create(3, o => o.Queue("a", q => q.MaxConcurrency = 1).Queue("b"));

        slots.Take("a");

        Assert.Equal([("b", 2)], slots.Plan(AllDue));   // a упёрлась в MaxConcurrency: n = 0
    }

    [Fact]
    public void Plan_skips_queues_that_are_not_due()
    {
        var slots = Create(4, o => o.Queue("a").Queue("b"));

        Assert.Equal([("b", 4)], slots.Plan(q => q != "a"));
    }

    [Fact]
    public void Distribution_strategy_is_replaceable()
    {
        var o = new PgBoltServerOptions { Workers = 2 }.Queue("a").Queue("b");
        var slots = new SlotAllocator(o.Workers, o.Queues, new ReverseOrder());

        Assert.Equal([("b", 2), ("a", 2)], slots.Plan(AllDue));
    }

    [Fact]
    public void Distribution_ending_plan_while_a_due_queue_has_free_slots_is_rejected()
    {
        var o = new PgBoltServerOptions { Workers = 2 }.Queue("a").Queue("b", q => q.MaxConcurrency = 1);
        var slots = new SlotAllocator(o.Workers, o.Queues, new Declining());

        // Пустой план при готовой очереди со свободным слотом: диспетчер крутился бы без ожидания — ошибка стратегии.
        Assert.Throws<InvalidOperationException>(() => slots.Plan(AllDue).ToList());

        Assert.Empty(slots.Plan(_ => false));      // готовых очередей нет — пустой план законен
        slots.Take("a");
        slots.Take("b");
        Assert.Empty(slots.Plan(AllDue));          // свободных слотов нет (a: общий пул занят, b: MaxConcurrency) — тоже
    }

    [Fact]
    public void Take_uses_reserved_first_then_shared()
    {
        var slots = Create(5, o => o.Queue("q", q => q.Reserved = 2));

        Assert.True(slots.Take("q"));
        Assert.True(slots.Take("q"));
        Assert.False(slots.Take("q"));
        Assert.Equal(3, slots.InUse("q"));
        Assert.Equal(4, slots.Available("q"));
    }

    [Fact]
    public void Max_concurrency_counts_reserved_and_shared()
    {
        var slots = Create(10, o => o.Queue("emails", q => { q.Reserved = 2; q.MaxConcurrency = 4; }).Queue("other"));

        Assert.Equal(4, slots.Available("emails"));
        TakeMany(slots, "emails", 3);             // 2 reserved + 1 shared
        Assert.Equal(1, slots.Available("emails"));
        slots.Take("emails");
        Assert.Equal(0, slots.Available("emails"));
        Assert.Equal(8, slots.Available("other"));
    }

    [Fact]
    public void Max_concurrency_never_goes_negative_when_shared_is_scarce()
    {
        var slots = Create(1, o => o.Queue("a").Queue("b", q => q.MaxConcurrency = 5));

        slots.Take("a");

        Assert.Equal(0, slots.Available("b"));
    }

    [Fact]
    public void Release_returns_slot_of_the_same_kind()
    {
        var slots = Create(1, o => o.Queue("a", q => q.Reserved = 1).Queue("b"));
        var reserved = slots.Take("a");
        var shared = slots.Take("a");
        Assert.Equal(0, slots.Available("b"));

        slots.Release("a", shared);
        Assert.Equal(1, slots.Available("b"));    // общий слот вернулся в пул

        slots.Release("a", reserved);
        Assert.Equal(1, slots.Available("b"));    // reserved-слот другим не достаётся
        Assert.Equal(2, slots.Available("a"));
        Assert.Equal(0, slots.InUse("a"));
    }

    [Fact]
    public void Release_of_a_slot_that_was_not_taken_throws()
    {
        var slots = Create(1, o => o.Queue("a", q => q.Reserved = 1));

        Assert.Throws<InvalidOperationException>(() => slots.Release("a", fromReserved: true));
        Assert.Throws<InvalidOperationException>(() => slots.Release("a", fromReserved: false));
    }

    [Fact]
    public void Available_is_zero_when_exhausted_and_take_throws()
    {
        var slots = Create(2, o => o.Queue("a"));
        TakeMany(slots, "a", 2);

        Assert.Equal(0, slots.Available("a"));
        Assert.Throws<InvalidOperationException>(() => slots.Take("a"));
        Assert.Equal(2, slots.InUse("a"));
        Assert.Empty(slots.Plan(AllDue));
    }

    [Fact]
    public void Zero_workers_queue_runs_only_on_reserved()
    {
        var slots = Create(0, o => o.Queue("a", q => q.Reserved = 2).Queue("b"));

        Assert.Equal(2, slots.Available("a"));
        Assert.Equal(0, slots.Available("b"));
        Assert.True(slots.Take("a"));
        Assert.True(slots.Take("a"));
        Assert.Equal(0, slots.Available("a"));
    }

    [Fact]
    public void Unknown_queue_is_rejected()
    {
        var slots = Create(1, o => o.Queue("a"));
        Assert.Throws<ArgumentException>(() => slots.Available("nope"));
        Assert.Throws<ArgumentException>(() => slots.Take("nope"));
    }

    [Fact]
    public void Concurrent_take_and_release_never_exceed_capacity()
    {
        const int workers = 4;
        var slots = Create(workers, o => o.Queue("a", q => q.Reserved = 2).Queue("b", q => q.MaxConcurrency = 3));
        int heldA = 0, heldB = 0, heldShared = 0, maxA = 0, maxB = 0, maxShared = 0, rejected = 0, taken = 0;

        Parallel.For(0, 8, new ParallelOptions { MaxDegreeOfParallelism = 8 }, worker =>
        {
            var queue = worker % 2 == 0 ? "a" : "b";
            for (var i = 0; i < 20_000; i++)
            {
                bool fromReserved;
                try
                {
                    fromReserved = slots.Take(queue);
                }
                catch (InvalidOperationException)
                {
                    Interlocked.Increment(ref rejected);
                    continue;
                }

                Interlocked.Increment(ref taken);
                if (queue == "a") Max(ref maxA, Interlocked.Increment(ref heldA));
                else Max(ref maxB, Interlocked.Increment(ref heldB));
                if (!fromReserved) Max(ref maxShared, Interlocked.Increment(ref heldShared));

                if (!fromReserved) Interlocked.Decrement(ref heldShared);
                if (queue == "a") Interlocked.Decrement(ref heldA);
                else Interlocked.Decrement(ref heldB);
                slots.Release(queue, fromReserved);
            }
        });

        Assert.True(taken > 0);
        Assert.InRange(maxA, 1, 2 + workers);
        Assert.InRange(maxB, 1, 3);
        Assert.InRange(maxShared, 1, workers);
        Assert.Equal(0, slots.InUse("a"));
        Assert.Equal(0, slots.InUse("b"));
        Assert.Equal(2 + workers, slots.Available("a"));
        Assert.Equal(3, slots.Available("b"));
        Assert.Equal(8 * 20_000, taken + rejected);
    }

    private static void Max(ref int target, int value)
    {
        var current = Volatile.Read(ref target);
        while (value > current)
        {
            var seen = Interlocked.CompareExchange(ref target, value, current);
            if (seen == current)
                return;
            current = seen;
        }
    }

    /// <summary>Подменная стратегия, заканчивающая план сразу (нарушает контракт при готовой очереди со свободным слотом).</summary>
    private sealed class Declining : ISharedPoolDistribution
    {
        public void BeginPlan(int queueCount, Func<int, bool> candidate) { }

        public (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available,
                                        Func<int, int> freeReserved, int freeShared) => null;

        public void OnTaken(int queue, bool fromReserved) { }
    }

    /// <summary>Подменная стратегия: обратный порядок объявления (проверка, что план берётся из стратегии).</summary>
    private sealed class ReverseOrder : ISharedPoolDistribution
    {
        public void BeginPlan(int queueCount, Func<int, bool> candidate) { }

        public void OnTaken(int queue, bool fromReserved) { }

        public (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available,
                                        Func<int, int> freeReserved, int freeShared)
        {
            for (var i = queueCount - 1; i >= 0; i--)
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

    /// <summary>Записывает вызовы контракта, ответы берёт у строгого порядка.</summary>
    private sealed class Recording : ISharedPoolDistribution
    {
        private readonly StrictOrderDistribution _inner = new();

        public List<string> Calls { get; } = [];

        public void BeginPlan(int queueCount, Func<int, bool> candidate) =>
            Calls.Add($"begin:{string.Join(',', Enumerable.Range(0, queueCount).Select(i => candidate(i) ? 1 : 0))}");

        public (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available,
                                        Func<int, int> freeReserved, int freeShared)
        {
            Calls.Add($"next:reserved={string.Join(',', Enumerable.Range(0, queueCount).Select(freeReserved))};shared={freeShared}");
            return _inner.Next(queueCount, candidate, available, freeReserved, freeShared);
        }

        public void OnTaken(int queue, bool fromReserved) => Calls.Add($"taken:{queue}:{fromReserved}");
    }

    [Fact]
    public void BeginPlan_is_called_once_per_plan_before_first_Next_with_the_same_candidates()
    {
        var o = new PgBoltServerOptions { Workers = 2 }.Queue("a").Queue("b");
        var rec = new Recording();
        var slots = new SlotAllocator(o.Workers, o.Queues, rec);

        _ = slots.Plan(q => q != "a").ToList();

        // b получает всё (2), затем кандидаток нет (a не готова, b выдана) — второй Next возвращает null.
        Assert.Equal(["begin:0,1", "next:reserved=0,0;shared=2", "next:reserved=0,0;shared=2"], rec.Calls);
    }

    [Fact]
    public void BeginPlan_is_called_for_a_plan_without_entries()
    {
        var o = new PgBoltServerOptions { Workers = 2 }.Queue("a");
        var rec = new Recording();
        var slots = new SlotAllocator(o.Workers, o.Queues, rec);

        _ = slots.Plan(_ => false).ToList();

        Assert.Equal(["begin:0", "next:reserved=0;shared=2"], rec.Calls);
    }

    [Fact]
    public void BeginPlan_is_not_called_until_the_plan_is_enumerated()
    {
        var o = new PgBoltServerOptions { Workers = 2 }.Queue("a");
        var rec = new Recording();
        var slots = new SlotAllocator(o.Workers, o.Queues, rec);

        var plan = slots.Plan(AllDue);

        Assert.Empty(rec.Calls);
        Assert.Single(plan.Take(1));
        Assert.Equal(["begin:1", "next:reserved=0;shared=2"], rec.Calls);
    }

    [Fact]
    public void OnTaken_is_called_for_every_Take_with_its_result()
    {
        var o = new PgBoltServerOptions { Workers = 3 }.Queue("a", q => q.Reserved = 1).Queue("b");
        var rec = new Recording();
        var slots = new SlotAllocator(o.Workers, o.Queues, rec);

        slots.Take("a");
        slots.Take("a");
        slots.Take("b");

        Assert.Equal(["taken:0:True", "taken:0:False", "taken:1:False"], rec.Calls);
    }

    [Fact]
    public void Failed_Take_does_not_call_OnTaken()
    {
        var o = new PgBoltServerOptions { Workers = 1 }.Queue("a", q => q.MaxConcurrency = 1);
        var rec = new Recording();
        var slots = new SlotAllocator(o.Workers, o.Queues, rec);

        slots.Take("a");
        Assert.Throws<InvalidOperationException>(() => slots.Take("a"));

        Assert.Equal(["taken:0:False"], rec.Calls);
    }

    [Fact]
    public void Next_gets_free_shared_of_the_server_and_zero_free_reserved_when_all_reserved_are_taken()
    {
        var o = new PgBoltServerOptions { Workers = 3 }.Queue("a", q => q.Reserved = 2).Queue("b", q => q.Reserved = 1);
        var rec = new Recording();
        var slots = new SlotAllocator(o.Workers, o.Queues, rec);

        slots.Take("a");   // reserved
        slots.Take("a");   // reserved
        slots.Take("a");   // общий
        slots.Take("b");   // reserved
        rec.Calls.Clear();

        _ = slots.Plan(AllDue).ToList();

        Assert.Equal("next:reserved=0,0;shared=2", rec.Calls[1]);
    }

    [Fact]
    public void Next_gets_free_reserved_of_partially_used_queues()
    {
        var o = new PgBoltServerOptions { Workers = 3 }.Queue("a", q => q.Reserved = 2).Queue("b", q => q.Reserved = 1);
        var rec = new Recording();
        var slots = new SlotAllocator(o.Workers, o.Queues, rec);

        slots.Take("a");   // reserved
        rec.Calls.Clear();

        _ = slots.Plan(AllDue).ToList();

        Assert.Equal("next:reserved=1,1;shared=3", rec.Calls[1]);
    }
}
