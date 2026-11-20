using System.Collections.Concurrent;
using System.Diagnostics;
using PgBolt.Benchmarks.Alternatives;
using PgBolt.Benchmarks.Benchmarks;
using PgBolt.Benchmarks.Copies;
using PgBolt.Benchmarks.Infrastructure;

namespace PgBolt.Benchmarks.Verification;

/// <summary>
/// Стресс-проверки ключевых инвариантов каждого варианта (и текущих копий — для сравнения) до замеров: бенчмарк
/// некорректного кода бессмыслен. Запуск: <c>--verify</c> (только проверка) или перед бенчмарками автоматически.
/// </summary>
internal static class Verifier
{
    private sealed record Outcome(string Area, string Variant, bool Passed, string Details, bool ExpectedToFail = false);

    /// <param name="only">Подстрока области (event, sink, slots, table, deadlines) — проверить только её; null — все.</param>
    public static bool Run(string? only = null)
    {
        var results = new List<Outcome>();
        var total = Stopwatch.StartNew();
        Console.WriteLine("== PgBolt.Benchmarks --verify ==");
        bool Want(string area) => only is null || area.Contains(only, StringComparison.OrdinalIgnoreCase);

        if (Want("event"))
        {
            foreach (var (name, factory) in EventVariants())
            {
                results.Add(Report(VerifyEventNoLostWakeups(name, factory)));
                results.Add(Report(VerifyEventPingPong(name, factory)));
            }
        }
        if (Want("sink"))
        {
            foreach (var (name, factory) in SinkVariants())
            {
                results.Add(Report(VerifySinkNoLostAcks(name, factory)));
                results.Add(Report(VerifySinkStop(name, factory)));
            }
        }
        if (Want("slots"))
        {
            results.Add(Report(VerifySlots("Lock (копия)", static (w, q) => new LockSlotAllocator(w, q))));
            results.Add(Report(VerifySlots("Interlocked", static (w, q) => new InterlockedSlotAllocator(w, q))));
        }
        if (Want("table"))
            results.Add(Report(VerifySnapshots()));
        if (Want("deadlines"))
        {
            results.Add(Report(VerifyDeadlines("Lock (копия)", static (t, c, i) => new LockLeaseDeadlines(t, c, i), expectedToFail: false)));
            results.Add(Report(VerifyDeadlines("Epoch (seqlock)", static (t, c, i) => new EpochLeaseDeadlines(t, c, i), expectedToFail: false)));
            results.Add(Report(VerifyDeadlines("Naive CAS (заведомо некорректный)", static (t, c, i) => new NaiveLeaseDeadlines(t, c, i), expectedToFail: true)));
        }

        var failed = results.Where(r => !r.Passed && !r.ExpectedToFail).ToList();
        Console.WriteLine();
        Console.WriteLine($"verify: {results.Count - failed.Count}/{results.Count} ok за {total.Elapsed.TotalSeconds:F1} с" +
                          (failed.Count == 0 ? "" : $"; ПРОВАЛ: {string.Join(", ", failed.Select(f => $"{f.Area}/{f.Variant}"))}"));
        return failed.Count == 0;
    }

    private static Outcome Report(Outcome outcome)
    {
        var status = outcome.ExpectedToFail
            ? (outcome.Passed ? "НЕ ПОЙМАН" : "ПОЙМАН (ожидаемо)")
            : (outcome.Passed ? "OK" : "FAIL");
        Console.WriteLine($"[{status,-17}] {outcome.Area,-34} {outcome.Variant,-36} {outcome.Details}");
        return outcome;
    }

    // ---------------------------------------------------------------- AsyncAutoResetEvent

    private interface IEvent
    {
        void Set();

        Task<bool> WaitAsync(TimeSpan timeout, CancellationToken ct);
    }

    private sealed class LockEvent : IEvent
    {
        private readonly LockAsyncAutoResetEvent _e = new();

        public void Set() => _e.Set();

        public Task<bool> WaitAsync(TimeSpan timeout, CancellationToken ct) => _e.WaitAsync(timeout, TimeProvider.System, ct);
    }

    private sealed class CasEvent : IEvent
    {
        private readonly CasAsyncAutoResetEvent _e = new();

        public void Set() => _e.Set();

        public Task<bool> WaitAsync(TimeSpan timeout, CancellationToken ct) => _e.WaitAsync(timeout, TimeProvider.System, ct);
    }

    private static IEnumerable<(string, Func<IEvent>)> EventVariants() =>
    [
        ("Lock (aaf1738)", static () => new LockEvent()),
        ("CAS (52b4a8f)", static () => new CasEvent()),
    ];

    /// <summary>
    /// Нет потерянных пробуждений: раунды, в которых 4 потока делают по 1–3 (produced++, Set), а ожидающий крутит
    /// WaitAsync со случайными таймаутом 0 / 1 мс / 20 мс и отменой через 0–200 мкс (границы таймаута и отмены). После
    /// раунда, если ожидающий не видел всех produced, следующий WaitAsync(2 с) обязан вернуть true (сигнал в латче или
    /// доставлен); false — сигнал потерян.
    /// </summary>
    private static Outcome VerifyEventNoLostWakeups(string variant, Func<IEvent> factory)
    {
        const int Rounds = 3_000, Setters = 4;
        var ev = factory();
        long produced = 0;
        var lost = 0;
        var rng = new Random(42);
        var go = new Barrier(Setters + 1);
        var stop = false;
        var settersDone = 0;
        var threads = Enumerable.Range(0, Setters).Select(t => new Thread(() =>
        {
            var local = new Random(t);
            while (true)
            {
                go.SignalAndWait();
                if (Volatile.Read(ref stop))
                    return;
                var n = local.Next(1, 4);
                for (var i = 0; i < n; i++)
                {
                    Thread.SpinWait(local.Next(0, 2_000));
                    Interlocked.Increment(ref produced);
                    ev.Set();
                }
                Interlocked.Increment(ref settersDone);
            }
        }) { IsBackground = true }).ToArray();
        foreach (var t in threads)
            t.Start();

        long consumed = 0;
        var sw = Stopwatch.StartNew();
        for (var round = 0; round < Rounds; round++)
        {
            Volatile.Write(ref settersDone, 0);
            go.SignalAndWait();
            while (Volatile.Read(ref settersDone) < Setters)
            {
                var mode = rng.Next(4);
                using var cts = new CancellationTokenSource();
                if (mode == 3)
                    cts.CancelAfter(TimeSpan.FromTicks(rng.Next(0, 2_000)));
                var timeout = mode switch { 0 => TimeSpan.Zero, 1 => TimeSpan.FromMilliseconds(1), _ => TimeSpan.FromMilliseconds(20) };
                bool signaled;
                try
                {
                    signaled = ev.WaitAsync(timeout, cts.Token).GetAwaiter().GetResult();
                }
                catch (OperationCanceledException)
                {
                    signaled = false;
                }
                if (signaled)
                    consumed = Volatile.Read(ref produced);
            }
            if (consumed < Volatile.Read(ref produced))
            {
                if (ev.WaitAsync(TimeSpan.FromSeconds(2), CancellationToken.None).GetAwaiter().GetResult())
                    consumed = Volatile.Read(ref produced);
                else
                    lost++;
            }
        }
        Volatile.Write(ref stop, true);
        go.SignalAndWait();
        foreach (var t in threads)
            t.Join();
        return new Outcome("AutoResetEvent: нет потерянных пробуждений", variant, lost == 0,
            $"раундов {Rounds}, Set {produced}, потеряно {lost}, {sw.Elapsed.TotalSeconds:F1} с");
    }

    /// <summary>Ping-pong: 100 000 кругов, каждый WaitAsync(5 с) обязан вернуть true.</summary>
    private static Outcome VerifyEventPingPong(string variant, Func<IEvent> factory)
    {
        const int RoundTrips = 100_000;
        var ping = factory();
        var pong = factory();
        var stop = false;
        var echoFailures = 0;
        var echo = Task.Run(async () =>
        {
            while (!Volatile.Read(ref stop))
            {
                if (await ping.WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None))
                    pong.Set();
                else if (!Volatile.Read(ref stop))
                    echoFailures++;
            }
        });
        var failures = 0;
        var sw = Stopwatch.StartNew();
        for (var i = 0; i < RoundTrips; i++)
        {
            ping.Set();
            if (!pong.WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None).GetAwaiter().GetResult())
                failures++;
        }
        Volatile.Write(ref stop, true);
        ping.Set();
        echo.GetAwaiter().GetResult();
        return new Outcome("AutoResetEvent: ping-pong", variant, failures == 0 && echoFailures == 0,
            $"кругов {RoundTrips}, таймаутов {failures + echoFailures}, {sw.Elapsed.TotalSeconds:F1} с");
    }

    // ---------------------------------------------------------------- CompletionSink

    private delegate ICompletionSink SinkFactory(ExecutionTable table, AckSendStub sender);

    private const int AckBatchSize = 100;
    private static readonly TimeSpan AckFlushInterval = TimeSpan.FromMilliseconds(20);

    private static IEnumerable<(string, SinkFactory)> SinkVariants() =>
    [
        ("(а) Lock (копия)", static (t, s) => new LockCompletionSink(t, s, AckBatchSize, AckFlushInterval, new MonoClock(TimeProvider.System))),
        ("(б) Cheap lock", static (t, s) => new CheapLockCompletionSink(t, s, AckBatchSize, AckFlushInterval, new MonoClock(TimeProvider.System))),
        ("(в) Lock-free", static (t, s) => new LockFreeCompletionSink(t, s, AckBatchSize, AckFlushInterval, new MonoClock(TimeProvider.System))),
        ("(в') Lock-free, триггер ==", static (t, s) => new LockFreeCompletionSink(t, s, AckBatchSize, AckFlushInterval, new MonoClock(TimeProvider.System),
            exactTrigger: true)),
    ];

    /// <summary>
    /// Ни одного потерянного ack: 8 потоков × 25 007 Complete (не кратно пакету — остаётся хвост), затем без Stop каждая
    /// запись должна уйти ровно один раз в пределах 1 с (хвост — по таймеру окна). 5 повторов.
    /// </summary>
    private static Outcome VerifySinkNoLostAcks(string variant, SinkFactory factory)
    {
        const int Threads = 8, PerThread = 25_007, Repeats = 5;
        var problems = new List<string>();
        var maxTail = TimeSpan.Zero;
        for (var repeat = 0; repeat < Repeats; repeat++)
        {
            var table = new ExecutionTable();
            var sender = new AckSendStub(table);
            var executions = NewExecutions(Threads * PerThread, table);
            var index = IndexOf(executions);
            var sent = new int[executions.Length];
            sender.OnSent = e => Interlocked.Increment(ref sent[index[e]]);
            var sink = factory(table, sender);
            sink.Start();
            var rejected = 0;
            RunThreads(Threads, t =>
            {
                for (var i = t * PerThread; i < (t + 1) * PerThread; i++)
                    if (!sink.Complete(executions[i], new JobOutcome.Ack()))
                        Interlocked.Increment(ref rejected);
            });
            var tail = Stopwatch.StartNew();
            while (sender.Sent < executions.Length && tail.Elapsed < TimeSpan.FromSeconds(1))
                Thread.Sleep(1);
            if (tail.Elapsed > maxTail)
                maxTail = tail.Elapsed;
            var missing = sent.Count(c => c == 0);
            var duplicated = sent.Count(c => c > 1);
            if (rejected != 0 || missing != 0 || duplicated != 0)
                problems.Add($"повтор {repeat}: отклонено {rejected}, не отправлено {missing}, дважды {duplicated}");
            sink.StopAsync(CancellationToken.None).GetAwaiter().GetResult();
        }
        return new Outcome("CompletionSink: нет потерянных ack", variant, problems.Count == 0,
            problems.Count == 0
                ? $"{Repeats}×{Threads * PerThread} ack, все ровно один раз; хвост ушёл за ≤ {maxTail.TotalMilliseconds:F0} мс"
                : string.Join("; ", problems));
    }

    /// <summary>
    /// Корректный Stop: 8 потоков непрерывно делают Complete, на ~10 мс — StopAsync, после его завершения ещё по 2 000
    /// Complete. Принятый (true) ack отправлен ровно один раз и до завершения StopAsync; отклонённый (false) — ни разу;
    /// после StopAsync отправок нет. 40 повторов.
    /// </summary>
    private static Outcome VerifySinkStop(string variant, SinkFactory factory)
    {
        const int Threads = 8, Capacity = 400_000, Repeats = 40;
        var problems = new List<string>();
        long acceptedTotal = 0, rejectedTotal = 0;
        for (var repeat = 0; repeat < Repeats; repeat++)
        {
            var table = new ExecutionTable();
            var sender = new AckSendStub(table);
            var executions = NewExecutions(Capacity, table);
            var index = IndexOf(executions);
            var sent = new int[Capacity];
            var accepted = new int[Capacity];       // 0 — не вызывался, 1 — true, 2 — false
            var stopDone = 0;
            var sentAfterStop = 0;
            sender.OnSent = e =>
            {
                if (Volatile.Read(ref stopDone) != 0)
                    Interlocked.Increment(ref sentAfterStop);
                Interlocked.Increment(ref sent[index[e]]);
            };
            var sink = factory(table, sender);
            sink.Start();
            var next = -1;
            var completers = Enumerable.Range(0, Threads).Select(_ => new Thread(() =>
            {
                var after = 0;
                while (after < 2_000)
                {
                    var i = Interlocked.Increment(ref next);
                    if (i >= Capacity)
                        return;
                    accepted[i] = sink.Complete(executions[i], new JobOutcome.Ack()) ? 1 : 2;
                    if (Volatile.Read(ref stopDone) != 0)
                        after++;
                }
            }) { IsBackground = true }).ToArray();
            foreach (var t in completers)
                t.Start();
            Thread.Sleep(10);
            sink.StopAsync(CancellationToken.None).GetAwaiter().GetResult();
            Volatile.Write(ref stopDone, 1);
            foreach (var t in completers)
                t.Join();
            Thread.Sleep(50);                         // окно таймера: «поздняя» отправка успела бы случиться
            int lostAccepted = 0, duplicated = 0, sentRejected = 0;
            for (var i = 0; i < Capacity; i++)
            {
                if (accepted[i] == 1 && sent[i] != 1)
                    lostAccepted += sent[i] == 0 ? 1 : 0;
                if (sent[i] > 1)
                    duplicated++;
                if (accepted[i] == 2 && sent[i] != 0)
                    sentRejected++;
                if (accepted[i] == 1)
                    acceptedTotal++;
                else if (accepted[i] == 2)
                    rejectedTotal++;
            }
            if (lostAccepted + duplicated + sentRejected + sentAfterStop != 0)
                problems.Add($"повтор {repeat}: принятых не отправлено {lostAccepted}, дважды {duplicated}, " +
                             $"отправлено отклонённых {sentRejected}, отправок после Stop {sentAfterStop}");
        }
        return new Outcome("CompletionSink: корректный Stop", variant, problems.Count == 0,
            problems.Count == 0
                ? $"{Repeats} гонок Stop/Complete: принято {acceptedTotal}, отклонено {rejectedTotal}, нарушений 0"
                : string.Join("; ", problems.Take(3)));
    }

    private static Execution[] NewExecutions(int count, ExecutionTable table)
    {
        var executions = new Execution[count];
        for (var i = 0; i < count; i++)
        {
            executions[i] = new Execution(Guid.NewGuid(), Guid.NewGuid(), "default", 0, ExecutionState.Running);
            table.Add(executions[i]);
        }
        return executions;
    }

    private static Dictionary<Execution, int> IndexOf(Execution[] executions)
    {
        var index = new Dictionary<Execution, int>(executions.Length, ReferenceEqualityComparer.Instance);
        for (var i = 0; i < executions.Length; i++)
            index.Add(executions[i], i);
        return index;
    }

    // ---------------------------------------------------------------- SlotAllocator

    /// <summary>
    /// Занятость не уходит в минус и не превышает лимит: диспетчер (Available + Take) и 7 освобождающих, лимиты малы
    /// (Workers 8; q0 Reserved 2; q1 Reserved 1, MaxConcurrency 4) — пул почти всё время исчерпан. 300 000 Take.
    /// Проверки: счётчики аллокатора после каждого Take/Release в [0, лимит]; независимый счётчик «удерживаемых» слотов
    /// (растёт после Take, падает до Release) не больше лимитов; в конце всё нулевое.
    /// </summary>
    private static Outcome VerifySlots(string variant, Func<int, IReadOnlyList<SlotQueue>, ISlotAllocator> factory)
    {
        const int Workers = 8, Releasers = 7, Total = 300_000;
        SlotQueue[] queues = [new("q0", 2, null), new("q1", 1, 4)];
        string[] names = ["q0", "q1"];
        var allocator = factory(Workers, queues);
        var rings = Enumerable.Range(0, Releasers).Select(_ => new ConcurrentQueue<(int Queue, bool Reserved)>()).ToArray();
        var heldQueue = new int[2];
        var heldReserved = new int[2];
        var heldShared = 0;
        var violations = new ConcurrentBag<string>();
        var dispatchDone = false;
        var released = 0;
        var exceptions = 0;

        // Сумму r + s проверяет только диспетчер: у Interlocked-варианта Counters читает счётчики по одному, и на потоке
        // освобождающего сумма значений из разных моментов (между ними Take) могла бы превысить лимит ложно. На диспетчере
        // параллельно идут только Release (уменьшения) — сумма прочитанных не больше истинной в момент первого чтения.
        void Check(string where, int q, bool checkSum)
        {
            var (r, s, total) = allocator.Counters(names[q]);
            if (r < 0 || s < 0 || total < 0)
                violations.Add($"{where}: отрицательный счётчик ({r}, {s}, {total})");
            if (r > queues[q].Reserved || total > Workers || (checkSum && queues[q].MaxConcurrency is int max && r + s > max))
                violations.Add($"{where}: превышен лимит ({r}, {s}, {total})");
        }

        var sw = Stopwatch.StartNew();
        var releasers = Enumerable.Range(0, Releasers).Select(k => new Thread(() =>
        {
            var spin = new SpinWait();
            while (true)
            {
                if (rings[k].TryDequeue(out var slot))
                {
                    Interlocked.Decrement(ref heldQueue[slot.Queue]);
                    if (slot.Reserved)
                        Interlocked.Decrement(ref heldReserved[slot.Queue]);
                    else
                        Interlocked.Decrement(ref heldShared);
                    try
                    {
                        allocator.Release(names[slot.Queue], slot.Reserved);
                    }
                    catch (InvalidOperationException)
                    {
                        Interlocked.Increment(ref exceptions);
                    }
                    Check("после Release", slot.Queue, checkSum: false);
                    Interlocked.Increment(ref released);
                    spin.Reset();
                }
                else if (Volatile.Read(ref dispatchDone) && rings[k].IsEmpty)
                    return;
                else
                    spin.SpinOnce(sleep1Threshold: -1);
            }
        }) { IsBackground = true }).ToArray();
        foreach (var t in releasers)
            t.Start();

        var rng = new Random(7);
        var spinD = new SpinWait();
        for (var taken = 0; taken < Total;)
        {
            var q = rng.Next(2);
            if (allocator.Available(names[q]) == 0)
            {
                spinD.SpinOnce(sleep1Threshold: -1);
                if (sw.Elapsed > TimeSpan.FromSeconds(60))
                {
                    violations.Add("диспетчер не продвигается (возможна утечка слотов)");
                    break;
                }
                continue;
            }
            spinD.Reset();
            bool reserved;
            try
            {
                reserved = allocator.Take(names[q]);
            }
            catch (InvalidOperationException)
            {
                Interlocked.Increment(ref exceptions);
                continue;
            }
            var hq = Interlocked.Increment(ref heldQueue[q]);
            var hr = reserved ? Interlocked.Increment(ref heldReserved[q]) : Volatile.Read(ref heldReserved[q]);
            var hs = reserved ? Volatile.Read(ref heldShared) : Interlocked.Increment(ref heldShared);
            if (hr > queues[q].Reserved || hs > Workers || (queues[q].MaxConcurrency is int max && hq > max))
                violations.Add($"удерживается сверх лимита: q{q} {hq}, reserved {hr}, shared {hs}");
            Check("после Take", q, checkSum: true);
            rings[taken % Releasers].Enqueue((q, reserved));
            taken++;
        }
        Volatile.Write(ref dispatchDone, true);
        foreach (var t in releasers)
            t.Join();
        for (var q = 0; q < 2; q++)
        {
            var (r, s, total) = allocator.Counters(names[q]);
            if (r != 0 || s != 0 || total != 0)
                violations.Add($"в конце q{q}: ({r}, {s}, {total}) ≠ 0");
        }
        if (exceptions != 0)
            violations.Add($"исключений Take/Release: {exceptions}");
        return new Outcome("SlotAllocator: 0 ≤ занятость ≤ лимит", variant, violations.IsEmpty,
            violations.IsEmpty
                ? $"Take/Release {Total}/{released}, нарушений 0, {sw.Elapsed.TotalSeconds:F1} с"
                : $"нарушений {violations.Count}: {string.Join("; ", violations.Take(3))}");
    }

    // ---------------------------------------------------------------- ExecutionTable

    /// <summary>
    /// Снимок не теряет записей, существовавших всё время: 500 постоянных записей, 6 потоков непрерывно добавляют и
    /// удаляют свои (в том числе с более ранними рубежами); 20 000 раз каждый вариант снимка обязан содержать все
    /// постоянные, а минимум по перечислению — быть не больше минимума постоянных и не меньше минимума всех возможных.
    /// </summary>
    private static Outcome VerifySnapshots()
    {
        const int Stable = 500, Churners = 6, Checks = 20_000;
        var table = new ExecutionTable();
        var clock = new MonoClock(new StubTimeProvider());
        var interval = TimeSpan.FromTicks(10);
        var stable = Enumerable.Range(0, Stable).Select(i => TableLoad.NewExecution(1_000_000 + i)).ToArray();
        foreach (var e in stable)
            table.Add(e);
        var stableIds = stable.Select(e => e.Lease.LeaseId).ToHashSet();
        var minStable = clock.Add(stable.Min(e => e.ValidUntil), -interval);
        var minPossible = clock.Add(1, -interval);
        var stop = false;
        var churners = Enumerable.Range(0, Churners).Select(c => new Thread(() =>
        {
            var rng = new Random(c);
            var own = Enumerable.Range(0, 64).Select(_ => TableLoad.NewExecution(rng.Next(1, 2_000_000))).ToArray();
            var i = 0;
            while (!Volatile.Read(ref stop))
            {
                var e = own[i++ & 63];
                table.Add(e);
                table.Remove(e);
            }
        }) { IsBackground = true }).ToArray();
        foreach (var t in churners)
            t.Start();

        int missCurrent = 0, missWithoutCount = 0, badMin = 0;
        for (var k = 0; k < Checks; k++)
        {
            var variant = k % 3;
            if (variant == 0)
                missCurrent += Stable - table.Snapshot().Count(e => stableIds.Contains(e.Lease.LeaseId));
            else if (variant == 1)
                missWithoutCount += Stable - table.SnapshotWithoutCount().Count(e => stableIds.Contains(e.Lease.LeaseId));
            else
            {
                var min = table.MinWatchedDeadline(clock, interval);
                if (min > minStable || min < minPossible)
                    badMin++;
            }
        }
        Volatile.Write(ref stop, true);
        foreach (var t in churners)
            t.Join();
        return new Outcome("ExecutionTable: снимок без потерь", "Snapshot / без Count / минимум",
            missCurrent + missWithoutCount + badMin == 0,
            $"{Checks} снимков: потеряно постоянных {missCurrent}/{missWithoutCount}, неверный минимум {badMin}");
    }

    // ---------------------------------------------------------------- LeaseKeeper

    /// <summary>
    /// Рубеж не пропускается: поток диспетчера регистрирует 1 000 000 записей со случайными рубежами (часто раньше
    /// взведённого), 2 потока делают Rearm с паузами 0–~2 мкс, 3 потока удаляют записи. Пока запись в таблице и отслеживается,
    /// таймер обязан быть взведён не позже её рубежа: проверяется сразу после Register и перед удалением.
    /// </summary>
    private static Outcome VerifyDeadlines(string variant, Func<ExecutionTable, MonoClock, TimeSpan, ILeaseDeadlines> factory,
        bool expectedToFail)
    {
        const int Total = 1_000_000, Rearmers = 2, Removers = 3, MaxPresent = 256;
        var interval = TimeSpan.FromTicks(10);
        var time = new StubTimeProvider();
        var clock = new MonoClock(time);
        var table = new ExecutionTable();
        var deadlines = factory(table, clock, interval);
        deadlines.Start();
        var timer = time.Timer!;
        var toRemove = new ConcurrentQueue<Execution>();
        var stop = false;
        var dispatchDone = false;
        long afterRegister = 0, beforeRemove = 0;

        // Между перевзводами — случайная пауза 0–~2 мкс: без неё два потока Rearm держат lock практически непрерывно, и
        // несправедливый lock/нечётная эпоха на минуты задерживают регистрацию (голодание, а не нарушение инварианта).
        var rearmers = Enumerable.Range(0, Rearmers).Select(r => new Thread(() =>
        {
            var local = new Random(100 + r);
            while (!Volatile.Read(ref stop))
            {
                deadlines.Rearm();
                Thread.SpinWait(local.Next(0, 1_000));
            }
        }) { IsBackground = true }).ToArray();
        var removers = Enumerable.Range(0, Removers).Select(r => new Thread(() =>
        {
            var spin = new SpinWait();
            while (true)
            {
                if (toRemove.TryDequeue(out var e))
                {
                    if (timer.Target > clock.Add(e.ValidUntil, -interval))
                        Interlocked.Increment(ref beforeRemove);
                    if (e.TryTransition(ExecutionState.Running, ExecutionState.Removed))
                        table.Remove(e);
                    spin.Reset();
                }
                else if (Volatile.Read(ref dispatchDone))
                    return;
                else
                    spin.SpinOnce(sleep1Threshold: -1);
            }
        }) { IsBackground = true }).ToArray();
        foreach (var t in rearmers.Concat(removers))
            t.Start();

        var sw = Stopwatch.StartNew();
        var rng = new Random(11);
        for (var i = 0; i < Total; i++)
        {
            while (toRemove.Count > MaxPresent)
                Thread.SpinWait(20);
            var e = deadlines.Register(Guid.NewGuid(), Guid.NewGuid(), "default", rng.Next(1_000, 1_000_000_000));
            if (timer.Target > clock.Add(e.ValidUntil, -interval))
                afterRegister++;
            toRemove.Enqueue(e);
        }
        Volatile.Write(ref dispatchDone, true);
        foreach (var t in removers)
            t.Join();
        Volatile.Write(ref stop, true);
        foreach (var t in rearmers)
            t.Join();
        deadlines.Stop();
        var violations = afterRegister + beforeRemove;
        return new Outcome("LeaseKeeper: рубеж не пропускается", variant, violations == 0,
            $"{Total} Register на фоне Rearm: таймер позже рубежа — сразу после Register {afterRegister}, " +
            $"перед удалением {beforeRemove}; {sw.Elapsed.TotalSeconds:F1} с", expectedToFail);
    }

    private static void RunThreads(int count, Action<int> body)
    {
        using var pool = new WorkerPool(count, "verify");
        pool.Run(body);
    }
}
