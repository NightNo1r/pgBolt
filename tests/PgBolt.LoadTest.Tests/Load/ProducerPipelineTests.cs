using System.Collections.Concurrent;
using Microsoft.Extensions.Time.Testing;
using PgBolt.LoadTest.Load;

namespace PgBolt.LoadTest.Tests.Load;

/// <summary>Генератор случайных чисел с постоянным значением: профиль выдаёт задачи одного ключа.</summary>
internal sealed class ConstRandom(double value) : Random
{
    protected override double Sample() => value;
}

public sealed class ProducerPipelineTests
{
    private const int Senders = 4;
    private const int ChannelCapacity = 8;
    private const int BatchSize = 100;

    /// <summary>Максимум задач, которые генератор успевает выдать при заблокированной отправке одного ключа:
    /// пачки в руках отправителей, полный канал и ещё одна готовая пачка, ждущая места.</summary>
    private const int StalledJobs = (Senders + ChannelCapacity + 1) * BatchSize;

    private static readonly TimeSpan WaitLimit = TimeSpan.FromSeconds(10);

    /// <summary>Фейковое время, которое считает взведённые таймеры: по счётчику видно, что генератор снова ждёт тика.</summary>
    private sealed class SpyTimeProvider : FakeTimeProvider
    {
        private int _armed;

        public int Armed => Volatile.Read(ref _armed);

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new SpyTimer(base.CreateTimer(callback, state, dueTime, period), this);
            if (dueTime != Timeout.InfiniteTimeSpan) Interlocked.Increment(ref _armed);
            return timer;
        }

        private sealed class SpyTimer(ITimer inner, SpyTimeProvider owner) : ITimer
        {
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                var changed = inner.Change(dueTime, period);
                if (dueTime != Timeout.InfiniteTimeSpan) Interlocked.Increment(ref owner._armed);
                return changed;
            }

            public void Dispose() => inner.Dispose();

            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }
    }

    private sealed class Harness : IAsyncDisposable
    {
        private readonly DateTimeOffset _start;
        private readonly CancellationTokenSource _cts = new();
        private Task _run = Task.CompletedTask;
        private volatile TaskCompletionSource? _gate;
        private int _blocked;
        private bool _stopped;

        public Harness(
            double rate,
            Random? rng = null,
            double? ceiling = null,
            Func<int>? pickDelay = null,
            Action<long>? onSeqIssued = null,
            Action<SendRequest>? onSend = null,
            bool blocked = false)
        {
            _start = Time.GetUtcNow();
            if (blocked) Block();
            Bucket = new TokenBucket(rate, ceiling ?? rate, Time);
            Pipeline = new ProducerPipeline(
                new ProfileB(rng ?? new Random(42)),
                Bucket,
                new BatchAssembler(),
                async (request, _) =>
                {
                    onSend?.Invoke(request);
                    Sent.Enqueue(request);
                    var gate = _gate;
                    if (gate is null) return;
                    Interlocked.Increment(ref _blocked);
                    await gate.Task;
                },
                () => (Time.GetUtcNow() - _start).Ticks / 10,
                pickDelay ?? (() => 1),
                Time,
                onSeqIssued ?? (_ => { }),
                Senders,
                ChannelCapacity);
        }

        public SpyTimeProvider Time { get; } = new();
        public TokenBucket Bucket { get; }
        public ProducerPipeline Pipeline { get; }
        public ConcurrentQueue<SendRequest> Sent { get; } = new();
        public Task Run => _run;

        /// <summary>Сколько отправителей застряло в заблокированном <c>send</c>.</summary>
        public int Blocked => Volatile.Read(ref _blocked);

        public void Start() => _run = Pipeline.RunAsync(_cts.Token);

        public void Block() => _gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Release()
        {
            var gate = _gate;
            _gate = null;
            gate?.TrySetResult();
        }

        /// <summary>Двигает время и ждёт, пока генератор отработает проход и снова встанет на таймер.</summary>
        public void Step(TimeSpan step)
        {
            var armed = Time.Armed;
            Time.Advance(step);
            WaitUntil(() => Time.Armed > armed);
        }

        public async Task StopGeneratorAsync()
        {
            await _cts.CancelAsync();
            await _run;
        }

        public async Task StopAsync()
        {
            if (_stopped) return;
            _stopped = true;
            Release();
            await StopGeneratorAsync();
            await Pipeline.StopAsync();
        }

        public async ValueTask DisposeAsync()
        {
            await StopAsync();
            _cts.Dispose();
        }
    }

    private static void WaitUntil(Func<bool> condition) =>
        Assert.True(SpinWait.SpinUntil(condition, WaitLimit), "условие не выполнилось за отведённое время");

    private static IEnumerable<long> SeqsOf(IEnumerable<SendRequest> sent) =>
        sent.SelectMany(r => r.Jobs).Select(j => j.Seq);

    [Fact(Timeout = 60_000)]
    public async Task Full_channel_accumulates_tokens_up_to_ceiling_and_resends()
    {
        await using var h = new Harness(rate: 10_000, rng: new ConstRandom(0.5), blocked: true);
        h.Start();

        h.Time.Advance(TimeSpan.FromMilliseconds(200));
        WaitUntil(() => h.Pipeline.Stats.Generated == StalledJobs && h.Blocked == Senders);

        // Генератор ждёт места и не списывает токены: 2000 начислено, 1300 выдано.
        Assert.InRange(h.Bucket.Available(), 699, 701);
        h.Time.Advance(TimeSpan.FromMilliseconds(500));
        Assert.InRange(h.Bucket.Available(), 5_699, 5_701);
        h.Time.Advance(TimeSpan.FromSeconds(2));
        Assert.Equal(10_000, h.Bucket.Available());
        Assert.Equal(StalledJobs, h.Pipeline.Stats.Generated);

        // После освобождения канала досылаются и недовыданные токены цикла, и накопленные до потолка.
        var armed = h.Time.Armed;
        h.Release();
        WaitUntil(() => h.Time.Armed > armed);
        Assert.Equal(2_000, h.Pipeline.Stats.Generated);
        h.Step(TimeSpan.FromMilliseconds(1));
        const int generated = StalledJobs + 10_000 + 10;
        WaitUntil(() => h.Pipeline.Stats.Generated == generated);
        await h.StopAsync();

        Assert.Equal(Enumerable.Range(0, generated).Select(i => (long)i), SeqsOf(h.Sent).Order());
    }

    [Fact(Timeout = 60_000)]
    public async Task Tokens_over_ceiling_are_dropped_and_counted()
    {
        await using var h = new Harness(rate: 5_000, rng: new ConstRandom(0.5), blocked: true);
        h.Start();

        h.Time.Advance(TimeSpan.FromMilliseconds(300));
        WaitUntil(() => h.Pipeline.Stats.Generated == StalledJobs && h.Blocked == Senders);
        h.Time.Advance(TimeSpan.FromSeconds(3));

        // Начислено 5000 × 3,3 с = 16 500; 1300 выдано, 5000 осталось под потолком, остальное отброшено.
        Assert.InRange(h.Pipeline.Stats.Dropped, 10_199, 10_201);
        Assert.Equal(StalledJobs, h.Pipeline.Stats.Generated);
    }

    [Fact(Timeout = 60_000)]
    public async Task Delayed_batch_has_one_delay()
    {
        var picks = 0;
        await using var h = new Harness(rate: 1_000, rng: new ConstRandom(0.05), pickDelay: () => Interlocked.Increment(ref picks));
        h.Start();

        h.Time.Advance(TimeSpan.FromSeconds(1));
        WaitUntil(() => h.Sent.Count == 10);

        Assert.Equal(10, picks);
        Assert.Equal(Enumerable.Range(1, 10), h.Sent.Select(r => r.DelaySeconds).Order());
        Assert.All(h.Sent, r =>
        {
            Assert.True(r.Key.Delayed);
            Assert.All(r.Jobs, j => Assert.True(j.Delayed));
        });
    }

    [Fact(Timeout = 60_000)]
    public async Task Immediate_batch_has_no_delay_and_RunAt_equals_EnqueuedAt()
    {
        var picks = 0;
        await using var h = new Harness(rate: 1_000, rng: new ConstRandom(0.5), pickDelay: () => Interlocked.Increment(ref picks));
        h.Start();

        h.Time.Advance(TimeSpan.FromSeconds(1));
        WaitUntil(() => h.Sent.Count == 10);

        Assert.Equal(0, picks);
        Assert.All(h.Sent, r =>
        {
            Assert.Equal(0, r.DelaySeconds);
            Assert.Equal(r.EnqueuedAtMicros, r.RunAtMicros);
        });
    }

    [Fact(Timeout = 60_000)]
    public async Task Batch_shares_one_EnqueuedAtMicros_and_RunAt_is_EnqueuedAt_plus_delay()
    {
        await using var h = new Harness(rate: 1_000, rng: new ConstRandom(0.05), pickDelay: () => 7, blocked: true);
        h.Start();

        // 10 пачек: 4 застряли в send на отметке 1 с, 6 ждут в канале.
        h.Time.Advance(TimeSpan.FromSeconds(1));
        WaitUntil(() => h.Pipeline.Stats.Generated == 1_000 && h.Blocked == Senders);
        await h.StopGeneratorAsync();
        h.Time.Advance(TimeSpan.FromMilliseconds(500));
        await h.StopAsync();

        // Метка берётся при вызове send, а не при сборке пачки: ждавшие в канале получили 1,5 с.
        Assert.Equal(10, h.Sent.Count);
        Assert.Equal(4, h.Sent.Count(r => r.EnqueuedAtMicros == 1_000_000));
        Assert.Equal(6, h.Sent.Count(r => r.EnqueuedAtMicros == 1_500_000));
        Assert.All(h.Sent, r => Assert.Equal(r.EnqueuedAtMicros + 7_000_000, r.RunAtMicros));
    }

    [Theory(Timeout = 60_000)]
    [InlineData(5_000, 55, 2)]
    [InlineData(50_000, 100, 11)]
    public async Task Batches_are_sent_at_100_jobs_or_10ms_age(int rate, int expectedSize, int expectedBatches)
    {
        await using var h = new Harness(rate, new ConstRandom(0.5));
        h.Start();

        for (var i = 0; i < 22; i++) h.Step(TimeSpan.FromMilliseconds(1));
        WaitUntil(() => h.Sent.Count == expectedBatches);
        await h.StopAsync();

        Assert.Equal(expectedBatches, h.Sent.Count);
        Assert.All(h.Sent, r => Assert.Equal(expectedSize, r.Jobs.Count));
    }

    [Fact(Timeout = 30_000)]
    public async Task Chunk_is_ensured_before_seq_is_issued()
    {
        const int total = (1 << 20) + 150_000;
        var counter = new ExecutionCounter(2 * (1 << 20));
        await using var h = new Harness(
            rate: total,
            rng: new ConstRandom(0.5),
            onSeqIssued: counter.EnsureChunk,
            onSend: request =>
            {
                foreach (var job in request.Jobs) counter.Increment(job.Seq);
            });
        h.Start();

        h.Time.Advance(TimeSpan.FromSeconds(1));
        WaitUntil(() => h.Pipeline.Stats.Generated == total);
        await h.StopAsync();

        for (long seq = 0; seq < total; seq++)
            if (counter.Get(seq) != 1) Assert.Fail($"Seq {seq}: выполнено {counter.Get(seq)} раз");
    }

    [Fact(Timeout = 60_000)]
    public async Task Failing_send_fails_run_and_stop_with_its_exception()
    {
        var h = new Harness(
            rate: 5_000,
            rng: new ConstRandom(0.5),
            onSend: _ => throw new InvalidOperationException("enqueue failed"));
        h.Start();

        // Задач больше, чем вмещают канал и отправители: без реакции на падение генератор ждал бы вечно.
        h.Time.Advance(TimeSpan.FromSeconds(1));

        var run = await Assert.ThrowsAsync<InvalidOperationException>(
            () => h.Run.WaitAsync(WaitLimit, TestContext.Current.CancellationToken));
        var stop = await Assert.ThrowsAsync<InvalidOperationException>(
            () => h.Pipeline.StopAsync().WaitAsync(WaitLimit, TestContext.Current.CancellationToken));
        Assert.Equal("enqueue failed", run.Message);
        Assert.Equal("enqueue failed", stop.Message);
    }

    [Fact(Timeout = 60_000)]
    public async Task SetRate_changes_flow()
    {
        await using var h = new Harness(rate: 1_000, rng: new ConstRandom(0.5));
        h.Start();

        for (var i = 0; i < 100; i++) h.Step(TimeSpan.FromMilliseconds(10));
        Assert.Equal(1_000, h.Pipeline.Stats.Generated);

        h.Pipeline.SetRate(4_000);
        for (var i = 0; i < 100; i++) h.Step(TimeSpan.FromMilliseconds(10));

        Assert.Equal(5_000, h.Pipeline.Stats.Generated);
    }

    [Fact(Timeout = 60_000)]
    public async Task SetRate_changes_ceiling()
    {
        await using var h = new Harness(rate: 1_000, rng: new ConstRandom(0.5), blocked: true);
        h.Start();
        h.Pipeline.SetRate(2_000);

        h.Time.Advance(TimeSpan.FromSeconds(1));
        WaitUntil(() => h.Pipeline.Stats.Generated == StalledJobs && h.Blocked == Senders);
        h.Time.Advance(TimeSpan.FromSeconds(3));

        // Начислено 2000 × 4 с = 8000; 1300 выдано, 2000 осталось под новым потолком, остальное отброшено.
        Assert.InRange(h.Pipeline.Stats.Dropped, 4_699, 4_701);
    }

    [Fact(Timeout = 60_000)]
    public async Task Concurrent_set_rate_and_generation_keep_state_consistent()
    {
        const int low = 20_000, high = 50_000;
        var time = TimeProvider.System;
        var start = time.GetTimestamp();
        var bucket = new TokenBucket(low, low, time);
        var pipeline = new ProducerPipeline(
            new ProfileB(new Random(1)),
            bucket,
            new BatchAssembler(),
            (_, _) => Task.CompletedTask,
            () => time.GetElapsedTime(start).Ticks / 10,
            () => ProfileB.PickDelaySeconds(Random.Shared),
            time,
            _ => { });
        using var cts = new CancellationTokenSource();
        var windows = new List<PipelineStats>();
        var overCeiling = 0;
        var stop = false;

        var run = Task.Run(() => pipeline.RunAsync(cts.Token), TestContext.Current.CancellationToken);
        var scenario = Task.Run(async () =>
        {
            for (var toggle = false; !Volatile.Read(ref stop); toggle = !toggle)
            {
                var rate = toggle ? low : high;
                pipeline.SetRate(rate);
                if (bucket.Available() > rate) Interlocked.Increment(ref overCeiling);
                await Task.Delay(1, TestContext.Current.CancellationToken);
            }
        }, TestContext.Current.CancellationToken);
        var collector = Task.Run(async () =>
        {
            while (!Volatile.Read(ref stop))
            {
                windows.Add(pipeline.TakeWindow());
                await Task.Delay(1, TestContext.Current.CancellationToken);
            }
        }, TestContext.Current.CancellationToken);

        await Task.Delay(300, TestContext.Current.CancellationToken);
        Volatile.Write(ref stop, true);
        await Task.WhenAll(scenario, collector);
        await cts.CancelAsync();
        await run;
        await pipeline.StopAsync();
        var elapsed = time.GetElapsedTime(start);
        windows.Add(pipeline.TakeWindow());

        var stats = pipeline.Stats;
        Assert.Equal(0, overCeiling);
        Assert.True(stats.Generated > 0);
        Assert.Equal(stats.Generated, windows.Sum(w => w.Generated));
        Assert.Equal(stats.Dropped, windows.Sum(w => w.Dropped));
        Assert.True(stats.Generated <= high * elapsed.TotalSeconds + 1);
    }

    [Fact(Timeout = 60_000)]
    public async Task TakeWindow_returns_deltas_and_resets()
    {
        await using var h = new Harness(rate: 5_000, rng: new ConstRandom(0.5), blocked: true);
        h.Start();
        h.Time.Advance(TimeSpan.FromMilliseconds(300));
        WaitUntil(() => h.Pipeline.Stats.Generated == StalledJobs && h.Blocked == Senders);

        var first = h.Pipeline.TakeWindow();

        Assert.Equal(StalledJobs, first.Generated);
        Assert.Equal(0, first.Dropped);
        Assert.Equal(TimeSpan.Zero, first.MaxGeneratorWait);

        // Генератор ждёт места 2 с, затем досылает остаток цикла (до 1500 задач). Потолок применяется при
        // начислении, поэтому начисляем до того, как генератор спишет остаток: токенов 200 + 10 000 сверх
        // потолка 5000 отбрасывается 5200.
        h.Time.Advance(TimeSpan.FromSeconds(2));
        Assert.Equal(5_000, h.Bucket.Available());
        h.Release();
        WaitUntil(() => h.Pipeline.Stats.Generated == 1_500);
        var second = h.Pipeline.TakeWindow();
        var third = h.Pipeline.TakeWindow();

        Assert.Equal(200, second.Generated);
        Assert.InRange(second.Dropped, 5_199, 5_201);
        Assert.InRange(second.MaxGeneratorWait, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2.001));
        Assert.Equal(new PipelineStats(0, 0, TimeSpan.Zero), third);
        var total = h.Pipeline.Stats;
        Assert.Equal(1_500, total.Generated);
        Assert.Equal(first.Dropped + second.Dropped, total.Dropped);
        Assert.Equal(second.MaxGeneratorWait, total.MaxGeneratorWait);
    }

    [Fact(Timeout = 60_000)]
    public async Task Stop_zeroes_rate_so_no_tokens_are_dropped_after_it()
    {
        await using var h = new Harness(rate: 1_000, rng: new ConstRandom(0.5));
        h.Start();
        h.Step(TimeSpan.FromMilliseconds(100));
        await h.StopAsync();
        h.Pipeline.TakeWindow();

        h.Time.Advance(TimeSpan.FromSeconds(10));

        Assert.Equal(0, h.Pipeline.TakeWindow().Dropped);
        Assert.Equal(0, h.Bucket.Available());
    }
}
