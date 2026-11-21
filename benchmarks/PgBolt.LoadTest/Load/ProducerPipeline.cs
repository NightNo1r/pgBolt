using System.Threading.Channels;

namespace PgBolt.LoadTest.Load;

/// <summary>Batch ready for <c>EnqueueManyAsync</c>. The caller builds <c>LoadArgs</c> from it.</summary>
/// <param name="DelaySeconds">0 for an immediate batch, or 1–30 for a delayed batch.</param>
/// <param name="EnqueuedAtMicros">One timestamp per batch, taken immediately before sending.</param>
internal sealed record SendRequest(BatchKey Key, IReadOnlyList<JobDraft> Jobs, int DelaySeconds, long EnqueuedAtMicros)
{
    public long RunAtMicros => EnqueuedAtMicros + DelaySeconds * 1_000_000L;
}

/// <param name="Generated">Jobs issued.</param>
/// <param name="Dropped">Tokens discarded above the bucket capacity.</param>
/// <param name="MaxGeneratorWait">Longest time the generator waited for space in a batch channel.</param>
internal readonly record struct PipelineStats(long Generated, long Dropped, TimeSpan MaxGeneratorWait);

/// <summary>
/// Database-independent producer. The generator spends tokens to issue jobs from the profile, assembles batches, and feeds senders through a
/// bounded channel per key. If a channel is full, the generator waits without spending tokens. Tokens accumulate up to capacity for later use.
/// The bucket counts and discards excess tokens. Queued jobs in memory are bounded by channel capacity and batches under construction.
/// </summary>
/// <remarks>
/// An exception from <c>send</c>, <c>nowMicros</c>, or <c>pickDelaySeconds</c> stops the pipeline. The generator stops waiting for channel
/// space. Both <see cref="RunAsync"/> and <see cref="StopAsync"/> report the exception.
/// </remarks>
internal sealed class ProducerPipeline(
    ProfileB profile,
    TokenBucket bucket,
    BatchAssembler assembler,
    Func<SendRequest, CancellationToken, Task> send,
    Func<long> nowMicros,
    Func<int> pickDelaySeconds,
    TimeProvider time,
    Action<long> onSeqIssued,
    int senders = 4,
    int channelCapacityPerKey = 8)
{
    private static readonly TimeSpan IdleTick = TimeSpan.FromMilliseconds(1);

    private readonly Channel<(PendingBatch Batch, SemaphoreSlim Slot)> _ready =
        Channel.CreateUnbounded<(PendingBatch, SemaphoreSlim)>();

    private readonly Dictionary<BatchKey, SemaphoreSlim> _slots = [];
    private readonly CancellationTokenSource _senderFailed = new();
    private Task[] _senders = [];
    private long _generated;
    private long _reportedGenerated;
    private long _reportedDropped;
    private long _maxWaitTicks;
    private long _windowMaxWaitTicks;

    /// <summary>Cumulative values since startup.</summary>
    public PipelineStats Stats =>
        new(Interlocked.Read(ref _generated), bucket.Dropped, TimeSpan.FromTicks(Interlocked.Read(ref _maxWaitTicks)));

    /// <summary>Updates the rate and bucket capacity together.</summary>
    public void SetRate(int rate) => bucket.SetRate(rate);

    /// <summary>
    /// Returns changes in <c>Generated</c> and <c>Dropped</c> and the maximum wait since the previous call, then resets the window. Intended for one
    /// caller thread.
    /// </summary>
    public PipelineStats TakeWindow()
    {
        var generated = Interlocked.Read(ref _generated);
        var dropped = bucket.Dropped;
        return new PipelineStats(
            generated - Interlocked.Exchange(ref _reportedGenerated, generated),
            dropped - Interlocked.Exchange(ref _reportedDropped, dropped),
            TimeSpan.FromTicks(Interlocked.Exchange(ref _windowMaxWaitTicks, 0)));
    }

    /// <summary>Runs the generator until <paramref name="ct"/> is cancelled, finishing the current pass. Propagates sender failures.</summary>
    public async Task RunAsync(CancellationToken ct)
    {
        _senders = [.. Enumerable.Range(0, senders).Select(_ => SendLoopAsync())];
        long seq = 0;
        try
        {
            while (!ct.IsCancellationRequested && !_senderFailed.IsCancellationRequested)
            {
                for (var left = bucket.Available(); left > 0 && bucket.TryConsume(); left--)
                {
                    onSeqIssued(seq);
                    var job = profile.Next(seq++);
                    Interlocked.Increment(ref _generated);
                    if (assembler.Add(job, nowMicros()) is { } full) await EnqueueAsync(full);
                }

                foreach (var aged in assembler.TakeAged(nowMicros())) await EnqueueAsync(aged);

                try
                {
                    await Task.Delay(IdleTick, time, ct);
                }
                catch (OperationCanceledException)
                {
                    // Остановка: условие цикла завершит работу.
                }
            }
        }
        catch (OperationCanceledException) when (_senderFailed.IsCancellationRequested)
        {
            // Ожидание места прервано падением отправителя: исключение отдаст ожидание отправителей ниже.
        }

        if (_senderFailed.IsCancellationRequested) await WaitSendersAsync();
    }

    /// <summary>
    /// Sets the rate to zero, flushes partial batches, and waits for senders. Propagates sender failures. Call after <see cref="RunAsync"/>
    /// completes.
    /// </summary>
    public async Task StopAsync()
    {
        // Генератор остановлен: иначе бакет копил бы токены дальше и статистика показывала бы ложный отброс.
        bucket.SetRate(0);
        try
        {
            foreach (var batch in assembler.TakeAll()) await EnqueueAsync(batch);
        }
        catch (OperationCanceledException) when (_senderFailed.IsCancellationRequested)
        {
            // Упал отправитель: исключение отдаст ожидание отправителей ниже.
        }

        await WaitSendersAsync();
    }

    private Task WaitSendersAsync()
    {
        _ready.Writer.TryComplete();
        return Task.WhenAll(_senders);
    }

    private async Task EnqueueAsync(PendingBatch batch)
    {
        if (!_slots.TryGetValue(batch.Key, out var slot))
            _slots[batch.Key] = slot = new SemaphoreSlim(channelCapacityPerKey);

        if (!slot.Wait(0))
        {
            var waitStart = time.GetTimestamp();
            await slot.WaitAsync(_senderFailed.Token);
            var wait = time.GetElapsedTime(waitStart).Ticks;
            RaiseTo(ref _maxWaitTicks, wait);
            RaiseTo(ref _windowMaxWaitTicks, wait);
        }

        _ready.Writer.TryWrite((batch, slot));
    }

    private async Task SendLoopAsync()
    {
        try
        {
            await foreach (var (batch, slot) in _ready.Reader.ReadAllAsync())
            {
                slot.Release();
                var delaySeconds = batch.Key.Delayed ? pickDelaySeconds() : 0;
                await send(new SendRequest(batch.Key, batch.Jobs, delaySeconds, nowMicros()), CancellationToken.None);
            }
        }
        catch
        {
            _senderFailed.Cancel();
            throw;
        }
    }

    private static void RaiseTo(ref long target, long value)
    {
        long current;
        while (value > (current = Interlocked.Read(ref target))
               && Interlocked.CompareExchange(ref target, value, current) != current)
        {
        }
    }
}
