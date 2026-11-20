using System.Threading.Channels;

namespace PgBolt.LoadTest.Load;

/// <summary>Пачка, готовая к вызову <c>EnqueueManyAsync</c>; <c>LoadArgs</c> из неё собирает вызывающий.</summary>
/// <param name="DelaySeconds">0 у немедленной пачки, 1..30 у отложенной.</param>
/// <param name="EnqueuedAtMicros">Одна метка на пачку, взятая непосредственно перед отправкой.</param>
internal sealed record SendRequest(BatchKey Key, IReadOnlyList<JobDraft> Jobs, int DelaySeconds, long EnqueuedAtMicros)
{
    public long RunAtMicros => EnqueuedAtMicros + DelaySeconds * 1_000_000L;
}

/// <param name="Generated">Выдано задач.</param>
/// <param name="Dropped">Токенов отброшено сверх потолка.</param>
/// <param name="MaxGeneratorWait">Самое долгое ожидание генератором места в канале пачек.</param>
internal readonly record struct PipelineStats(long Generated, long Dropped, TimeSpan MaxGeneratorWait);

/// <summary>
/// Продюсер без БД: генератор по токенам выдаёт задачи профиля, собирает их в пачки и через ограниченный
/// канал на ключ отдаёт отправителям. Если места для пачки ключа нет, генератор ждёт и не списывает токены:
/// они копятся в бакете до потолка и после освобождения канала выдаются, а излишек бакет отбрасывает и считает.
/// Поэтому в памяти не больше задач, чем вмещают каналы и пачки в сборке.
/// </summary>
/// <remarks>
/// Исключение <c>send</c> (как и <c>nowMicros</c>, <c>pickDelaySeconds</c>) останавливает остальных: генератор
/// перестаёт ждать место в канале, а <see cref="RunAsync"/> и <see cref="StopAsync"/> завершаются с этим исключением.
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

    /// <summary>Накопительные значения с начала работы.</summary>
    public PipelineStats Stats =>
        new(Interlocked.Read(ref _generated), bucket.Dropped, TimeSpan.FromTicks(Interlocked.Read(ref _maxWaitTicks)));

    /// <summary>Меняет поток и потолок бакета вместе.</summary>
    public void SetRate(int rate) => bucket.SetRate(rate);

    /// <summary>
    /// Значения с прошлого вызова: приращения <c>Generated</c> и <c>Dropped</c> и максимум ожидания;
    /// значения окна сбрасываются. Рассчитан на один вызывающий поток.
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

    /// <summary>
    /// Цикл генератора; останавливается по <paramref name="ct"/> после начатого прохода. Если отправитель
    /// упал, завершается с его исключением.
    /// </summary>
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
    /// Обнуляет поток, досылает неполные пачки и ждёт отправителей; если отправитель упал, завершается с его
    /// исключением. Вызывать после завершения <see cref="RunAsync"/>.
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
