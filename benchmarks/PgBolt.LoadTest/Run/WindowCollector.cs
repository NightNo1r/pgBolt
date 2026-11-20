using PgBolt.LoadTest.Load;
using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Run;

/// <summary>
/// Статистика продюсера за окно: <see cref="Enqueued"/> — задачи пачек, вызов <c>EnqueueManyAsync</c> которых
/// успешно завершился в окне; <see cref="EnqueueCalls"/> — завершившиеся в окне вызовы, включая неуспешные.
/// </summary>
internal sealed record ProducerWindow(long Enqueued, long EnqueueCalls, long EnqueueErrors, double EnqueueP99Ms,
    TimeSpan MaxGeneratorWait, long DroppedTokens);

/// <summary>
/// Закрывает окна 10 с: приращения счётчиков с прошлого закрытия (первое — с создания) и сводки источников за окно.
/// Доля CPU — приращение процессорного времени на 10 с всех ядер, поэтому у неполного окна она занижена.
/// Рассчитан на один вызывающий поток.
/// </summary>
internal sealed class WindowCollector(
    Func<long> executedTotal, Func<LatencySummary> closeImmediate, Func<LatencySummary> closeDelayed,
    Func<ProducerWindow> takeProducerWindow, Func<TimeSpan> processorTime, int processorCount)
{
    private static readonly TimeSpan Length = TimeSpan.FromSeconds(10);

    private long _executed = executedTotal();
    private TimeSpan _cpu = processorTime();

    public WindowStats CloseWindow(int index, TimeSpan start)
    {
        var executed = executedTotal();
        var cpu = processorTime();
        var producer = takeProducerWindow();
        var window = new WindowStats(index, start, executed - _executed, producer.Enqueued, producer.EnqueueCalls,
            producer.EnqueueErrors, producer.EnqueueP99Ms, (cpu - _cpu) / (Length * processorCount),
            producer.MaxGeneratorWait, producer.DroppedTokens, closeImmediate(), closeDelayed());
        (_executed, _cpu) = (executed, cpu);
        return window;
    }

    /// <summary>Закрывает окна 10 с на [from, to) по часам прогона; последнее короче, если отрезок не кратен 10 с.</summary>
    public async Task CloseWindowsAsync(List<WindowStats> windows, TimeSpan from, TimeSpan to, CancellationToken ct)
    {
        for (var start = from; start < to; start += Length)
        {
            await RunClock.DelayUntilAsync(start + Length < to ? start + Length : to, ct);
            windows.Add(CloseWindow(windows.Count, start));
        }
    }
}
