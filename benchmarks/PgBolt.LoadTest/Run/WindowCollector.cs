using PgBolt.LoadTest.Load;
using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Run;

/// <summary>
/// Producer statistics per window. <see cref="Enqueued"/> counts jobs in batches whose <c>EnqueueManyAsync</c> calls succeeded during the
/// window. <see cref="EnqueueCalls"/> counts all calls that finished in it, including failures.
/// </summary>
internal sealed record ProducerWindow(long Enqueued, long EnqueueCalls, long EnqueueErrors, double EnqueueP99Ms,
    TimeSpan MaxGeneratorWait, long DroppedTokens);

/// <summary>
/// Closes 10-second windows using counter deltas since the previous close (or creation for the first) and per-window source summaries. Divides
/// process CPU time by 10 seconds across all cores, which underestimates CPU usage for a partial window. Intended for one caller thread.
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

    /// <summary>
    /// Closes 10-second windows in [from, to) on the run clock. The final window is shorter if the interval is not a multiple of 10 seconds.
    /// </summary>
    public async Task CloseWindowsAsync(List<WindowStats> windows, TimeSpan from, TimeSpan to, CancellationToken ct)
    {
        for (var start = from; start < to; start += Length)
        {
            await RunClock.DelayUntilAsync(start + Length < to ? start + Length : to, ct);
            windows.Add(CloseWindow(windows.Count, start));
        }
    }
}
