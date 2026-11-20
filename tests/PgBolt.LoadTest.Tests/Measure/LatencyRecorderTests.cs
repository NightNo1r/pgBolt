using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Tests.Measure;

public sealed class LatencyRecorderTests
{
    private const long Ms = 1_000;

    // HdrHistogram с 3 значащими цифрами возвращает значение с точностью до 0,1 %.
    private static void Near(double expected, double actual) =>
        Assert.InRange(actual, expected * 0.999, expected * 1.001);

    private static void RecordMs(LatencyRecorder recorder, long ms, int count)
    {
        for (var i = 0; i < count; i++) recorder.Record(ms * Ms);
    }

    [Fact]
    public void Saturates_above_60s_and_counts_overflow()
    {
        var recorder = new LatencyRecorder();

        recorder.Record(60_000 * Ms);
        recorder.Record(61_000 * Ms);
        recorder.Record(long.MaxValue);
        var window = recorder.CloseWindow();

        Assert.Equal(2, recorder.Overflows);
        Assert.Equal(3, window.Count);
        Near(60_000, window.MaxMs);
    }

    [Fact]
    public void Window_and_total_percentiles()
    {
        var recorder = new LatencyRecorder();

        for (var ms = 1; ms <= 1_000; ms++) recorder.Record(ms * Ms);
        var window = recorder.CloseWindow();
        for (var ms = 1; ms <= 1_000; ms++) recorder.Record(ms * Ms);
        recorder.CloseWindow();
        var total = recorder.Total();

        Assert.Equal(1_000, window.Count);
        Near(500, window.P50Ms);
        Near(900, window.P90Ms);
        Near(990, window.P99Ms);
        Near(999, window.P999Ms);
        Near(1_000, window.MaxMs);
        Assert.Equal(2_000, total.Count);
        Near(500, total.P50Ms);
        Near(990, total.P99Ms);
    }

    [Fact]
    public void Concurrent_record_is_safe()
    {
        var recorder = new LatencyRecorder();
        var start = new Barrier(9);
        var threads = Enumerable.Range(0, 8).Select(t => new Thread(() =>
        {
            start.SignalAndWait();
            for (var i = 0; i < 100_000; i++) recorder.Record((t + 1) * Ms);
        })).ToList();
        threads.ForEach(t => t.Start());

        // Окна закрываются во время записи: ни одно значение не теряется и не учитывается дважды.
        long windows = 0;
        start.SignalAndWait(TestContext.Current.CancellationToken);
        while (threads.Any(t => t.IsAlive)) windows += recorder.CloseWindow().Count;
        threads.ForEach(t => t.Join());
        windows += recorder.CloseWindow().Count;

        Assert.Equal(800_000, windows);
        Assert.Equal(800_000, recorder.Total().Count);
    }

    [Fact]
    public void Total_excludes_windows_before_reset()
    {
        var recorder = new LatencyRecorder();
        RecordMs(recorder, 900, 100);
        recorder.CloseWindow();

        recorder.ResetTotal();
        RecordMs(recorder, 10, 50);
        recorder.CloseWindow();
        RecordMs(recorder, 20, 50);
        recorder.CloseWindow();
        var total = recorder.Total();

        Assert.Equal(100, total.Count);
        Near(20, total.MaxMs);
    }

    [Fact]
    public void Segment_summary_merges_windows()
    {
        var recorder = new LatencyRecorder();
        RecordMs(recorder, 900, 1_000);
        recorder.CloseWindow();

        recorder.BeginSegment();
        RecordMs(recorder, 600, 100);
        var slowWindow = recorder.CloseWindow();
        RecordMs(recorder, 10, 9_900);
        recorder.CloseWindow();
        var segment = recorder.SegmentSummary();

        Near(600, slowWindow.P99Ms);
        Assert.Equal(10_000, segment.Count);
        Near(10, segment.P99Ms);
        Near(600, segment.MaxMs);
    }

    [Fact]
    public void Begin_segment_starts_empty()
    {
        var recorder = new LatencyRecorder();
        recorder.BeginSegment();
        RecordMs(recorder, 10, 10);
        recorder.CloseWindow();

        recorder.BeginSegment();

        Assert.Equal(0, recorder.SegmentSummary().Count);
    }
}
