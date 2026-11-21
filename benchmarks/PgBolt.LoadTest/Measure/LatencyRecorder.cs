using HdrHistogram;

namespace PgBolt.LoadTest.Measure;

/// <summary>Latency percentiles in milliseconds. An empty histogram has all values zero.</summary>
internal sealed record LatencySummary(long Count, double P50Ms, double P90Ms, double P99Ms, double P999Ms, double MaxMs);

/// <summary>
/// Start latency histogram written by many threads, with one thread closing 10-second windows. Each closed window's histogram contributes to the
/// total since <see cref="ResetTotal"/> and the segment since <see cref="BeginSegment"/>. Total and segment percentiles are computed from pooled
/// observations.
/// </summary>
internal sealed class LatencyRecorder
{
    /// <summary>Caps latency at 60 seconds so recording a measurement in the job handler never throws for a larger value.</summary>
    private const long MaxMicros = 60_000_000;

    // Recorder только переключает гистограммы окна; одновременную запись из многих потоков держит сама гистограмма.
    private readonly Recorder _recorder = new(1, MaxMicros, 3,
        (id, lowest, highest, digits) => new LongConcurrentHistogram(id, lowest, highest, digits));
    private readonly LongHistogram _total = new(1, MaxMicros, 3);
    private readonly LongHistogram _segment = new(1, MaxMicros, 3);
    private readonly Lock _lock = new();
    private HistogramBase? _interval;
    private long _overflows;

    /// <summary>Number of values above 60 seconds capped at 60 seconds.</summary>
    public long Overflows => Interlocked.Read(ref _overflows);

    public void Record(long micros)
    {
        if (micros > MaxMicros)
        {
            micros = MaxMicros;
            Interlocked.Increment(ref _overflows);
        }

        // Метки продюсера и обработчика сняты в разных потоках; отрицательное значение HdrHistogram не принимает.
        _recorder.RecordValue(Math.Max(micros, 0));
    }

    public LatencySummary CloseWindow()
    {
        lock (_lock)
        {
            _interval = _recorder.GetIntervalHistogram(_interval);
            _total.Add(_interval);
            _segment.Add(_interval);
            return Summarize(_interval);
        }
    }

    public LatencySummary Total()
    {
        lock (_lock) return Summarize(_total);
    }

    public void ResetTotal()
    {
        lock (_lock) _total.Reset();
    }

    public void BeginSegment()
    {
        lock (_lock) _segment.Reset();
    }

    /// <summary>
    /// Summarizes windows closed since the last <see cref="BeginSegment"/>. Before the first <see cref="BeginSegment"/>, the segment starts with the
    /// run.
    /// </summary>
    public LatencySummary SegmentSummary()
    {
        lock (_lock) return Summarize(_segment);
    }

    // У пустой гистограммы HdrHistogram не находит перцентиль и бросает.
    private static LatencySummary Summarize(HistogramBase h) => h.TotalCount == 0 ? new(0, 0, 0, 0, 0, 0) : new(
        h.TotalCount, Ms(h.GetValueAtPercentile(50)), Ms(h.GetValueAtPercentile(90)),
        Ms(h.GetValueAtPercentile(99)), Ms(h.GetValueAtPercentile(99.9)), Ms(h.GetMaxValue()));

    private static double Ms(long micros) => micros / 1_000.0;
}
