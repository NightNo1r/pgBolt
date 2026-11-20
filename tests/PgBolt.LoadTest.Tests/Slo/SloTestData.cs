using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Slo;

namespace PgBolt.LoadTest.Tests.Slo;

/// <summary>Синтетические окна, срезы и прогоны с «хорошими» значениями по умолчанию.</summary>
internal static class SloTestData
{
    public const int Rate = 10_000;
    public const long PerWindow = Rate * 10L;

    public static readonly LatencySummary GoodImmediate = new(1_000, 10, 20, 100, 500, 900);
    public static readonly LatencySummary GoodDelayed = new(1_000, 100, 200, 500, 900, 1_000);
    public static readonly LatencySummary Empty = new(0, 0, 0, 0, 0, 0);

    public static WindowStats Window(double startSeconds, long completed = PerWindow, long enqueued = PerWindow,
        double cpu = 0.1, LatencySummary? immediate = null, LatencySummary? delayed = null) =>
        new((int)(startSeconds / 10), TimeSpan.FromSeconds(startSeconds), completed, enqueued, 400, 0, 5, cpu,
            TimeSpan.Zero, 0, immediate ?? GoodImmediate, delayed ?? GoodDelayed);

    public static Sample Sample(double atSeconds, long? backlog = 100, long? queueBytes = 1_000_000,
        long? maxWaitMicros = 1_000, int? connections = 30) =>
        new(TimeSpan.FromSeconds(atSeconds), backlog, queueBytes, maxWaitMicros, 0, connections);

    /// <summary>Окна 10 с, начинающиеся в [from, to).</summary>
    public static List<WindowStats> Windows(double from, double to) =>
        Enumerable.Range(0, (int)((to - from) / 10)).Select(i => Window(from + i * 10)).ToList();

    /// <summary>Срезы раз в секунду в [from, to).</summary>
    public static List<Sample> Samples(double from, double to) =>
        Enumerable.Range(0, (int)(to - from)).Select(i => Sample(from + i)).ToList();

    public static readonly CorrectnessData Correct = new(0, 0, 0, 0, 0, true, 0, 0, 0);

    /// <summary>Прогон <c>sustained</c>: окно замера [60 с, 660 с), окна и срезы только в нём.</summary>
    public static RunData Sustained(LoadMode mode = LoadMode.Direct) =>
        new(Rate, Windows(60, 660), Samples(60, 660), GoodImmediate, GoodDelayed, Correct, mode, null,
            TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(660));

    public static readonly PhaseMarks VacuumPhases = new(
        TimeSpan.FromSeconds(120), TimeSpan.FromSeconds(420), TimeSpan.FromSeconds(720), TimeSpan.FromSeconds(1_320));

    /// <summary>Прогон <c>vacuum</c>: прогрев [0, 120 с), фаза 1 — 5 мин, фаза 2 — 5 мин, фаза 3 — 10 мин.</summary>
    public static RunData Vacuum(PhaseMarks? phases = null)
    {
        var p = phases ?? VacuumPhases;
        return new(Rate, Windows(0, p.End.TotalSeconds), Samples(0, p.End.TotalSeconds), GoodImmediate, GoodDelayed,
            null, LoadMode.Direct, p, p.Phase1Start, p.End);
    }

    public static SloVerdict Single(IReadOnlyList<SloVerdict> verdicts, int number) =>
        Assert.Single(verdicts, v => v.Number == number);

    /// <summary>Заменяет элементы списка, удовлетворяющие условию.</summary>
    public static List<T> Replace<T>(IEnumerable<T> items, Func<T, bool> match, Func<T, T> change) =>
        items.Select(x => match(x) ? change(x) : x).ToList();
}
