using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Run;

namespace PgBolt.LoadTest.Tests.Run;

public sealed class WindowCollectorTests
{
    [Fact]
    public void Cpu_fraction_and_completed_delta_per_window()
    {
        long executed = 100;
        var cpu = TimeSpan.FromSeconds(1);
        var immediate = new Queue<LatencySummary>([new(10, 1, 2, 3, 4, 5), new(20, 6, 7, 8, 9, 10)]);
        var delayed = new Queue<LatencySummary>([new(1, 11, 12, 13, 14, 15), new(2, 16, 17, 18, 19, 20)]);
        var producer = new Queue<ProducerWindow>(
        [
            new(5_000, 50, 1, 12.5, TimeSpan.FromMilliseconds(3), 7),
            new(4_000, 40, 0, 8, TimeSpan.Zero, 0),
        ]);
        var collector = new WindowCollector(() => executed, immediate.Dequeue, delayed.Dequeue, producer.Dequeue,
            () => cpu, processorCount: 4);

        executed = 5_100;
        cpu = TimeSpan.FromSeconds(21);
        var first = collector.CloseWindow(0, TimeSpan.FromSeconds(5));
        executed = 9_100;
        cpu = TimeSpan.FromSeconds(31);
        var second = collector.CloseWindow(1, TimeSpan.FromSeconds(15));

        Assert.Equal(new WindowStats(0, TimeSpan.FromSeconds(5), 5_000, 5_000, 50, 1, 12.5, 0.5,
            TimeSpan.FromMilliseconds(3), 7, new(10, 1, 2, 3, 4, 5), new(1, 11, 12, 13, 14, 15)), first);
        Assert.Equal(new WindowStats(1, TimeSpan.FromSeconds(15), 4_000, 4_000, 40, 0, 8, 0.25,
            TimeSpan.Zero, 0, new(20, 6, 7, 8, 9, 10), new(2, 16, 17, 18, 19, 20)), second);
    }
}
