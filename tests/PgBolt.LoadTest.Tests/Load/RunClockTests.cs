using PgBolt.LoadTest.Load;

namespace PgBolt.LoadTest.Tests.Load;

public sealed class RunClockTests
{
    [Fact]
    public void Reset_restarts_count_from_zero()
    {
        RunClock.Reset();
        Thread.Sleep(50);
        var before = RunClock.NowMicros;
        Assert.True(before >= 50_000);

        RunClock.Reset();

        Assert.True(RunClock.NowMicros < before);
    }
}
