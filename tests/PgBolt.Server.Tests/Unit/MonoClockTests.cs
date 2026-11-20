using Microsoft.Extensions.Time.Testing;
using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Unit;

public sealed class MonoClockTests
{
    [Fact]
    public void Now_follows_time_provider()
    {
        var time = new FakeTimeProvider();
        var clock = new MonoClock(time);
        var start = clock.Now;

        Assert.Equal(time.GetTimestamp(), start);
        Assert.Same(time, clock.Time);
        time.Advance(TimeSpan.FromSeconds(3));
        Assert.Equal(TimeSpan.FromSeconds(3), time.GetElapsedTime(start, clock.Now));
    }

    [Fact]
    public void Add_matches_advance()
    {
        var time = new FakeTimeProvider();
        var clock = new MonoClock(time);
        var target = clock.Add(clock.Now, TimeSpan.FromSeconds(5));

        time.Advance(TimeSpan.FromSeconds(5));

        Assert.Equal(target, clock.Now);
    }

    [Fact]
    public void Add_accepts_negative_duration()
    {
        var clock = new MonoClock(new FakeTimeProvider());
        var now = clock.Now;
        Assert.Equal(now, clock.Add(clock.Add(now, TimeSpan.FromSeconds(7)), TimeSpan.FromSeconds(-7)));
    }

    [Fact]
    public void Until_counts_down_to_zero_and_stays_there()
    {
        var time = new FakeTimeProvider();
        var clock = new MonoClock(time);
        var target = clock.Add(clock.Now, TimeSpan.FromSeconds(10));

        Assert.Equal(TimeSpan.FromSeconds(10), clock.Until(target));
        time.Advance(TimeSpan.FromSeconds(4));
        Assert.Equal(TimeSpan.FromSeconds(6), clock.Until(target));
        time.Advance(TimeSpan.FromSeconds(6));
        Assert.Equal(TimeSpan.Zero, clock.Until(target));
        time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(TimeSpan.Zero, clock.Until(target));
    }

    [Fact]
    public void Elapsed_grows_and_is_never_negative()
    {
        var time = new FakeTimeProvider();
        var clock = new MonoClock(time);
        var since = clock.Now;

        Assert.Equal(TimeSpan.Zero, clock.Elapsed(since));
        time.Advance(TimeSpan.FromMilliseconds(1500));
        Assert.Equal(TimeSpan.FromMilliseconds(1500), clock.Elapsed(since));
        Assert.Equal(TimeSpan.Zero, clock.Elapsed(clock.Add(clock.Now, TimeSpan.FromSeconds(1))));
    }

    [Fact]
    public void Converts_between_units_at_nanosecond_frequency()
    {
        var time = new ManualTimestamps(frequency: 1_000_000_000) { Timestamp = 1_000 };
        var clock = new MonoClock(time);

        Assert.Equal(1_000 + 100, clock.Add(1_000, TimeSpan.FromTicks(1)));          // 1 тик = 100 нс
        Assert.Equal(1_000 - 2_000_000_000, clock.Add(1_000, TimeSpan.FromSeconds(-2)));
        Assert.Equal(TimeSpan.FromTicks(2), clock.Until(1_000 + 150));               // 150 нс → вверх до 2 тиков
        Assert.Equal(TimeSpan.FromTicks(1), clock.Elapsed(1_000 - 150));             // 150 нс → вниз до 1 тика
    }

    [Fact]
    public void Rejects_null_time_provider() => Assert.Throws<ArgumentNullException>(() => new MonoClock(null!));

    private sealed class ManualTimestamps(long frequency) : TimeProvider
    {
        public long Timestamp { get; set; }
        public override long GetTimestamp() => Timestamp;
        public override long TimestampFrequency => frequency;
    }
}
