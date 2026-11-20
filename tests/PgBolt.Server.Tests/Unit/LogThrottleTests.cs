using Microsoft.Extensions.Time.Testing;
using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Unit;

public sealed class LogThrottleTests
{
    [Fact]
    public void Allows_one_entry_per_period()
    {
        var time = new FakeTimeProvider();
        var throttle = new LogThrottle(new MonoClock(time), TimeSpan.FromMinutes(1));

        Assert.True(throttle.TryEnter());
        time.Advance(TimeSpan.FromSeconds(59));
        Assert.False(throttle.TryEnter());
        time.Advance(TimeSpan.FromSeconds(1));
        Assert.True(throttle.TryEnter());
    }
}
