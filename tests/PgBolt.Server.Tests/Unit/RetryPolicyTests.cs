namespace PgBolt.Server.Tests.Unit;

public sealed class RetryPolicyTests
{
    private static readonly Exception Error = new InvalidOperationException("boom");

    [Fact]
    public void Exponential_without_jitter_doubles_up_to_the_cap()
    {
        var policy = RetryPolicy.Exponential(TimeSpan.FromSeconds(15), TimeSpan.FromHours(1), 0);

        TimeSpan?[] expected =
        [
            TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2),
            TimeSpan.FromMinutes(4), TimeSpan.FromMinutes(8), TimeSpan.FromMinutes(16), TimeSpan.FromMinutes(32),
            TimeSpan.FromHours(1), TimeSpan.FromHours(1),
        ];
        Assert.Equal(expected, Enumerable.Range(1, 10).Select(a => policy.GetDelay(a, Error)));
    }

    [Theory]
    [InlineData(63)]
    [InlineData(64)]
    [InlineData(10_000)]
    [InlineData(int.MaxValue)]
    public void Large_attempt_returns_the_cap_without_overflow(int attempt)
    {
        var policy = RetryPolicy.Exponential(TimeSpan.FromSeconds(15), TimeSpan.FromHours(1), 0);
        Assert.Equal(TimeSpan.FromHours(1), policy.GetDelay(attempt, Error));
    }

    [Fact]
    public void Huge_cap_with_huge_attempt_does_not_overflow()
    {
        var policy = RetryPolicy.Exponential(TimeSpan.FromTicks(1), TimeSpan.MaxValue, 0);
        Assert.Equal(TimeSpan.FromTicks(1L << 40), policy.GetDelay(41, Error));
        Assert.Equal(TimeSpan.MaxValue, policy.GetDelay(10_000, Error));
    }

    [Fact]
    public void Jitter_lower_bound_is_applied_after_the_cap()
    {
        var policy = RetryPolicy.Exponential(TimeSpan.FromSeconds(15), TimeSpan.FromHours(1), 0.2, () => 0.0);

        AssertClose(TimeSpan.FromSeconds(15 * 0.8), policy.GetDelay(1, Error));
        AssertClose(TimeSpan.FromSeconds(3600 * 0.8), policy.GetDelay(10_000, Error));
    }

    [Fact]
    public void Jitter_upper_bound_stays_below_one_plus_jitter()
    {
        var almostOne = Math.BitDecrement(1.0);
        var policy = RetryPolicy.Exponential(TimeSpan.FromSeconds(15), TimeSpan.FromHours(1), 0.2, () => almostOne);

        var capped = policy.GetDelay(10_000, Error)!.Value;
        AssertClose(TimeSpan.FromSeconds(3600 * 1.2), capped);
        Assert.True(capped <= TimeSpan.FromSeconds(3600 * 1.2));
    }

    [Fact]
    public void Jitter_midpoint_returns_the_nominal_delay()
    {
        var policy = RetryPolicy.Exponential(TimeSpan.FromSeconds(15), TimeSpan.FromHours(1), 0.2, () => 0.5);
        Assert.Equal(TimeSpan.FromSeconds(30), policy.GetDelay(2, Error));
    }

    [Fact]
    public void Random_jitter_stays_within_bounds()
    {
        var policy = RetryPolicy.Exponential(TimeSpan.FromSeconds(15), TimeSpan.FromHours(1), 0.2);
        for (var i = 0; i < 1000; i++)
            Assert.InRange(policy.GetDelay(3, Error)!.Value, TimeSpan.FromSeconds(48), TimeSpan.FromSeconds(72));
    }

    [Fact]
    public void Default_is_exponential_15s_1h_20_percent()
    {
        Assert.Same(RetryPolicy.Default, RetryPolicy.Default);
        for (var i = 0; i < 200; i++)
        {
            Assert.InRange(RetryPolicy.Default.GetDelay(1, Error)!.Value, TimeSpan.FromSeconds(12), TimeSpan.FromSeconds(18));
            Assert.InRange(RetryPolicy.Default.GetDelay(9, Error)!.Value, TimeSpan.FromSeconds(2880), TimeSpan.FromSeconds(4320));
        }
    }

    [Fact]
    public void Server_options_default_to_the_default_policy() =>
        Assert.Same(RetryPolicy.Default, new PgBoltServerOptions().RetryPolicy);

    [Fact]
    public void Null_retry_policy_is_rejected_by_validation()
    {
        var o = new PgBoltServerOptions { Workers = 1, RetryPolicy = null! }.Queue("q");
        var e = Assert.Throws<InvalidOperationException>(() => o.Validate("orders"));
        Assert.Contains("'orders'", e.Message, StringComparison.Ordinal);
        Assert.Contains("RetryPolicy", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Exponential_rejects_attempt_below_one() =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            RetryPolicy.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), 0).GetDelay(0, Error));

    [Theory]
    [InlineData(0, 10, 0.2)]      // baseDelay = 0
    [InlineData(-1, 10, 0.2)]     // baseDelay < 0
    [InlineData(10, 9, 0.2)]      // maxDelay < baseDelay
    [InlineData(1, 10, -0.01)]    // jitter < 0
    [InlineData(1, 10, 1.0)]      // jitter = 1
    [InlineData(1, 10, double.NaN)]
    public void Exponential_validates_arguments(int baseSeconds, int maxSeconds, double jitter) =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            RetryPolicy.Exponential(TimeSpan.FromSeconds(baseSeconds), TimeSpan.FromSeconds(maxSeconds), jitter));

    [Fact]
    public void Exponential_accepts_equal_base_and_max()
    {
        var policy = RetryPolicy.Exponential(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5), 0);
        Assert.Equal(TimeSpan.FromSeconds(5), policy.GetDelay(1, Error));
        Assert.Equal(TimeSpan.FromSeconds(5), policy.GetDelay(7, Error));
    }

    [Fact]
    public void Custom_passes_attempt_and_error_through()
    {
        (int Attempt, Exception Error)? seen = null;
        var policy = RetryPolicy.Custom((attempt, error) =>
        {
            seen = (attempt, error);
            return attempt < 3 ? TimeSpan.FromSeconds(attempt) : null;
        });

        Assert.Equal(TimeSpan.FromSeconds(2), policy.GetDelay(2, Error));
        Assert.Equal((2, Error), seen);
        Assert.Null(policy.GetDelay(3, Error));
    }

    [Fact]
    public void Custom_rejects_null_delegate() => Assert.Throws<ArgumentNullException>(() => RetryPolicy.Custom(null!));

    [Fact]
    public void Executor_not_found_exception_carries_job_name()
    {
        var e = new JobExecutorNotFoundException("orders.ship");
        Assert.Equal("orders.ship", e.JobName);
        Assert.Contains("orders.ship", e.Message, StringComparison.Ordinal);
    }

    private static void AssertClose(TimeSpan expected, TimeSpan? actual) =>
        Assert.InRange(actual!.Value, expected - TimeSpan.FromMilliseconds(1), expected + TimeSpan.FromMilliseconds(1));
}
