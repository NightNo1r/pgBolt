using Microsoft.Extensions.Logging;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Unit;

public sealed class RetryDeciderTests
{
    private static readonly Exception Error = new InvalidOperationException("boom");

    /// <summary>Политика «attempt минут» — отличима от любых retry_delays в тестах.</summary>
    private static readonly RetryPolicy Minutes = RetryPolicy.Custom((attempt, _) => TimeSpan.FromMinutes(attempt));

    private readonly ListLogger _logger = new();

    [Theory]
    [InlineData(1, 5)]
    [InlineData(2, 10)]
    [InlineData(3, 20)]
    public void Job_retry_delays_win_over_policy(int attempt, int expectedSeconds)
    {
        var decider = new RetryDecider(Minutes, _logger);
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), decider.GetDelay(attempt, [5, 10, 20], Error));
    }

    [Theory]
    [InlineData(4)]
    [InlineData(100)]
    public void Last_retry_delay_repeats_when_array_is_short(int attempt)
    {
        var decider = new RetryDecider(Minutes, _logger);
        Assert.Equal(TimeSpan.FromSeconds(20), decider.GetDelay(attempt, [5, 10, 20], Error));
    }

    [Fact]
    public void Empty_retry_delays_fall_back_to_policy()
    {
        var decider = new RetryDecider(Minutes, _logger);
        Assert.Equal(TimeSpan.FromMinutes(2), decider.GetDelay(2, [], Error));
    }

    [Fact]
    public void Null_retry_delays_fall_back_to_policy()
    {
        var decider = new RetryDecider(Minutes, _logger);
        Assert.Equal(TimeSpan.FromMinutes(3), decider.GetDelay(3, null, Error));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void Zero_or_negative_retry_delay_means_immediate_retry(int seconds)
    {
        var decider = new RetryDecider(Minutes, _logger);
        Assert.Equal(TimeSpan.Zero, decider.GetDelay(1, [seconds], Error));
    }

    [Fact]
    public void Policy_null_means_no_retry()
    {
        var decider = new RetryDecider(RetryPolicy.Custom((_, _) => null), _logger);
        Assert.Null(decider.GetDelay(1, null, Error));
    }

    [Fact]
    public void Negative_policy_delay_is_clamped_to_zero()
    {
        var decider = new RetryDecider(RetryPolicy.Custom((_, _) => TimeSpan.FromSeconds(-1)), _logger);
        Assert.Equal(TimeSpan.Zero, decider.GetDelay(1, null, Error));
    }

    [Fact]
    public void Throwing_policy_is_logged_and_means_no_retry()
    {
        var thrown = new NotSupportedException("policy bug");
        var decider = new RetryDecider(RetryPolicy.Custom((_, _) => throw thrown), _logger);

        Assert.Null(decider.GetDelay(2, null, Error));

        var entry = Assert.Single(_logger.At(LogLevel.Error));
        Assert.Same(thrown, entry.Exception);
    }

    [Fact]
    public void Policy_receives_one_based_attempt_and_the_job_error()
    {
        var calls = new List<(int, Exception)>();
        var decider = new RetryDecider(RetryPolicy.Custom((attempt, error) =>
        {
            calls.Add((attempt, error));
            return TimeSpan.Zero;
        }), _logger);

        decider.GetDelay(1, null, Error);
        decider.GetDelay(2, [], Error);

        Assert.Equal([(1, Error), (2, Error)], calls);
    }

    [Fact]
    public void No_executor_error_is_passed_to_policy()
    {
        Exception? seen = null;
        var decider = new RetryDecider(RetryPolicy.Custom((_, error) => { seen = error; return null; }), _logger);

        Assert.Null(decider.GetDelay(1, null, new JobExecutorNotFoundException("x.job")));
        Assert.Equal("x.job", Assert.IsType<JobExecutorNotFoundException>(seen).JobName);
    }

    [Fact]
    public void Attempt_below_one_is_rejected() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new RetryDecider(Minutes, _logger).GetDelay(0, [1], Error));
}
