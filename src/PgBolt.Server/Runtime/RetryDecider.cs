using Microsoft.Extensions.Logging;

namespace PgBolt.Server.Runtime;

/// <summary>Selects the retry delay (§3.4) from the job's <c>retry_delays</c>, falling back to the server policy.</summary>
internal sealed partial class RetryDecider(RetryPolicy policy, ILogger logger)
{
    private readonly RetryPolicy _policy = policy ?? throw new ArgumentNullException(nameof(policy));
    private readonly ILogger _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <param name="attempt">One-based number of the attempt that just finished.</param>
    /// <param name="retryDelaysSeconds">
    /// The job's retry_delays. Uses element [attempt−1], repeating the last value if the array is too short. An empty
    /// array is equivalent to null.
    /// </param>
    /// <param name="error">Exception from the failed attempt, passed to the server retry policy.</param>
    /// <returns>Delay (≥ 0), or null to stop retrying.</returns>
    public TimeSpan? GetDelay(int attempt, int[]? retryDelaysSeconds, Exception error)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
        if (retryDelaysSeconds is { Length: > 0 })
        {
            var seconds = retryDelaysSeconds[Math.Min(attempt - 1, retryDelaysSeconds.Length - 1)];
            return seconds <= 0 ? TimeSpan.Zero : TimeSpan.FromSeconds(seconds);
        }

        TimeSpan? delay;
        try
        {
            delay = _policy.GetDelay(attempt, error);
        }
        catch (Exception e)
        {
            Log.RetryPolicyFailed(_logger, e, attempt);
            return null;
        }

        return delay < TimeSpan.Zero ? TimeSpan.Zero : delay;
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 3100, EventName = nameof(RetryPolicyFailed), Level = LogLevel.Error,
            Message = "PgBolt: RetryPolicy threw for attempt {Attempt}; the job is failed without retry.")]
        internal static partial void RetryPolicyFailed(ILogger logger, Exception e, int attempt);
    }
}
