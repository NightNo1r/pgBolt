using Microsoft.Extensions.Logging;

namespace PgBolt.Server.Runtime;

/// <summary>Выбор задержки повтора (§3.4): <c>retry_delays</c> задачи → политика сервера.</summary>
internal sealed partial class RetryDecider(RetryPolicy policy, ILogger logger)
{
    private readonly RetryPolicy _policy = policy ?? throw new ArgumentNullException(nameof(policy));
    private readonly ILogger _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <param name="attempt">Номер завершившейся попытки, 1-based.</param>
    /// <param name="retryDelaysSeconds">retry_delays задачи; элемент [attempt−1], при нехватке — последний; пустой = null.</param>
    /// <returns>Задержка (≥ 0); null — не повторять.</returns>
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
