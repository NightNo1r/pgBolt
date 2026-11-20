namespace PgBolt.Server;

/// <summary>Задержка до следующей попытки после ошибки задачи (§3.4).</summary>
public abstract class RetryPolicy
{
    /// <param name="attempt">Номер завершившейся попытки, 1-based (как <c>JobContext.Attempt</c>).</param>
    /// <returns>Задержка до следующей попытки; null — не повторять.</returns>
    public abstract TimeSpan? GetDelay(int attempt, Exception error);

    /// <summary>По умолчанию: <c>Exponential(15 с, 1 ч, 0.2)</c>.</summary>
    public static RetryPolicy Default { get; } = Exponential(TimeSpan.FromSeconds(15), TimeSpan.FromHours(1), 0.2);

    /// <summary><c>min(baseDelay × 2^(attempt−1), maxDelay) × (1 + U(−jitter, +jitter))</c>.</summary>
    public static RetryPolicy Exponential(TimeSpan baseDelay, TimeSpan maxDelay, double jitter) =>
        Exponential(baseDelay, maxDelay, jitter, static () => Random.Shared.NextDouble());

    /// <param name="random">Источник U[0, 1) — для детерминированных тестов.</param>
    internal static RetryPolicy Exponential(TimeSpan baseDelay, TimeSpan maxDelay, double jitter, Func<double> random)
    {
        if (baseDelay <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(baseDelay), baseDelay, "baseDelay must be > 0.");
        if (maxDelay < baseDelay)
            throw new ArgumentOutOfRangeException(nameof(maxDelay), maxDelay, "maxDelay must be >= baseDelay.");
        if (!(jitter >= 0 && jitter < 1))
            throw new ArgumentOutOfRangeException(nameof(jitter), jitter, "jitter must be in [0, 1).");
        ArgumentNullException.ThrowIfNull(random);
        return new ExponentialRetryPolicy(baseDelay, maxDelay, jitter, random);
    }

    /// <summary>Политика на делегате; исключение делегата обрабатывается как null (§3.4).</summary>
    public static RetryPolicy Custom(Func<int, Exception, TimeSpan?> getDelay)
    {
        ArgumentNullException.ThrowIfNull(getDelay);
        return new CustomRetryPolicy(getDelay);
    }

    private sealed class ExponentialRetryPolicy(TimeSpan baseDelay, TimeSpan maxDelay, double jitter, Func<double> random)
        : RetryPolicy
    {
        public override TimeSpan? GetDelay(int attempt, Exception error)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
            var capped = Capped(attempt - 1);
            if (jitter == 0)
                return capped;
            // Jitter после потолка: результат в capped × (1 ± jitter).
            var factor = 1 + jitter * (2 * random() - 1);
            var ticks = capped.Ticks * factor;
            return ticks >= TimeSpan.MaxValue.Ticks ? TimeSpan.MaxValue : TimeSpan.FromTicks((long)Math.Round(ticks));
        }

        /// <summary>min(baseDelay × 2^exponent, maxDelay) в целых тиках, без переполнения при любом exponent.</summary>
        private TimeSpan Capped(int exponent)
        {
            if (exponent >= 62)
                return maxDelay;
            var multiplier = 1L << exponent;
            return baseDelay.Ticks > maxDelay.Ticks / multiplier
                ? maxDelay
                : TimeSpan.FromTicks(Math.Min(baseDelay.Ticks * multiplier, maxDelay.Ticks));
        }
    }

    private sealed class CustomRetryPolicy(Func<int, Exception, TimeSpan?> getDelay) : RetryPolicy
    {
        public override TimeSpan? GetDelay(int attempt, Exception error) => getDelay(attempt, error);
    }
}
