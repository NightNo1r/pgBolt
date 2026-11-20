using Npgsql;

namespace PgBolt.Postgres.Client;

internal readonly record struct ResolvedEnqueue(
    string Queue, short Priority, int MaxAttempts, int[]? RetryDelaysSeconds,
    DateTime? RunAtUtc, TimeSpan Delay, NpgsqlTransaction? Transaction);

/// <summary>[Job]-умолчания + EnqueueOptions (§6.2): поле опций, если задано, переопределяет умолчание.</summary>
internal static class EnqueueOptionsResolver
{
    public static ResolvedEnqueue Resolve(JobDescriptor descriptor, EnqueueOptions? options)
    {
        if (options is null)
            return new ResolvedEnqueue(descriptor.Queue, descriptor.Priority, descriptor.MaxAttempts,
                descriptor.RetryDelaysSeconds, null, TimeSpan.Zero, null);

        ValidateValues(nameof(EnqueueOptions), options.RunAt, options.Delay, options.MaxAttempts, options.Queue, nameof(options));

        var transaction = options.Transaction switch
        {
            null => null,
            NpgsqlTransaction t => t,
            _ => throw new ArgumentException("EnqueueOptions.Transaction must be an NpgsqlTransaction.", nameof(options)),
        };

        return new ResolvedEnqueue(
            options.Queue ?? descriptor.Queue,
            options.Priority ?? descriptor.Priority,
            options.MaxAttempts ?? descriptor.MaxAttempts,
            options.RetryDelays is null ? descriptor.RetryDelaysSeconds : options.RetryDelays.Select(ToSeconds).ToArray(),
            options.RunAt?.UtcDateTime,
            options.Delay ?? TimeSpan.Zero,
            transaction);
    }

    /// <summary>Значения опций, общие для EnqueueOptions и StepOptions (часть 5 §5.2); type — имя типа опций в сообщениях.</summary>
    internal static void ValidateValues(string type, DateTimeOffset? runAt, TimeSpan? delay, int? maxAttempts, string? queue,
        string paramName)
    {
        if (runAt is not null && delay is not null)
            throw new ArgumentException($"{type}.RunAt and {type}.Delay are mutually exclusive.", paramName);
        if (delay < TimeSpan.Zero)
            throw new ArgumentException($"{type}.Delay must be non-negative.", paramName);
        if (maxAttempts < 1)
            throw new ArgumentException($"{type}.MaxAttempts must be >= 1.", paramName);
        if (queue is { Length: 0 })
            throw new ArgumentException($"{type}.Queue must not be empty.", paramName);
    }

    internal static int ToSeconds(TimeSpan delay) =>
        delay < TimeSpan.Zero
            ? throw new ArgumentException("Retry delays must be non-negative.")
            : checked((int)Math.Ceiling(delay.TotalSeconds));
}
