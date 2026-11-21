using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace PgBolt.Server;

/// <summary>Virtual server settings (§3.2). Registration calls <see cref="Validate"/>.</summary>
public sealed class PgBoltServerOptions
{
    /// <summary>
    /// Upper bound for LeaseDuration, ShutdownTimeout, AckFlushInterval, MaxPollInterval, and maintenance intervals. All become .NET timer delays,
    /// limited to 2³² − 2 ms. A one-day cap leaves room for jitter.
    /// </summary>
    internal static readonly TimeSpan MaxInterval = TimeSpan.FromDays(1);

    /// <summary>Maximum .NET timer delay (2³² − 2 ms, about 49.7 days), which bounds DefaultJobTimeout and [Job(TimeoutSeconds)].</summary>
    internal static readonly TimeSpan MaxJobTimeout = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    // Стенд объединяет длительности FetchAsync обоих хостов в одну гистограмму.
    internal Action<TimeSpan, int?>? FetchMeasured { get; set; }

    private readonly List<QueueOptions> _queues = [];
    private readonly List<Type> _filterTypes = [];
    private readonly List<Action<IServiceCollection>> _filterRegistrations = [];

    /// <summary>Registers a scoped filter in AddFilter call order. AddFilter annotates T for AOT.</summary>
    internal IReadOnlyList<Action<IServiceCollection>> FilterRegistrations => _filterRegistrations;

    /// <summary>Number of slots in the shared worker pool.</summary>
    public int Workers { get; set; } = Environment.ProcessorCount * 2;

    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Must not exceed <c>LeaseDuration / 3</c>.</summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(15);

    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(25);

    /// <summary>Timeout for jobs without <c>[Job(TimeoutSeconds)]</c>. Null disables the default timeout.</summary>
    public TimeSpan? DefaultJobTimeout { get; set; }

    /// <summary>Retry policy for jobs without <c>retry_delays</c> (§3.4).</summary>
    public RetryPolicy RetryPolicy { get; set; } = RetryPolicy.Default;

    public int AckBatchSize { get; set; } = 100;

    public TimeSpan AckFlushInterval { get; set; } = TimeSpan.FromMilliseconds(20);

    /// <summary>Adds a queue. Registration order determines priority in the shared pool (§4.2).</summary>
    public PgBoltServerOptions Queue(string name, Action<QueueOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var queue = new QueueOptions(name);
        configure?.Invoke(queue);
        _queues.Add(queue);
        return this;
    }

    /// <summary>Adds a filter for this server. It runs after global filters (§3.6).</summary>
    public PgBoltServerOptions AddFilter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>()
        where T : class, IJobFilter
    {
        _filterTypes.Add(typeof(T));
        _filterRegistrations.Add(static services => services.TryAddScoped<T>());
        return this;
    }

    internal IReadOnlyList<QueueOptions> Queues => _queues;

    internal IReadOnlyList<Type> FilterTypes => _filterTypes;

    /// <summary>Server capacity, <c>Workers + Σ Reserved</c> (§3.3), published in <c>servers.capacity</c>.</summary>
    internal int Capacity
    {
        get
        {
            var capacity = Workers;
            foreach (var queue in _queues)
                capacity += queue.Reserved;
            return capacity;
        }
    }

    internal void Validate(string serverName)
    {
        if (Workers < 0)
            throw Invalid(serverName, nameof(Workers), "must be >= 0");
        if (_queues.Count == 0)
            throw Invalid(serverName, nameof(Queue), "at least one queue must be declared");

        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var queue in _queues)
        {
            if (!names.Add(queue.Name))
                throw Invalid(serverName, nameof(Queue), $"'{queue.Name}' is declared more than once");
            queue.Validate(serverName);
        }

        if (Capacity < 1)
            throw Invalid(serverName, nameof(Workers), "Workers + sum of Reserved must be >= 1");
        if (LeaseDuration <= TimeSpan.Zero)
            throw Invalid(serverName, nameof(LeaseDuration), "must be > 0");
        if (LeaseDuration > MaxInterval)
            throw Invalid(serverName, nameof(LeaseDuration), $"must be <= {MaxInterval}");
        if (HeartbeatInterval <= TimeSpan.Zero)
            throw Invalid(serverName, nameof(HeartbeatInterval), "must be > 0");
        if (HeartbeatInterval.Ticks > LeaseDuration.Ticks / 3)
            throw Invalid(serverName, nameof(HeartbeatInterval), "must be <= LeaseDuration / 3");   // отсюда и H ≤ MaxInterval / 3
        if (ShutdownTimeout < TimeSpan.Zero)
            throw Invalid(serverName, nameof(ShutdownTimeout), "must be >= 0");
        if (ShutdownTimeout > MaxInterval)
            throw Invalid(serverName, nameof(ShutdownTimeout), $"must be <= {MaxInterval}");
        if (DefaultJobTimeout is { } timeout && timeout <= TimeSpan.Zero)
            throw Invalid(serverName, nameof(DefaultJobTimeout), "must be null or > 0");
        if (DefaultJobTimeout > MaxJobTimeout)
            throw Invalid(serverName, nameof(DefaultJobTimeout), $"must be null or <= {MaxJobTimeout}");
        if (RetryPolicy is null)
            throw Invalid(serverName, nameof(RetryPolicy), "must not be null");
        if (AckBatchSize < 1)
            throw Invalid(serverName, nameof(AckBatchSize), "must be >= 1");
        if (AckFlushInterval <= TimeSpan.Zero)
            throw Invalid(serverName, nameof(AckFlushInterval), "must be > 0");
        if (AckFlushInterval > MaxInterval)
            throw Invalid(serverName, nameof(AckFlushInterval), $"must be <= {MaxInterval}");
    }

    internal static InvalidOperationException Invalid(string serverName, string field, string rule) =>
        new($"PgBolt server '{serverName}': {field} {rule}.");
}
