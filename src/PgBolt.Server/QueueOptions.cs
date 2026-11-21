using PgBolt.Postgres.Client;

namespace PgBolt.Server;

/// <summary>A virtual server's queue (§3.3). Created through <see cref="PgBoltServerOptions.Queue"/>.</summary>
public sealed class QueueOptions
{
    internal QueueOptions(string name) => Name = name;

    public string Name { get; }

    /// <summary>Number of slots reserved exclusively for this queue.</summary>
    public int Reserved { get; set; }

    /// <summary>Maximum concurrent jobs for this queue on this server across reserved and shared slots. Null means unlimited.</summary>
    public int? MaxConcurrency { get; set; }

    /// <summary>
    /// Default cluster-wide limit on jobs that may hold a lease in this queue simultaneously. Null means unlimited. The process publishes this value
    /// at startup. An IQueueSettingsManager override takes precedence. If another process introduces a limit after startup, this process picks it up
    /// within PgBoltProcessingOptions.QueueSettingsRefreshInterval. Until then it may dequeue jobs without enforcing the limit.
    /// </summary>
    public int? ClusterMaxConcurrency { get; set; }

    /// <summary>
    /// Default cluster-wide queue weight (1 through 1000). Each server divides its shared slots among weighted queues in proportion to their
    /// weights. Null means no weight. An IQueueSettingsManager override takes precedence.
    /// </summary>
    public int? Weight { get; set; }

    /// <summary>Minimum adaptive polling interval.</summary>
    public TimeSpan MinPollInterval { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Maximum adaptive polling interval.</summary>
    public TimeSpan MaxPollInterval { get; set; } = TimeSpan.FromSeconds(10);

    internal void Validate(string serverName)
    {
        var field = $"Queue('{Name}')";
        if (Reserved < 0)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(Reserved)}", "must be >= 0");
        if (MaxConcurrency is int max && max < Math.Max(1, Reserved))
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(MaxConcurrency)}", "must be null or >= max(1, Reserved)");
        if (ClusterMaxConcurrency is < 1)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(ClusterMaxConcurrency)}", "must be null or >= 1");
        if (Weight is < 1 or > QueueSettingsManager.MaxWeight)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(Weight)}", $"must be null or in [1, {QueueSettingsManager.MaxWeight}]");
        if (MinPollInterval <= TimeSpan.Zero)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(MinPollInterval)}", "must be > 0");
        if (MaxPollInterval < MinPollInterval)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(MaxPollInterval)}", "must be >= MinPollInterval");
        if (MaxPollInterval > PgBoltServerOptions.MaxInterval)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(MaxPollInterval)}", $"must be <= {PgBoltServerOptions.MaxInterval}");
    }
}
