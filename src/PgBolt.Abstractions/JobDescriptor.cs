namespace PgBolt;

/// <summary>Job defaults from [Job], or from job_types when enqueuing by name.</summary>
public sealed record JobDescriptor(string Name, string Queue, int MaxAttempts, short Priority, int[]? RetryDelaysSeconds)
{
    /// <summary>
    /// Execution timeout in seconds from [Job]. Zero disables the timeout. The init-only property preserves the positional constructor.
    /// </summary>
    public int TimeoutSeconds { get; init; }

    /// <summary>Compares descriptors by value, including TimeoutSeconds and the contents of RetryDelaysSeconds.</summary>
    public bool SameAs(JobDescriptor other) =>
        Name == other.Name && Queue == other.Queue && MaxAttempts == other.MaxAttempts && Priority == other.Priority
        && TimeoutSeconds == other.TimeoutSeconds
        && (RetryDelaysSeconds ?? []).AsSpan().SequenceEqual(other.RetryDelaysSeconds ?? [])
        && (RetryDelaysSeconds is null) == (other.RetryDelaysSeconds is null);
}
