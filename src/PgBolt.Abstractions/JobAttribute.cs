namespace PgBolt;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class JobAttribute(string name) : Attribute
{
    public string Name { get; } = name;
    public string Queue { get; init; } = "default";
    public int MaxAttempts { get; init; } = 10;
    public short Priority { get; init; }
    public int[]? RetryDelaysSeconds { get; init; }

    /// <summary>
    /// Timeout for one execution, in seconds. The default, 0, disables the timeout. The executing server reads its local registry. The timeout is
    /// omitted from job_types. Negative values cause generator error BOLT007. Values above 4294967 seconds produce warning BOLT008 and are capped by
    /// the server at the .NET timer limit (2³² − 2 ms, about 49.7 days).
    /// </summary>
    public int TimeoutSeconds { get; init; }
}
