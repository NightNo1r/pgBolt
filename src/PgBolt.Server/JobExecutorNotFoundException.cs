namespace PgBolt.Server;

/// <summary>
/// No executor for this job type exists in the process registry. Passed to <see cref="RetryPolicy"/> on the NoExecutor path (§3.4, §5.2).
/// </summary>
public sealed class JobExecutorNotFoundException(string jobName)
    : Exception($"No executor is registered for job type '{jobName}'.")
{
    public string JobName { get; } = jobName;
}
