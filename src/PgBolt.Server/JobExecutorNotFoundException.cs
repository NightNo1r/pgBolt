namespace PgBolt.Server;

/// <summary>Для типа задачи нет исполнителя в registry процесса; передаётся в <see cref="RetryPolicy"/> на пути NoExecutor (§3.4, §5.2).</summary>
public sealed class JobExecutorNotFoundException(string jobName)
    : Exception($"No executor is registered for job type '{jobName}'.")
{
    public string JobName { get; } = jobName;
}
