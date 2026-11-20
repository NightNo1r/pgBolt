namespace PgBolt;

public sealed class UnknownJobException(string jobName)
    : Exception($"Job '{jobName}' is not published in job_types.")
{
    public string JobName { get; } = jobName;
}
