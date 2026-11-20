namespace PgBolt;

public interface IJob<in TArgs>
{
    Task ExecuteAsync(TArgs args, JobContext context, CancellationToken cancellationToken);
}
