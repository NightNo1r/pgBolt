namespace PgBolt.Server;

/// <summary>Invokes the next filter in the pipeline, or the job executor (§3.6).</summary>
public delegate ValueTask JobExecutionDelegate();

/// <summary>
/// An execution filter resolved from the job's DI scope (register it as scoped, §3.6). It can run code before or after <c>next</c>, catch or
/// replace an exception, or return without calling <c>next</c> to complete the job successfully. Exceptions thrown by a filter are treated as
/// job exceptions (§5.2).
/// </summary>
public interface IJobFilter
{
    ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next);
}
