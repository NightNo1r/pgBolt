namespace PgBolt.Server;

/// <summary>Job execution context available to filters (§3.6).</summary>
public sealed class JobExecutingContext
{
    internal JobExecutingContext(JobContext job, IServiceProvider services, ReadOnlyMemory<byte> argsUtf8, CancellationToken cancellationToken)
    {
        Job = job;
        Services = services;
        ArgsUtf8 = argsUtf8;
        CancellationToken = cancellationToken;
    }

    public JobContext Job { get; }

    /// <summary>Service provider for the job's DI scope.</summary>
    public IServiceProvider Services { get; }

    /// <summary>UTF-8 job arguments. The invoker receives the same buffer.</summary>
    public ReadOnlyMemory<byte> ArgsUtf8 { get; }

    /// <summary>Execution token cancelled on shutdown, job cancellation, timeout, or loss of the lease.</summary>
    public CancellationToken CancellationToken { get; }
}
