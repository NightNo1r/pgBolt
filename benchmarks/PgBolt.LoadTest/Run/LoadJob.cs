using PgBolt.LoadTest.Load;

namespace PgBolt.LoadTest.Run;

/// <summary>Load job that counts executions, records first-attempt start latency, optionally fails the first attempt, and performs
/// work.</summary>
internal sealed class LoadJob(RunContext ctx) : IJob<LoadArgs>
{
    public static readonly JobDescriptor Descriptor = new("load.job", "default", MaxAttempts: 10, Priority: 0, null);

    public static void Register(IPgBoltJobsBuilder jobs) =>
        jobs.AddExecutor<LoadArgs, LoadJob>(Descriptor, LoadJson.Default.LoadArgs);

    public async Task ExecuteAsync(LoadArgs args, JobContext context, CancellationToken cancellationToken)
    {
        // Первым шагом: падение ниже не должно потерять учёт выполнения.
        ctx.CountExecution(args.Seq);
        if (context.Attempt == 1)
        {
            // Задержка повтора задаётся политикой повтора, поэтому пишется только первая попытка.
            var now = RunClock.NowMicros;
            if (args.RunAtMicros == args.EnqueuedAtMicros) ctx.Immediate.Record(now - args.EnqueuedAtMicros);
            else ctx.Delayed.Record(now - args.RunAtMicros);

            if (args.FailFirstAttempt)
                throw new InvalidOperationException($"Job {args.Seq} fails on its first attempt.");
        }

        await Task.Delay(args.WorkMillis, cancellationToken);
    }
}
