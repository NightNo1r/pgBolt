using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using PgBolt.E2E.Contracts;

[assembly: PgBolt.PgBoltJsonContext(typeof(PgBolt.E2E.JobsA.JobsAJson))]

namespace PgBolt.E2E.JobsA;

public static class PingProbe
{
    public static ConcurrentQueue<string> Messages { get; } = new();
}

public sealed class PingJob : IJob<PingArgs>
{
    public Task ExecuteAsync(PingArgs args, JobContext context, CancellationToken cancellationToken)
    {
        PingProbe.Messages.Enqueue(args.Message);
        return Task.CompletedTask;
    }
}

[JsonSerializable(typeof(PingArgs))]
internal sealed partial class JobsAJson : JsonSerializerContext;
