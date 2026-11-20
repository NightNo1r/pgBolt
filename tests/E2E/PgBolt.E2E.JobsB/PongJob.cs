using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using PgBolt.E2E.Contracts;

[assembly: PgBolt.PgBoltJsonContext(typeof(PgBolt.E2E.JobsB.JobsBJson))]

namespace PgBolt.E2E.JobsB;

public static class PongProbe
{
    public static ConcurrentQueue<int> Values { get; } = new();
}

public sealed class PongJob : IJob<PongArgs>
{
    public Task ExecuteAsync(PongArgs args, JobContext context, CancellationToken cancellationToken)
    {
        PongProbe.Values.Enqueue(args.N);
        return Task.CompletedTask;
    }
}

[JsonSerializable(typeof(PongArgs))]
internal sealed partial class JobsBJson : JsonSerializerContext;
