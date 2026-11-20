using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Text.Json.Serialization;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Tests;

public sealed record ClientArgs(int Order);
public sealed record UnregisteredArgs(int X);

[JsonSerializable(typeof(ClientArgs))]
internal sealed partial class ClientJson : JsonSerializerContext;

internal static class ClientMetadata
{
    public static readonly JobDescriptor Descriptor = new("client.test", "client", 4, 2, [1, 5]);

    /// <summary>Имитирует сгенерированный module initializer (Task 26).</summary>
    public static void EnsureRegistered() => JobMetadataRegistry.Register(Descriptor, ClientJson.Default.ClientArgs);
}

/// <summary>Клиент и менеджер расписаний для тестов, создающих их напрямую, с собственным инструментированием.</summary>
internal static class ClientTelemetry
{
    private static PgBoltInstrumentation Instrumentation() => new(null, TimeProvider.System, null);

    public static PgBoltJobClient Client(PgBoltStore store, IWakeupSignal wakeup)
    {
        var instrumentation = Instrumentation();
        return new(store, new JobTypesCache(store, TimeSpan.FromSeconds(30), TimeProvider.System), wakeup, instrumentation,
            new PgBoltClientMetrics(instrumentation));
    }

    public static RecurringJobManager Manager(PgBoltStore store, IWakeupSignal wakeup)
    {
        var instrumentation = Instrumentation();
        return new(store, new JobTypesCache(store, TimeSpan.FromSeconds(30), TimeProvider.System), wakeup, instrumentation,
            new PgBoltClientMetrics(instrumentation));
    }
}

public sealed class RecordingWakeup : IWakeupSignal
{
    public ConcurrentQueue<string> Signals { get; } = new();
    public void Signal(string queue) => Signals.Enqueue(queue);
    public IDisposable Subscribe(string queue, Action callback) => NoopSubscription.Instance;

    private sealed class NoopSubscription : IDisposable
    {
        public static readonly NoopSubscription Instance = new();
        public void Dispose() { }
    }
}

public sealed class FakeTransaction : DbTransaction
{
    public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
    protected override DbConnection? DbConnection => null;
    public override void Commit() { }
    public override void Rollback() { }
}
