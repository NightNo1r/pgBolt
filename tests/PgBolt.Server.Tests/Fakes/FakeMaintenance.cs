using System.Collections.Concurrent;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Schema;

namespace PgBolt.Server.Tests.Fakes;

/// <summary>Вызов FakeMaintenance: операция и её аргумент (daysAhead, olderThan или null).</summary>
internal sealed record MaintenanceCall(string Op, object? Detail);

/// <summary>Программируемый IPgBoltMaintenance; вызовы пишутся в Calls до вызова делегата.</summary>
internal sealed class FakeMaintenance : IPgBoltMaintenance
{
    public static RetentionResult Empty { get; } = new([], [], 0, [], [], 0);

    public ConcurrentQueue<MaintenanceCall> Calls { get; } = new();

    public Func<CancellationToken, Task<PartitionEnsureResult>> OnEnsure { get; set; } =
        _ => Task.FromResult(new PartitionEnsureResult(0, TimedOut: false, TodayPresent: true));

    public Func<CancellationToken, Task<RetentionResult>> OnRetention { get; set; } = _ => Task.FromResult(Empty);

    public Func<TimeSpan, CancellationToken, Task<int>> OnRemoveDeadServers { get; set; } = (_, _) => Task.FromResult(0);

    public IEnumerable<string> Ops => Calls.Select(c => c.Op);

    public Task<PartitionEnsureResult> EnsurePartitionsAsync(int? daysAhead = null, CancellationToken ct = default)
    {
        Calls.Enqueue(new MaintenanceCall("Ensure", daysAhead));
        return OnEnsure(ct);
    }

    public Task<RetentionResult> ApplyRetentionAsync(CancellationToken ct = default)
    {
        Calls.Enqueue(new MaintenanceCall("Retention", null));
        return OnRetention(ct);
    }

    public Task<int> RemoveDeadServersAsync(TimeSpan olderThan, CancellationToken ct = default)
    {
        Calls.Enqueue(new MaintenanceCall("RemoveDeadServers", olderThan));
        return OnRemoveDeadServers(olderThan, ct);
    }
}
