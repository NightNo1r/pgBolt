using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Server.Runtime;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>Веса очередей на работающем сервере: доли общего пула по весам и возврат к строгому порядку.</summary>
public abstract class QueueWeightsTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    private async Task<List<JobId>> BacklogAsync(string queue, int count)
    {
        var results = await Client.EnqueueManyAsync(Enumerable.Range(0, count).Select(_ => new SlowArgs(50)).ToList(),
            new EnqueueOptions { Queue = queue }, null, Ct);
        return [.. results.Select(r => r.Id!.Value)];
    }

    /// <summary>Задачи из ids, завершённые после момента since (Stopwatch.GetTimestamp()).</summary>
    private int CompletedSince(IEnumerable<JobId> ids, long since) =>
        ids.SelectMany(Probe.Runs).Count(r => r.End == RunEnd.Completed && r.FinishedAt >= since);

    [Fact]
    public async Task Shares_follow_weights_and_reset_restores_strict_order()
    {
        var a = await BacklogAsync("a", 600);
        var b = await BacklogAsync("b", 300);
        var host = await StartHostAsync(s => s.AddServer("s1", o =>
        {
            o.Workers = 4;
            o.Queue("a", ServerTestHost.FastQueue).Queue("b", ServerTestHost.FastQueue);
        }));
        var manager = host.Services.GetRequiredService<IQueueSettingsManager>();
        var settings = host.Services.GetRequiredService<QueueSettingsCache>();

        await manager.SetWeightOverrideAsync("a", 3, ct: Ct);
        await manager.SetWeightOverrideAsync("b", 1, ct: Ct);
        await WaitUntilAsync(() => settings.Current.Weights is { Count: 2 } w && w.GetValueOrDefault("a") == 3 && w.GetValueOrDefault("b") == 1,
            TimeSpan.FromSeconds(5), "the weights reach the settings snapshot of the process");
        var since = Stopwatch.GetTimestamp();
        await Task.Delay(TimeSpan.FromSeconds(3), Ct);
        var (doneA, doneB) = (CompletedSince(a, since), CompletedSince(b, since));

        var share = (double)doneA / (doneA + doneB);
        Assert.True(doneB > 0 && Math.Abs(share - 0.75) <= 0.1, $"a completed {doneA}, b completed {doneB}: share of a {share:F2}");

        await manager.ResetWeightOverrideAsync("a", Ct);
        await manager.ResetWeightOverrideAsync("b", Ct);
        await WaitUntilAsync(() => settings.Current.Weights.Count == 0, TimeSpan.FromSeconds(5),
            "the reset reaches the settings snapshot of the process");
        // Задачи b, начатые до применения снимка, ещё могут завершиться; дальше b почти не выбирается, пока у a есть backlog.
        await Task.Delay(TimeSpan.FromMilliseconds(300), Ct);
        since = Stopwatch.GetTimestamp();
        await Task.Delay(TimeSpan.FromSeconds(1), Ct);
        (doneA, doneB) = (CompletedSince(a, since), CompletedSince(b, since));

        Assert.True(doneA > 0 && doneB * 10 <= doneA, $"strict order: a completed {doneA}, b completed {doneB}");
    }
}

[Collection(TimingSensitiveCollection.Name)]
public sealed class QueueWeightsTests_Direct(PostgresFixture pg) : QueueWeightsTests(pg, ConnectionMode.Direct);

[Collection(TimingSensitiveCollection.Name)]
public sealed class QueueWeightsTests_Bouncer(PostgresFixture pg) : QueueWeightsTests(pg, ConnectionMode.Bouncer);
