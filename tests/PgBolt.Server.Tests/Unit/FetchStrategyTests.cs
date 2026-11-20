using System.Collections.Frozen;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class FetchStrategyTests
{
    private static readonly Guid ServerId = Guid.CreateVersion7();
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(60);
    private static readonly IReadOnlyList<LeasedJob> PlainJobs = [Job()];
    private static readonly IReadOnlyList<LeasedJob> LimitedJobs = [Job(), Job()];

    private readonly FakeServerStore _store = new();
    private QueueSettingsSnapshot _settings = QueueSettingsSnapshot.Empty;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static LeasedJob Job() => new(Guid.CreateVersion7(), Guid.NewGuid(), "t", "{}", 1, 3, null, null, null, null);

    private DequeueFetchStrategy Create(bool limitReached = false)
    {
        _store.OnDequeue = (_, _, _) => Task.FromResult(PlainJobs);
        _store.OnDequeueLimited = (_, _, _) => Task.FromResult(new LimitedDequeueResult(LimitedJobs, limitReached));
        return new DequeueFetchStrategy(_store, ServerId, Lease, () => _settings);
    }

    private static QueueSettingsSnapshot Snapshot(string[] limits, params (string Queue, int Weight)[] weights) =>
        new(limits.ToFrozenDictionary(q => q, _ => 1), weights.ToFrozenDictionary(w => w.Queue, w => w.Weight));

    /// <summary>Вызовы dequeue стора: операция и (queue, max, serverId, lease).</summary>
    private IEnumerable<(string Op, object? Detail)> Dequeues() =>
        _store.Calls.Where(c => c.Op.StartsWith("Dequeue", StringComparison.Ordinal)).Select(c => (c.Op, c.Detail));

    [Fact]
    public async Task Queue_outside_Limits_uses_plain_dequeue()
    {
        var result = await Create(limitReached: true).FetchAsync("q", 3, Ct);

        Assert.Equal([("Dequeue", (object?)("q", 3, ServerId, Lease))], Dequeues());
        Assert.Same(PlainJobs, result.Jobs);
        Assert.False(result.LimitReached);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Queue_in_Limits_uses_limited_dequeue_and_returns_its_result(bool limitReached)
    {
        _settings = Snapshot(["q"]);

        var result = await Create(limitReached).FetchAsync("q", 3, Ct);

        Assert.Equal([("DequeueLimited", (object?)("q", 3, ServerId, Lease))], Dequeues());
        Assert.Same(LimitedJobs, result.Jobs);
        Assert.Equal(limitReached, result.LimitReached);
    }

    [Fact]
    public async Task Weighted_queue_without_limit_uses_plain_dequeue()
    {
        _settings = Snapshot([], ("q", 5));

        var result = await Create().FetchAsync("q", 3, Ct);

        Assert.Equal([("Dequeue", (object?)("q", 3, ServerId, Lease))], Dequeues());
        Assert.Same(PlainJobs, result.Jobs);
    }

    [Fact]
    public async Task Snapshot_swap_between_calls_changes_the_variant()
    {
        var strategy = Create();
        await strategy.FetchAsync("q", 3, Ct);

        _settings = Snapshot(["q"]);
        await strategy.FetchAsync("q", 3, Ct);

        Assert.Equal(["Dequeue", "DequeueLimited"], Dequeues().Select(d => d.Op));
    }
}
