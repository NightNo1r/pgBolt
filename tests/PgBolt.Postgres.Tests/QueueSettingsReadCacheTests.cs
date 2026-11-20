using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Client;

namespace PgBolt.Postgres.Tests;

public sealed class QueueSettingsReadCacheTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(10);

    private readonly FakeTimeProvider _time = new();

    private static IReadOnlyList<QueueSettings> Snapshot(string queue) =>
        [new QueueSettings(queue, null, null, null, false, null, null, null, false, null, null, null)];

    /// <summary>
    /// Каждый вызов загрузки — свой TaskCompletionSource: тест завершает его сам, а при <see cref="Sync"/> он сразу завершён
    /// снимком «load{номер вызова}».
    /// </summary>
    private sealed class Loader
    {
        private readonly List<TaskCompletionSource<IReadOnlyList<QueueSettings>>> _loads = [];
        public bool Sync;

        public Task<IReadOnlyList<QueueSettings>> Load(CancellationToken _)
        {
            var tcs = new TaskCompletionSource<IReadOnlyList<QueueSettings>>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (Sync)
                tcs.SetResult(Snapshot($"load{_loads.Count}"));
            _loads.Add(tcs);
            return tcs.Task;
        }

        public int Count => _loads.Count;
        public TaskCompletionSource<IReadOnlyList<QueueSettings>> this[int i] => _loads[i];
    }

    [Fact]
    public async Task Without_duration_always_loads()
    {
        var cache = new QueueSettingsReadCache(null, _time);
        var loader = new Loader { Sync = true };

        Assert.False(cache.Enabled);
        Assert.Equal("load0", (await cache.GetAsync(loader.Load, Ct))[0].Queue);
        Assert.Equal("load1", (await cache.GetAsync(loader.Load, Ct))[0].Queue);
    }

    [Fact]
    public async Task Serves_snapshot_until_ttl()
    {
        var cache = new QueueSettingsReadCache(Ttl, _time);
        var loader = new Loader { Sync = true };

        Assert.True(cache.Enabled);
        Assert.Equal("load0", (await cache.GetAsync(loader.Load, Ct))[0].Queue);
        _time.Advance(Ttl - TimeSpan.FromTicks(1));
        Assert.Equal("load0", (await cache.GetAsync(loader.Load, Ct))[0].Queue);
        Assert.Equal(1, loader.Count);

        _time.Advance(TimeSpan.FromTicks(1));
        Assert.Equal("load1", (await cache.GetAsync(loader.Load, Ct))[0].Queue);
        Assert.Equal(2, loader.Count);
    }

    [Fact]
    public async Task Concurrent_misses_share_one_load()
    {
        var cache = new QueueSettingsReadCache(Ttl, _time);
        var loader = new Loader();

        var a = cache.GetAsync(loader.Load, Ct).AsTask();
        var b = cache.GetAsync(loader.Load, Ct).AsTask();
        Assert.Equal(1, loader.Count);
        var snapshot = Snapshot("x");
        loader[0].SetResult(snapshot);

        Assert.Same(snapshot, await a);
        Assert.Same(snapshot, await b);
        Assert.Equal(1, loader.Count);
    }

    [Fact]
    public async Task Error_is_not_cached()
    {
        var cache = new QueueSettingsReadCache(Ttl, _time);
        var loader = new Loader();

        var failed = cache.GetAsync(loader.Load, Ct).AsTask();
        Assert.Equal(1, loader.Count);
        loader[0].SetException(new InvalidOperationException("boom"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => failed);

        loader.Sync = true;
        Assert.Equal("load1", (await cache.GetAsync(loader.Load, Ct))[0].Queue);
    }

    [Fact]
    public async Task Load_started_before_invalidate_is_not_cached()
    {
        var cache = new QueueSettingsReadCache(Ttl, _time);
        var loader = new Loader();

        var a = cache.GetAsync(loader.Load, Ct).AsTask();
        Assert.Equal(1, loader.Count);
        cache.Invalidate();
        loader[0].SetResult(Snapshot("stale"));
        Assert.Equal("stale", (await a)[0].Queue);

        loader.Sync = true;
        Assert.Equal("load1", (await cache.GetAsync(loader.Load, Ct))[0].Queue);
    }

    [Fact]
    public async Task Caller_after_invalidate_does_not_wait_old_load()
    {
        var cache = new QueueSettingsReadCache(Ttl, _time);
        var loader = new Loader();

        var a = cache.GetAsync(loader.Load, Ct).AsTask();
        Assert.Equal(1, loader.Count);
        cache.Invalidate();
        var b = cache.GetAsync(loader.Load, Ct).AsTask();
        Assert.Equal(2, loader.Count);

        loader[1].SetResult(Snapshot("fresh"));
        Assert.Equal("fresh", (await b)[0].Queue);
        Assert.False(a.IsCompleted);

        // Старая загрузка, завершившись позже, не затирает снимок новой.
        loader[0].SetResult(Snapshot("stale"));
        await a;
        Assert.Equal("fresh", (await cache.GetAsync(loader.Load, Ct))[0].Queue);
        Assert.Equal(2, loader.Count);
    }

    [Fact]
    public async Task Cancelling_one_waiter_keeps_shared_load()
    {
        var cache = new QueueSettingsReadCache(Ttl, _time);
        var loader = new Loader();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);

        var cancelled = cache.GetAsync(loader.Load, cts.Token).AsTask();
        var kept = cache.GetAsync(loader.Load, Ct).AsTask();
        Assert.Equal(1, loader.Count);
        await cts.CancelAsync();
        // Ограничение времени: если отмена не прерывает ожидание, тест падает с TimeoutException, а не зависает.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelled.WaitAsync(TimeSpan.FromSeconds(10), Ct));

        loader[0].SetResult(Snapshot("x"));
        Assert.Equal("x", (await kept)[0].Queue);
        Assert.Equal(1, loader.Count);
    }
}
