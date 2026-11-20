using Microsoft.Extensions.DependencyInjection;
using PgBolt.Postgres.Client;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class QueueSettingsManagerTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    // Источник данных принадлежит провайдеру (owned): с o.DataSource его Dispose ничего не закрывает.
    private ServiceProvider Build(Action<IServiceCollection>? extra = null, TimeSpan? cacheDuration = null)
    {
        var services = new ServiceCollection();
        services.AddPgBolt(o =>
        {
            o.ConnectionString = Db.ConnectionString;
            o.Schema = Db.Schema;
            o.QueueSettingsCacheDuration = cacheDuration;
        });
        extra?.Invoke(services);
        return services.BuildServiceProvider();
    }

    private Task SeedDefaultAsync(string queue, int? limit, int? weight) =>
        Db.ExecAsync("""
            INSERT INTO {schema}.queue_settings (queue, default_max_concurrency, default_weight, default_published_by, default_updated_at)
            VALUES (@q, @l, @w, 'code/1', now())
            """, ("q", queue), ("l", limit), ("w", weight));

    [Fact]
    public async Task Concurrency_override_beats_default_and_resets()
    {
        await using var sp = Build();
        var manager = sp.GetRequiredService<IQueueSettingsManager>();
        await SeedDefaultAsync("a", 5, null);

        await manager.SetConcurrencyOverrideAsync("a", 2, "op", Ct);
        var set = (await manager.GetAsync("a", Ct))!;
        Assert.Equal((2, "op", true), (set.EffectiveMaxConcurrency, set.OverrideConcurrencyUpdatedBy, set.OverrideConcurrencyUpdatedAt is not null));

        await manager.SetConcurrencyOverrideAsync("a", null, "op", Ct);
        var unlimited = (await manager.GetAsync("a", Ct))!;
        Assert.Equal((null, true), (unlimited.EffectiveMaxConcurrency, unlimited.OverrideConcurrencySet));

        await manager.SetConcurrencyOverrideAsync("a", 0, "op", Ct);
        Assert.True((await manager.GetAsync("a", Ct))!.Paused);

        await manager.ResetConcurrencyOverrideAsync("a", Ct);
        var reset = (await manager.GetAsync("a", Ct))!;
        Assert.Equal((5, false, null, null), (reset.EffectiveMaxConcurrency, reset.OverrideConcurrencySet,
            reset.OverrideConcurrencyUpdatedBy, reset.OverrideConcurrencyUpdatedAt));
    }

    [Fact]
    public async Task Weight_override_is_independent()
    {
        await using var sp = Build();
        var manager = sp.GetRequiredService<IQueueSettingsManager>();
        await SeedDefaultAsync("a", 5, 3);

        await manager.SetWeightOverrideAsync("a", 7, "op", Ct);
        var weighted = (await manager.GetAsync("a", Ct))!;
        Assert.Equal((7, 5, false), (weighted.EffectiveWeight, weighted.EffectiveMaxConcurrency, weighted.OverrideConcurrencySet));

        await manager.SetConcurrencyOverrideAsync("a", 2, "op", Ct);
        await manager.ResetConcurrencyOverrideAsync("a", Ct);
        var kept = (await manager.GetAsync("a", Ct))!;
        Assert.Equal((7, true), (kept.EffectiveWeight, kept.OverrideWeightSet));

        await manager.SetWeightOverrideAsync("a", null, "op", Ct);
        var unweighted = (await manager.GetAsync("a", Ct))!;
        Assert.Equal((null, true), (unweighted.EffectiveWeight, unweighted.OverrideWeightSet));

        await manager.ResetWeightOverrideAsync("a", Ct);
        var reset = (await manager.GetAsync("a", Ct))!;
        Assert.Equal((3, false, null, null), (reset.EffectiveWeight, reset.OverrideWeightSet, reset.OverrideWeightUpdatedBy, reset.OverrideWeightUpdatedAt));
    }

    [Fact]
    public async Task Override_creates_row_without_defaults()
    {
        await using var sp = Build();
        var manager = sp.GetRequiredService<IQueueSettingsManager>();

        await manager.SetConcurrencyOverrideAsync("a", 1, null, Ct);

        var s = (await manager.GetAsync("a", Ct))!;
        Assert.Equal((null, null, null), (s.DefaultMaxConcurrency, s.DefaultWeight, s.DefaultPublishedBy));
        Assert.Equal(["a"], (await manager.ListAsync(Ct)).Select(x => x.Queue));
    }

    [Fact]
    public async Task Reset_without_row_is_noop()
    {
        await using var sp = Build();
        var manager = sp.GetRequiredService<IQueueSettingsManager>();

        await manager.ResetConcurrencyOverrideAsync("a", Ct);
        await manager.ResetWeightOverrideAsync("a", Ct);

        Assert.Equal(0L, await CountAsync("queue_settings"));
        Assert.Null(await manager.GetAsync("a", Ct));
    }

    [Fact]
    public async Task Arguments_are_validated()
    {
        await using var sp = Build();
        var manager = sp.GetRequiredService<IQueueSettingsManager>();

        foreach (var queue in new string?[] { null, "", " " })
        {
            await Assert.ThrowsAnyAsync<ArgumentException>(async () => await manager.SetConcurrencyOverrideAsync(queue!, 1, null, Ct));
            await Assert.ThrowsAnyAsync<ArgumentException>(async () => await manager.ResetConcurrencyOverrideAsync(queue!, Ct));
            await Assert.ThrowsAnyAsync<ArgumentException>(async () => await manager.SetWeightOverrideAsync(queue!, 1, null, Ct));
            await Assert.ThrowsAnyAsync<ArgumentException>(async () => await manager.ResetWeightOverrideAsync(queue!, Ct));
            await Assert.ThrowsAnyAsync<ArgumentException>(async () => await manager.GetAsync(queue!, Ct));
        }
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await manager.SetConcurrencyOverrideAsync("a", -1, null, Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await manager.SetWeightOverrideAsync("a", 0, null, Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await manager.SetWeightOverrideAsync("a", 1001, null, Ct));

        Assert.Equal(0L, await CountAsync("queue_settings"));
    }

    public static TheoryData<int> WriteMethods => [0, 1, 2, 3];

    private static ValueTask Write(IQueueSettingsManager m, int method) => method switch
    {
        0 => m.SetConcurrencyOverrideAsync("a", 2, null, Ct),
        1 => m.ResetConcurrencyOverrideAsync("a", Ct),
        2 => m.SetWeightOverrideAsync("a", 2, null, Ct),
        _ => m.ResetWeightOverrideAsync("a", Ct),
    };

    [Theory]
    [MemberData(nameof(WriteMethods))]
    public async Task Write_calls_refresher_even_on_failure(int method)
    {
        var refresher = new CountingRefresher();
        await using var sp = Build(s => s.AddSingleton<IQueueSettingsRefresher>(refresher));
        var manager = sp.GetRequiredService<IQueueSettingsManager>();

        await Write(manager, method);
        Assert.Equal(1, refresher.Calls);

        // Источник принадлежит провайдеру: после Dispose открытие соединения бросает, а сам провайдер жив.
        await sp.GetRequiredService<PgBoltDataSource>().DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await Write(manager, method));
        Assert.Equal(2, refresher.Calls);
    }

    [Fact]
    public async Task Write_without_refresher_succeeds()
    {
        await using var sp = Build();

        await sp.GetRequiredService<IQueueSettingsManager>().SetWeightOverrideAsync("a", 2, null, Ct);
    }

    private Task SetDefaultLimitAsync(string queue, int limit) =>
        Db.ExecAsync("UPDATE {schema}.queue_settings SET default_max_concurrency = @l WHERE queue = @q", ("q", queue), ("l", limit));

    [Fact]
    public async Task Get_without_cache_reads_db()
    {
        await using var sp = Build();
        var manager = sp.GetRequiredService<IQueueSettingsManager>();
        await SeedDefaultAsync("a", 5, null);
        Assert.Equal(5, (await manager.GetAsync("a", Ct))!.DefaultMaxConcurrency);

        await SetDefaultLimitAsync("a", 6);

        Assert.Equal(6, (await manager.GetAsync("a", Ct))!.DefaultMaxConcurrency);
    }

    [Theory]
    [MemberData(nameof(WriteMethods))]
    public async Task Get_reads_from_list_snapshot_and_write_invalidates(int method)
    {
        await using var sp = Build(cacheDuration: TimeSpan.FromHours(1));
        var manager = sp.GetRequiredService<IQueueSettingsManager>();
        await SeedDefaultAsync("a", 5, null);
        await manager.ListAsync(Ct);

        await SetDefaultLimitAsync("a", 6);
        Assert.Equal(5, (await manager.GetAsync("a", Ct))!.DefaultMaxConcurrency);
        Assert.Null(await manager.GetAsync("b", Ct));

        await Write(manager, method);
        Assert.Equal(6, (await manager.GetAsync("a", Ct))!.DefaultMaxConcurrency);
    }

    [Theory]
    [MemberData(nameof(WriteMethods))]
    public async Task Failed_write_invalidates_cache(int method)
    {
        await using var sp = Build(cacheDuration: TimeSpan.FromHours(1));
        var manager = sp.GetRequiredService<IQueueSettingsManager>();
        await SeedDefaultAsync("a", 5, null);
        await manager.ListAsync(Ct);

        await sp.GetRequiredService<PgBoltDataSource>().DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await Write(manager, method));

        // Снимок сброшен, а новая загрузка не удаётся: старый снимок не возвращается.
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await manager.ListAsync(Ct));
    }

    [Fact]
    public async Task InvalidateCache_drops_snapshot()
    {
        await using var sp = Build(cacheDuration: TimeSpan.FromHours(1));
        var manager = sp.GetRequiredService<IQueueSettingsManager>();
        await SeedDefaultAsync("a", 5, null);
        await manager.ListAsync(Ct);
        await SetDefaultLimitAsync("a", 6);

        manager.InvalidateCache();

        Assert.Equal(6, (await manager.ListAsync(Ct))[0].DefaultMaxConcurrency);
    }

    private sealed class CountingRefresher : IQueueSettingsRefresher
    {
        public int Calls;
        public void RefreshNow() => Interlocked.Increment(ref Calls);
    }
}

public sealed class QueueSettingsManagerTests_Direct(PostgresFixture pg) : QueueSettingsManagerTests(pg, ConnectionMode.Direct);
public sealed class QueueSettingsManagerTests_Bouncer(PostgresFixture pg) : QueueSettingsManagerTests(pg, ConnectionMode.Bouncer);
