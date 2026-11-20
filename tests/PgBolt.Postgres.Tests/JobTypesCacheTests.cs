using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Только Direct: проверяется логика TTL-кэша; SQL job_types покрыт в обоих режимах в JobTypesTests.</summary>
public sealed class JobTypesCacheTests(PostgresFixture pg) : StoreTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Caches_for_ttl_then_refreshes()
    {
        var time = new FakeTimeProvider();
        var cache = new JobTypesCache(Store, TimeSpan.FromSeconds(30), time);
        await Store.PublishJobTypesAsync([new JobTypeRecord("x.job", "{}", "q1", 3, 0, null)], "s", Ct);

        Assert.Equal("q1", (await cache.GetAsync("x.job", Ct))!.DefaultQueue);
        await Store.PublishJobTypesAsync([new JobTypeRecord("x.job", "{}", "q2", 3, 0, null)], "s", Ct);
        Assert.Equal("q1", (await cache.GetAsync("x.job", Ct))!.DefaultQueue);

        time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal("q2", (await cache.GetAsync("x.job", Ct))!.DefaultQueue);
    }

    [Fact]
    public async Task Missing_type_is_null()
    {
        var cache = new JobTypesCache(Store, TimeSpan.FromSeconds(30), TimeProvider.System);
        Assert.Null(await cache.GetAsync("nope", Ct));
    }
}
