using Microsoft.Extensions.DependencyInjection;
using PgBolt.Postgres.Client;
using PgBolt.Server.Runtime;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>Публикация значений очередей из кода при старте процесса и регистрация кэша настроек.</summary>
public abstract class QueueSettingsStartupTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    [Fact]
    public async Task Start_publishes_queue_defaults_once_per_queue()
    {
        static void Limited(QueueOptions q)
        {
            q.ClusterMaxConcurrency = 3;
            q.Weight = 2;
        }

        var host = await StartHostAsync(s => s
            .AddServer("s1", o => o.Queue("a", Limited))
            .AddServer("s2", o => o.Queue("a", Limited)));

        Assert.Equal(1, await CountAsync("SELECT count(*) FROM {schema}.queue_settings"));
        Assert.Equal("a/3/2", await Db.ScalarAsync<string>(
            "SELECT format('%s/%s/%s', queue, default_max_concurrency, default_weight) FROM {schema}.queue_settings"));
        var cache = host.Services.GetRequiredService<QueueSettingsCache>();
        Assert.Same(cache, host.Services.GetService<IQueueSettingsRefresher>());
        Assert.Same(cache, host.Processing.QueueSettings);
    }
}

public sealed class QueueSettingsStartupTests_Direct(PostgresFixture pg) : QueueSettingsStartupTests(pg, ConnectionMode.Direct);
public sealed class QueueSettingsStartupTests_Bouncer(PostgresFixture pg) : QueueSettingsStartupTests(pg, ConnectionMode.Bouncer);
