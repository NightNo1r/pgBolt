using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgBolt.E2E.Contracts;
using PgBolt.E2E.JobsA;   // оба namespace импортированы одновременно:
using PgBolt.E2E.JobsB;   // сгенерированные PgBoltJobRegistry не конфликтуют (нет CS0121/CS0104)
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Jobs;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.E2E.Tests;

public abstract class EndToEndTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private static readonly Guid Server = Guid.NewGuid();
    private readonly List<IHost> _hosts = [];
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, migrate: false);

    public async ValueTask DisposeAsync()
    {
        for (var i = _hosts.Count - 1; i >= 0; i--)
        {
            await _hosts[i].StopAsync();
            _hosts[i].Dispose();
        }
        await _db.DisposeAsync();
    }

    [Fact]
    public async Task Jobs_from_three_executor_assemblies_run_end_to_end()
    {
        // Метки прогона: пробы — статические очереди, общие для _Direct/_Bouncer и других E2E-тестов процесса; без
        // уникальных значений второй прогон проходил бы проверки Contains без исполнения своих задач.
        var run = Guid.NewGuid().ToString("N");
        var (typed, dynamic) = ($"hello-{run}", $"dynamic-{run}");
        var (pong, local) = (Random.Shared.Next(), Random.Shared.Next());
        var services = new ServiceCollection();
        services.AddPgBolt(o =>
        {
            o.ConnectionString = pg.ConnectionString(mode);
            o.Schema = _db.Schema;
            o.AddJobs(PgBolt.E2E.JobsA.PgBoltJobRegistry.Register);
            o.AddJobs(PgBolt.E2E.JobsB.PgBoltJobRegistry.Register);
            o.AddJobs(PgBoltJobRegistry.Register);                  // своя сборка: PgBolt.E2E.Tests.PgBoltJobRegistry
            o.AddJobs(PgBolt.E2E.JobsA.PgBoltJobRegistry.Register); // повтор идемпотентен
        });
        await using var sp = services.BuildServiceProvider();
        await sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct);

        var client = sp.GetRequiredService<IJobClient>();
        var store = sp.GetRequiredService<PgBoltStore>();
        var registry = sp.GetRequiredService<JobRegistry>();
        Assert.Equal(5, registry.All.Count);
        await store.PublishJobTypesAsync(registry.ToJobTypeRecords(), "e2e-server", Ct);

        var ids = new List<JobId>
        {
            (await client.EnqueueAsync(new PingArgs(typed), ct: Ct)).Id!.Value,
            (await client.EnqueueAsync(new PongArgs(pong), ct: Ct)).Id!.Value,
            (await client.EnqueueAsync(new LocalArgs(local), ct: Ct)).Id!.Value,
        };
        using (var dynamicArgs = JsonDocument.Parse($$"""{"Message":"{{dynamic}}"}"""))
            ids.Add((await client.EnqueueAsync("e2e.ping", dynamicArgs.RootElement, ct: Ct)).Id!.Value);

        Assert.DoesNotContain(typed, PingProbe.Messages);
        Assert.DoesNotContain(dynamic, PingProbe.Messages);
        Assert.DoesNotContain(pong, PongProbe.Values);
        Assert.DoesNotContain(local, LocalProbe.Values);

        var processed = 0;
        while (processed < ids.Count)
            processed += await E2EProcessing.ProcessOnceAsync(sp, Server, Ct);

        Assert.Contains(typed, PingProbe.Messages);
        Assert.Contains(dynamic, PingProbe.Messages);
        Assert.Contains(pong, PongProbe.Values);
        Assert.Contains(local, LocalProbe.Values);
        foreach (var id in ids)
            Assert.Equal(JobState.Succeeded, (await client.GetAsync(id, Ct))!.State);
    }

    [Fact]
    public async Task Cluster_limit_holds_across_two_servers()
    {
        var run = Guid.NewGuid().ToString("N");
        var probe = LimitedProbe.For(run);
        var first = await StartServerAsync("e2e-1");
        await StartServerAsync("e2e-2");   // после первого: схему мигрирует первый хост

        await first.Services.GetRequiredService<IJobClient>().EnqueueManyAsync(
            Enumerable.Range(0, 60).Select(n => new LimitedArgs(run, n)).ToList(), ct: Ct);
        await Eventually.True(() => probe.Completed >= 60, "all limited jobs are completed", TimeSpan.FromSeconds(60));

        Assert.Equal(60, probe.Completed);
        Assert.Equal(3, probe.Max);   // не выше лимита и хотя бы раз ровно лимит
    }

    /// <summary>Хост-процесс с сервером на очереди e2e-limited; у каждого сервера слотов больше лимита кластера.</summary>
    private async Task<IHost> StartServerAsync(string name)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Services.AddPgBolt(o =>
        {
            o.ConnectionString = pg.ConnectionString(mode);
            o.Schema = _db.Schema;
            o.AddJobs(PgBoltJobRegistry.Register);
        });
        builder.Services.AddPgBoltServer(name, o =>
        {
            o.Workers = 8;
            o.Queue("e2e-limited", LimitedProbe.Queue);
        });
        var host = builder.Build();
        _hosts.Add(host);
        await host.StartAsync(Ct);
        return host;
    }
}

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class LimitedQueueCollection
{
    /// <summary>Тесты очереди e2e-limited: идут поочерёдно, без чужой нагрузки на тайминги лимита.</summary>
    public const string Name = "e2e-limited";
}

[Collection(LimitedQueueCollection.Name)]
public sealed class EndToEndTests_Direct(PostgresFixture pg) : EndToEndTests(pg, ConnectionMode.Direct);
[Collection(LimitedQueueCollection.Name)]
public sealed class EndToEndTests_Bouncer(PostgresFixture pg) : EndToEndTests(pg, ConnectionMode.Bouncer);
