using Microsoft.Extensions.DependencyInjection;
using PgBolt.E2E.Contracts;
using PgBolt.E2E.JobsA;
using PgBolt.E2E.JobsB;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Jobs;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.E2E.Tests;

/// <summary>Часть 5 E2E: типизированные шаги workflow (Enqueue / ContinueWith / EnqueueMany) с метаданными генератора.</summary>
public abstract class WorkflowEndToEndTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private static readonly Guid Server = Guid.NewGuid();
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, migrate: false);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Typed_workflow_runs_end_to_end_with_generated_metadata()
    {
        var run = Guid.NewGuid().ToString("N");
        var (ping, pong) = ($"flow-{run}", Random.Shared.Next());
        int[] locals = [Random.Shared.Next(), Random.Shared.Next()];
        var services = new ServiceCollection();
        services.AddPgBolt(o =>
        {
            o.ConnectionString = pg.ConnectionString(mode);
            o.Schema = _db.Schema;
            o.AddJobs(PgBolt.E2E.JobsA.PgBoltJobRegistry.Register);
            o.AddJobs(PgBolt.E2E.JobsB.PgBoltJobRegistry.Register);
            o.AddJobs(PgBoltJobRegistry.Register);
        });
        await using var sp = services.BuildServiceProvider();
        await sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct);
        var client = sp.GetRequiredService<IJobClient>();
        var store = sp.GetRequiredService<PgBoltStore>();
        var registry = sp.GetRequiredService<JobRegistry>();
        await store.PublishJobTypesAsync(registry.ToJobTypeRecords(), "e2e-server", Ct);

        var first = client.Workflow().Enqueue(new PingArgs(ping));
        var second = first.ContinueWith(new PongArgs(pong));
        IReadOnlyList<IJobStep> members = [];
        var fan = second.ContinueWithBatch(b => members = b.EnqueueMany(locals.Select(n => new LocalArgs(n)).ToArray()));
        var result = await fan.SubmitAsync(Ct);

        var processed = 0;
        var rounds = new List<Guid[]>();
        for (var round = 0; round < 50 && processed < result.Jobs.Count; round++)
        {
            var leased = await store.DequeueAsync("e2e", 10, Server, TimeSpan.FromMinutes(1), Ct);
            rounds.Add(leased.Select(j => j.Id).ToArray());
            foreach (var job in leased)
            {
                await E2EProcessing.InvokeAsync(sp, job, Ct);
                processed += (await store.AckAsync(Server, [new LeaseRef(job.Id, job.LeaseId)], Ct)).Count;
            }
            // Ack сам продолжения не поднимает: до прохода resolver'а готовых задач нет.
            Assert.Empty(await store.DequeueAsync("e2e", 10, Server, TimeSpan.FromMinutes(1), Ct));
            await store.CountCompletionsAsync(500, Ct);                      // процесс без PgBolt.Server: проход resolver'а вручную
            await store.ResolveAsync(500, 5000, TimeSpan.FromSeconds(1), Ct);
        }

        Assert.Equal(result.Jobs.Count, processed);
        // Поколения готовы строго по очереди: корень, затем продолжение, затем члены batch. Пустой раунд между двумя
        // последними — штатный: запуск batch пишет сигнал Activated, членов поднимает следующий проход resolver'а (§6.4).
        var generations = rounds.Where(r => r.Length > 0).ToList();
        Assert.Equal(3, generations.Count);
        Assert.Equal([first.Id.Value], generations[0]);
        Assert.Equal([second.Id.Value], generations[1]);
        Assert.Equal(members.Select(m => m.Id.Value).Order(), generations[2].Order());
        Assert.Contains(ping, PingProbe.Messages);
        Assert.Contains(pong, PongProbe.Values);
        Assert.All(locals, n => Assert.Contains(n, LocalProbe.Values));
        Assert.Equal(first.Id, (await client.GetAsync(second.Id, Ct))!.ParentId);
        Assert.Equal(BatchState.Succeeded, (await client.GetBatchAsync(fan.Id, Ct))!.State);
    }
}

public sealed class WorkflowEndToEndTests_Direct(PostgresFixture pg) : WorkflowEndToEndTests(pg, ConnectionMode.Direct);
public sealed class WorkflowEndToEndTests_Bouncer(PostgresFixture pg) : WorkflowEndToEndTests(pg, ConnectionMode.Bouncer);
