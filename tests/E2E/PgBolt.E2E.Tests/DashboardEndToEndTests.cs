using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.E2E.Contracts;
using PgBolt.Postgres.Jobs;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.E2E.Tests;

/// <summary>Dashboard поверх схемы E2E — страницы и действия в режимах Direct и Bouncer.</summary>
public abstract class DashboardEndToEndTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private static readonly Guid Server = Guid.NewGuid();
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, migrate: false);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Dashboard_pages_and_action_work_over_e2e_schema()
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddPgBolt(o =>
        {
            o.ConnectionString = pg.ConnectionString(mode);
            o.Schema = _db.Schema;
            o.AddJobs(PgBolt.E2E.JobsA.PgBoltJobRegistry.Register);
            o.AddJobs(PgBolt.E2E.JobsB.PgBoltJobRegistry.Register);
            o.AddJobs(PgBoltJobRegistry.Register);
        });
        builder.Services.AddPgBoltDashboard(o => o.AllowAnonymousRemote = true);
        await using var app = builder.Build();
        app.MapPgBoltDashboard();
        await app.StartAsync(Ct); // PgBoltInitializer мигрирует схему
        using var http = app.GetTestClient();

        var sp = app.Services;
        var client = sp.GetRequiredService<IJobClient>();
        var store = sp.GetRequiredService<PgBoltStore>();
        var registry = sp.GetRequiredService<JobRegistry>();
        await store.PublishJobTypesAsync(registry.ToJobTypeRecords(), "e2e-server", Ct);

        // Обработанная задача (до остальных, чтобы dequeue взял только её).
        var succeeded = (await client.EnqueueAsync(new PingArgs($"dashboard-{Guid.NewGuid():N}"), ct: Ct)).Id!.Value;
        Assert.Equal(1, await E2EProcessing.ProcessOnceAsync(sp, Server, Ct));
        var enqueued = (await client.EnqueueAsync(new PingArgs("enqueued"), ct: Ct)).Id!.Value;
        var scheduled = (await client.EnqueueAsync(new PingArgs("scheduled"), new EnqueueOptions { Delay = TimeSpan.FromHours(1) }, Ct)).Id!.Value;
        // Workflow: продолжение-задача и batch-продолжение корня; у batch — член, вложенный batch и продолжение.
        var root = client.Workflow().Enqueue(new PingArgs("root"));
        root.ContinueWith(new PongArgs(1));
        var fan = root.ContinueWithBatch(b =>
        {
            b.EnqueueMany([new LocalArgs(1)]);
            b.StartBatch(n => n.Enqueue(new LocalArgs(2)));
        });
        fan.ContinueWith(new PongArgs(2));
        await fan.SubmitAsync(Ct);
        await sp.GetRequiredService<IRecurringJobManager>().AddOrUpdateAsync("e2e.dashboard", "0 0 * * *", new PingArgs("recurring"), ct: Ct);

        string[] pages =
        [
            "/pgbolt/", "/pgbolt/overview/hour", "/pgbolt/overview/day", "/pgbolt/queues", "/pgbolt/servers", "/pgbolt/types",
            "/pgbolt/types/enqueue?name=e2e.ping",
            "/pgbolt/jobs/scheduled", "/pgbolt/jobs/enqueued?queue=e2e", "/pgbolt/jobs/processing", "/pgbolt/jobs/awaiting",
            "/pgbolt/jobs/succeeded", "/pgbolt/jobs/failed", "/pgbolt/jobs/cancelled",
            $"/pgbolt/jobs/{succeeded}", $"/pgbolt/jobs/{enqueued}", $"/pgbolt/jobs/{scheduled}",
            $"/pgbolt/jobs/{root.Id}", $"/pgbolt/jobs/{root.Id}/continuations", $"/pgbolt/jobs/{root.Id}/batch-continuations",
            "/pgbolt/batches", $"/pgbolt/batches/{fan.Id}", $"/pgbolt/batches/{fan.Id}/jobs", $"/pgbolt/batches/{fan.Id}/nested",
            $"/pgbolt/batches/{fan.Id}/continuations", $"/pgbolt/batches/{fan.Id}/batch-continuations",
            "/pgbolt/recurring", "/pgbolt/recurring/item?id=e2e.dashboard",
        ];
        foreach (var page in pages)
        {
            using var response = await http.GetAsync(page, Ct);
            Assert.True(response.StatusCode == HttpStatusCode.OK, $"GET {page}: {(int)response.StatusCode}");
        }

        Assert.Equal(JobState.Scheduled, (await client.GetAsync(scheduled, Ct))!.State);
        await PostAsync(http, $"/pgbolt/actions/jobs/{scheduled}/trigger?view=row", []);
        Assert.Equal(JobState.Enqueued, (await client.GetAsync(scheduled, Ct))!.State);

        var created = await PostAsync(http, "/pgbolt/actions/types/enqueue?name=e2e.ping", [new("json", """{"Message":"from-dashboard"}""")]);
        Assert.Contains("задача поставлена", created, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Dashboard_limit_pause_and_weight()
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddPgBolt(o =>
        {
            o.ConnectionString = pg.ConnectionString(mode);
            o.Schema = _db.Schema;
            o.AddJobs(PgBoltJobRegistry.Register);
        });
        builder.Services.AddPgBoltServer("e2e", o =>
        {
            o.Workers = 8;
            o.Queue("e2e-limited", LimitedProbe.Queue);
        });
        builder.Services.AddPgBoltDashboard(o => o.AllowAnonymousRemote = true);
        await using var app = builder.Build();
        app.MapPgBoltDashboard();
        await app.StartAsync(Ct);
        try
        {
            using var http = app.GetTestClient();

            var run = Guid.NewGuid().ToString("N");
            var probe = LimitedProbe.For(run);
            await app.Services.GetRequiredService<IJobClient>().EnqueueManyAsync(
                Enumerable.Range(0, 200).Select(n => new LimitedArgs(run, n)).ToList(), ct: Ct);
            await WaitUntilAsync(() => probe.Completed > 0, "jobs run before the pause");

            const string Limit = "/pgbolt/actions/queues/limit?queue=e2e-limited";
            await PostAsync(http, Limit, [new("op", "pause")]);
            // Задачи, взятые до паузы, дорабатывают; новых нет. Dequeue, начатый до коммита паузы, может запустить задачу
            // уже после первого Current == 0: ждём дольше одного dequeue и снова дожидаемся простоя.
            await WaitUntilAsync(() => probe.Current == 0, "jobs taken before the pause finish");
            await Task.Delay(TimeSpan.FromMilliseconds(500), Ct);
            await WaitUntilAsync(() => probe.Current == 0, "jobs dequeued while the pause was committing finish");
            var paused = probe.Completed;
            await Task.Delay(TimeSpan.FromSeconds(1), Ct);
            Assert.Equal(paused, probe.Completed);

            await PostAsync(http, Limit, [new("op", "reset")]);
            await WaitUntilAsync(() => probe.Completed > paused, "jobs run after the limit is reset to the code value");

            var limited = await PostAsync(http, Limit, [new("op", "set"), new("value", "2")]);
            Assert.Matches(@"\d+ / 2", limited);
            var weighted = await PostAsync(http, "/pgbolt/actions/queues/weight?queue=e2e-limited", [new("op", "set"), new("value", "7")]);
            Assert.Matches(@"<td>\s*7\s*<span class=""muted"">переопределён", weighted);
            await PostAsync(http, "/pgbolt/actions/queues/cache/reset", []);
        }
        finally
        {
            // DisposeAsync хоста не останавливает hosted services: без StopAsync сервер работал бы дальше над удалённой схемой.
            await app.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>Ждёт условие до 5 с.</summary>
    private static async Task WaitUntilAsync(Func<bool> condition, string because)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, $"timed out waiting for: {because}");
            await Task.Delay(10, Ct);
        }
    }

    /// <summary>POST действия htmx (HX-Request, same-origin): 200 и тело ответа.</summary>
    private static async Task<string> PostAsync(HttpClient http, string path, KeyValuePair<string, string>[] form)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, path) { Content = new FormUrlEncodedContent(form) };
        request.Headers.Add("HX-Request", "true");
        request.Headers.Add("Sec-Fetch-Site", "same-origin");
        using var response = await http.SendAsync(request, Ct);
        var body = await response.Content.ReadAsStringAsync(Ct);
        Assert.True(response.StatusCode == HttpStatusCode.OK, $"POST {path}: {(int)response.StatusCode} {body}");
        return body;
    }
}

[Collection(LimitedQueueCollection.Name)]
public sealed class DashboardEndToEndTests_Direct(PostgresFixture pg) : DashboardEndToEndTests(pg, ConnectionMode.Direct);
[Collection(LimitedQueueCollection.Name)]
public sealed class DashboardEndToEndTests_Bouncer(PostgresFixture pg) : DashboardEndToEndTests(pg, ConnectionMode.Bouncer);
