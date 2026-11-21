using System.Net;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Dashboard.Tests.Support;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Endpoints;

/// <summary>Компонент хоста для MapRazorComponents: маршрут "/" — только у компонентов с [Route].</summary>
[Route("/")]
public sealed class HostApp : ComponentBase
{
    protected override void BuildRenderTree(RenderTreeBuilder builder) => builder.AddContent(0, "host");
}

public sealed class AntiforgeryHostTests(PostgresFixture pg)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Form_post_works_in_host_with_razor_components_and_antiforgery()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, builder: b =>
        {
            b.Services.AddRazorComponents();
            b.Services.AddAntiforgery();
        }, beforeMap: app =>
        {
            app.UseAntiforgery();
            app.MapRazorComponents<HostApp>();
        });

        using var page = await host.GetAsync("/pgbolt/jobs/scheduled");
        Assert.True(page.StatusCode == HttpStatusCode.OK, await page.Content.ReadAsStringAsync(Ct));
        await AssertBulkPostAsync(db, host);
        await AssertEnqueuePostAsync(host);
        using var root = await host.GetAsync("/");
        Assert.Equal(HttpStatusCode.OK, root.StatusCode);
        Assert.Contains("host", await root.Content.ReadAsStringAsync(Ct), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Form_post_works_in_host_without_antiforgery_services()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db);

        await AssertBulkPostAsync(db, host);
        await AssertEnqueuePostAsync(host);
    }

    [Fact]
    public async Task Host_razor_pages_do_not_conflict()
    {
        await using var db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        await using var host = await DashboardHost.StartAsync(db, builder: b => b.Services.AddRazorPages(),
            beforeMap: app => app.MapRazorPages());

        using var page = await host.GetAsync("/pgbolt/jobs/scheduled");
        Assert.True(page.StatusCode == HttpStatusCode.OK, await page.Content.ReadAsStringAsync(Ct));
        await AssertBulkPostAsync(db, host);
    }

    /// <summary>Действие над выбранными (form-urlencoded) → 200 со сводкой.</summary>
    private static async Task AssertBulkPostAsync(TestDb db, DashboardHost host)
    {
        var id = (await new JobSeeder(db, host.Services.GetRequiredService<PgBoltStore>()).SeedJobsAsync(1, "dash.job", "default", 0,
            "scheduled"))[0];

        using var response = await host.PostAsync("/pgbolt/actions/jobs/trigger", [new("ids", id.ToString())]);
        var body = await response.Content.ReadAsStringAsync(Ct);

        Assert.True(response.StatusCode == HttpStatusCode.OK, body);
        Assert.Contains("Applied 1, not applicable 0", body, StringComparison.Ordinal);
    }

    /// <summary>Форма «Поставить» (form-urlencoded) → 200, задача создана.</summary>
    private static async Task AssertEnqueuePostAsync(DashboardHost host)
    {
        await host.Services.GetRequiredService<PgBoltStore>().PublishJobTypesAsync(
            [new JobTypeRecord("dash.order", TestSchemas.OrderJson, "orders", 3, 0, null)], "tests", Ct);

        using var response = await host.PostAsync("/pgbolt/actions/types/enqueue?name=dash.order", TestSchemas.OrderForm());
        var body = await response.Content.ReadAsStringAsync(Ct);

        Assert.True(response.StatusCode == HttpStatusCode.OK, body);
        var id = Guid.Parse(Regex.Match(body, "href=\"jobs/([0-9a-f-]{36})\"").Groups[1].Value);
        Assert.NotNull(await host.Services.GetRequiredService<IJobClient>().GetAsync(new JobId(id), Ct));
    }
}
