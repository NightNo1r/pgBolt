using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Dashboard.Components.Parts;
using PgBolt.Dashboard.Endpoints;
using PgBolt.Dashboard.Forms;
using PgBolt.Dashboard.Tests.Support;
using static VerifyXunit.Verifier;

namespace PgBolt.Dashboard.Tests.Snapshots;

public sealed class SchemaFormSnapshotTests
{
    [Fact]
    public async Task Order_fields()
    {
        await using var services = new ServiceCollection().AddLogging().BuildServiceProvider();

        var html = await DashboardView.RenderAsync(new DefaultHttpContext { RequestServices = services }, typeof(SchemaFields), new Dictionary<string, object?>
        {
            [nameof(SchemaFields.Fields)] = SchemaForm.TryBuild(TestSchemas.OrderJson),
        });

        await Verify(html, "html");
    }
}
