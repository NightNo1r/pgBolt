using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Dashboard.Components.Parts;
using PgBolt.Dashboard.Endpoints;
using PgBolt.Dashboard.Views;
using static VerifyXunit.Verifier;

namespace PgBolt.Dashboard.Tests.Snapshots;

public sealed class JobSnapshotTests
{
    private static readonly DashboardContext Context = new("/pgbolt", CanAct: false, TimeSpan.FromSeconds(5),
        TimeSpan.FromDays(7), "assets/0/pgbolt.css", "assets/0/htmx.min.js", "assets/0/pgbolt.js");

    private static readonly DateTime At = new(2026, 10, 5, 11, 1, 0, DateTimeKind.Utc);

    private static async Task<string> RenderAsync(Type component, Dictionary<string, object?> parameters)
    {
        await using var services = new ServiceCollection().AddLogging().BuildServiceProvider();
        return await DashboardView.RenderAsync(new DefaultHttpContext { RequestServices = services }, component, parameters);
    }

    [Fact]
    public async Task Failed_row()
    {
        var row = new JobListRow(Guid.Parse("0199b3a0-0000-7000-8000-000000000001"), "reports.daily", "default", 5, 3, 3, At,
            null, At.AddSeconds(5), Guid.Parse("0199b3a0-0000-7000-8000-0000000000aa"), At.AddSeconds(9), false, JobEventKind.Failed,
            "disk <full>");

        var html = await RenderAsync(typeof(JobRow), new()
        {
            [nameof(JobRow.Row)] = row,
            [nameof(JobRow.Kind)] = JobState.Failed,
            [nameof(JobRow.Context)] = Context,
        });

        await Verify(html, "html");
    }

    [Fact]
    public async Task Card()
    {
        var at = new DateTimeOffset(At);
        var job = new JobInfo(new JobId(Guid.Parse("0199b3a0-0000-7000-8000-000000000002")), "reports.daily", "default", 5, JobState.Failed,
            2, 3, at, at, at.AddSeconds(5), at.AddSeconds(9), Guid.Parse("0199b3a0-0000-7000-8000-0000000000aa"), "report:2026-10-05", false,
            JsonDocument.Parse("""{"day":"2026-10-05","имя":"отчёт"}""").RootElement,
            [
                new JobEventInfo(JobEventKind.Retry, at.AddSeconds(3), Guid.Parse("0199b3a0-0000-7000-8000-0000000000aa"), 1, "timeout", null),
                new JobEventInfo(JobEventKind.Failed, at.AddSeconds(9), Guid.Parse("0199b3a0-0000-7000-8000-0000000000aa"), 2, "boom",
                    "System.Exception: boom\n   at Job.Run()"),
            ],
            ParentId: new JobId(Guid.Parse("0199b3a0-0000-7000-8000-000000000003")),
            BatchId: new BatchId(Guid.Parse("0199b3a0-0000-7000-8000-000000000004")),
            Continuation: ContinuationCondition.OnAnyFinished);

        var html = await RenderAsync(typeof(JobCard), new()
        {
            [nameof(JobCard.Job)] = job,
            [nameof(JobCard.Context)] = Context,
        });

        await Verify(html, "html");
    }
}
