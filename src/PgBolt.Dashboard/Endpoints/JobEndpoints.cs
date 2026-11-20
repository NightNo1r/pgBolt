using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using PgBolt.Dashboard.Components.Pages;
using PgBolt.Dashboard.Components.Parts;
using PgBolt.Dashboard.Data;
using PgBolt.Dashboard.Views;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>Вкладки задач по состояниям, карточка задачи и её продолжения фрагментами (§6.1, §6.2, §9).</summary>
internal static class JobEndpoints
{
    public static void Map(RouteGroupBuilder g)
    {
        DashboardRoutes.Get(g, "/jobs/{list}", async (string list, string? queue, string? type, string? after, HttpContext http,
            DashboardStore store) =>
        {
            var kind = DashboardRoutes.ParseEnum<JobState>(list) ?? throw new DashboardNotFoundException($"unknown job list: {list}");
            queue = string.IsNullOrEmpty(queue) ? null : queue;
            type = string.IsNullOrEmpty(type) ? null : type;
            // Отклонение 22: Enqueued без очереди — выбор очереди, ListJobsAsync не вызывается.
            var chooseQueue = kind == JobState.Enqueued && queue is null;
            return await DashboardView.PageAsync<JobsPage>(http, "Задачи", new Dictionary<string, object?>
            {
                [nameof(JobsPage.Kind)] = kind,
                [nameof(JobsPage.Queue)] = queue,
                [nameof(JobsPage.Type)] = type,
                [nameof(JobsPage.Page)] = chooseQueue ? null : await store.ListJobsAsync(kind, queue, type, after, http.RequestAborted),
                [nameof(JobsPage.Queues)] = chooseQueue ? await store.ListQueuesAsync(http.RequestAborted) : null,
                [nameof(JobsPage.Context)] = DashboardContext.Of(http),
            });
        });
        DashboardRoutes.Get(g, "/jobs/{id:guid}", async (Guid id, HttpContext http, IJobClient client) =>
            await DashboardView.PageAsync<JobPage>(http, "Задача", new Dictionary<string, object?>
            {
                [nameof(JobPage.Job)] = await GetJobAsync(client, id, http.RequestAborted),
                [nameof(JobPage.Context)] = DashboardContext.Of(http),
            }));
        DashboardRoutes.Get(g, "/jobs/{id:guid}/continuations", async (Guid id, string? after, HttpContext http, IJobClient client,
            DashboardStore store) =>
        {
            var job = await GetJobAsync(client, id, http.RequestAborted);
            return await FragmentAsync(http, $"jobs/{id}/continuations",
                await store.ListContinuationsAsync(id, batchParent: false, job.CreatedAt, after, http.RequestAborted));
        });
        DashboardRoutes.Get(g, "/jobs/{id:guid}/batch-continuations", async (Guid id, string? after, HttpContext http, IJobClient client,
            DashboardStore store) =>
        {
            await GetJobAsync(client, id, http.RequestAborted);   // §10: несуществующий родитель — 404, как у соседних фрагментов
            return await FragmentAsync(http, $"jobs/{id}/batch-continuations",
                await store.ListBatchContinuationsAsync(id, after, http.RequestAborted));
        });
    }

    /// <summary>Порция связанных задач (RelatedJobs) — фрагмент без &lt;title&gt;.</summary>
    internal static Task<IResult> FragmentAsync(HttpContext http, string baseUrl, Page<RelatedJobRow> page) =>
        DashboardView.PageAsync<RelatedJobs>(http, "", new Dictionary<string, object?>
        {
            [nameof(RelatedJobs.Page)] = page,
            [nameof(RelatedJobs.BaseUrl)] = baseUrl,
        }, partial: true);

    /// <summary>Порция связанных batches (RelatedBatches) — фрагмент без &lt;title&gt;.</summary>
    internal static Task<IResult> FragmentAsync(HttpContext http, string baseUrl, Page<BatchRow> page) =>
        DashboardView.PageAsync<RelatedBatches>(http, "", new Dictionary<string, object?>
        {
            [nameof(RelatedBatches.Page)] = page,
            [nameof(RelatedBatches.BaseUrl)] = baseUrl,
        }, partial: true);

    internal static async Task<JobInfo> GetJobAsync(IJobClient client, Guid id, CancellationToken ct) =>
        await client.GetAsync(new JobId(id), ct) ?? throw new DashboardNotFoundException($"job {id} not found");
}
