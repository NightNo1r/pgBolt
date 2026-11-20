using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using PgBolt.Dashboard.Components.Pages;
using PgBolt.Dashboard.Components.Parts;
using PgBolt.Dashboard.Data;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>Batches с фильтром состояния, карточка batch и её списки фрагментами (§6.1, §6.2).</summary>
internal static class BatchEndpoints
{
    public static void Map(RouteGroupBuilder g)
    {
        DashboardRoutes.Get(g, "/batches", async (string? state, string? after, HttpContext http, DashboardStore store) =>
        {
            var filter = DashboardRoutes.OptionalEnum<BatchState>(state, "batch state");
            return await DashboardView.PageAsync<BatchesPage>(http, "Batches", new Dictionary<string, object?>
            {
                [nameof(BatchesPage.State)] = filter,
                [nameof(BatchesPage.Page)] = await store.ListBatchesAsync(filter, after, http.RequestAborted),
            });
        });
        DashboardRoutes.Get(g, "/batches/{id:guid}", async (Guid id, HttpContext http, IJobClient client) =>
            await DashboardView.PageAsync<BatchPage>(http, "Batch", new Dictionary<string, object?>
            {
                [nameof(BatchPage.Batch)] = await GetBatchAsync(client, id, http.RequestAborted),
                [nameof(BatchPage.Context)] = DashboardContext.Of(http),
            }));
        DashboardRoutes.Get(g, "/batches/{id:guid}/jobs", async (Guid id, string? after, HttpContext http, IJobClient client,
            DashboardStore store) =>
        {
            var batch = await GetBatchAsync(client, id, http.RequestAborted);
            return await JobEndpoints.FragmentAsync(http, $"batches/{id}/jobs",
                await store.ListBatchJobsAsync(id, batch.CreatedAt, after, http.RequestAborted));
        });
        DashboardRoutes.Get(g, "/batches/{id:guid}/nested", async (Guid id, string? after, HttpContext http, IJobClient client,
            DashboardStore store) =>
        {
            await GetBatchAsync(client, id, http.RequestAborted);   // §10: несуществующий batch — 404
            return await JobEndpoints.FragmentAsync(http, $"batches/{id}/nested",
                await store.ListNestedBatchesAsync(id, after, http.RequestAborted));
        });
        DashboardRoutes.Get(g, "/batches/{id:guid}/continuations", async (Guid id, string? after, HttpContext http, IJobClient client,
            DashboardStore store) =>
        {
            var batch = await GetBatchAsync(client, id, http.RequestAborted);
            return await JobEndpoints.FragmentAsync(http, $"batches/{id}/continuations",
                await store.ListContinuationsAsync(id, batchParent: true, batch.CreatedAt, after, http.RequestAborted));
        });
        DashboardRoutes.Get(g, "/batches/{id:guid}/batch-continuations", async (Guid id, string? after, HttpContext http, IJobClient client,
            DashboardStore store) =>
        {
            await GetBatchAsync(client, id, http.RequestAborted);
            return await JobEndpoints.FragmentAsync(http, $"batches/{id}/batch-continuations",
                await store.ListBatchContinuationsAsync(id, after, http.RequestAborted));
        });
    }

    internal static async Task<BatchInfo> GetBatchAsync(IJobClient client, Guid id, CancellationToken ct) =>
        await client.GetBatchAsync(new BatchId(id), ct) ?? throw new DashboardNotFoundException($"batch {id} not found");
}
