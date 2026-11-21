using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using PgBolt.Dashboard.Components.Pages;
using PgBolt.Dashboard.Data;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>Schedule list, schedule details and run history. The schedule ID travels in the query string (deviation 8).</summary>
internal static class RecurringEndpoints
{
    public static void Map(RouteGroupBuilder g)
    {
        DashboardRoutes.Get(g, "/recurring", async (string? after, HttpContext http, DashboardStore store) =>
            await DashboardView.PageAsync<RecurringListPage>(http, "Recurring", new Dictionary<string, object?>
            {
                [nameof(RecurringListPage.Page)] = await store.ListRecurringAsync(after, http.RequestAborted),
                [nameof(RecurringListPage.Context)] = DashboardContext.Of(http),
            }));
        DashboardRoutes.Get(g, "/recurring/item", async (string? id, string? after, HttpContext http, IRecurringJobManager manager,
            DashboardStore store) =>
        {
            if (string.IsNullOrEmpty(id))
                throw new DashboardBadRequestException("id is required");
            var recurring = await manager.GetAsync(id, http.RequestAborted)
                ?? throw new DashboardNotFoundException($"recurring job {id} not found");
            return await DashboardView.PageAsync<RecurringPage>(http, "Schedule", new Dictionary<string, object?>
            {
                [nameof(RecurringPage.Recurring)] = recurring,
                [nameof(RecurringPage.Runs)] = await store.ListRecurringRunsAsync(id, after, http.RequestAborted),
                [nameof(RecurringPage.Context)] = DashboardContext.Of(http),
            });
        });
    }
}
