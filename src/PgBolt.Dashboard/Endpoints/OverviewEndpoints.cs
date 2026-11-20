using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using PgBolt.Dashboard.Components.Pages;
using PgBolt.Dashboard.Components.Parts;
using PgBolt.Dashboard.Data;
using PgBolt.Dashboard.Views;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>Обзор (час — polling, сутки — отдельный фрагмент), очереди, серверы, типы задач (§6.1, §9).</summary>
internal static class OverviewEndpoints
{
    public static void Map(RouteGroupBuilder g)
    {
        DashboardRoutes.Get(g, "/", async (HttpContext http, DashboardStore store) =>
            await DashboardView.PageAsync<OverviewPage>(http, "Обзор", new Dictionary<string, object?>
            {
                [nameof(OverviewPage.Data)] = await store.GetOverviewAsync(http.RequestAborted),
                [nameof(OverviewPage.Context)] = DashboardContext.Of(http),
            }));
        DashboardRoutes.Get(g, "/overview/hour", async (HttpContext http, DashboardStore store) =>
            await DashboardView.PageAsync<HourPanel>(http, "Обзор", new Dictionary<string, object?>
            {
                [nameof(HourPanel.Data)] = await store.GetOverviewAsync(http.RequestAborted),
            }, partial: true));
        DashboardRoutes.Get(g, "/overview/day", async (HttpContext http, DashboardStore store) =>
            await DashboardView.PageAsync<DayPanel>(http, "Обзор", new Dictionary<string, object?>
            {
                [nameof(DayPanel.Points)] = await store.GetThroughputDayAsync(http.RequestAborted),
            }, partial: true));
        DashboardRoutes.Get(g, "/queues", async (HttpContext http, DashboardStore store, IQueueSettingsManager settings) =>
            await DashboardView.PageAsync<QueuesPage>(http, "Очереди", new Dictionary<string, object?>
            {
                [nameof(QueuesPage.Queues)] = await QueueLinesAsync(store, settings, null, http.RequestAborted),
                [nameof(QueuesPage.Context)] = DashboardContext.Of(http),
            }));
        DashboardRoutes.Get(g, "/queues/rows", async (HttpContext http, DashboardStore store, IQueueSettingsManager settings) =>
            await DashboardView.PageAsync<QueueRows>(http, "Очереди", new Dictionary<string, object?>
            {
                [nameof(QueueRows.Queues)] = await QueueLinesAsync(store, settings, null, http.RequestAborted),
                [nameof(QueueRows.Context)] = DashboardContext.Of(http),
            }, partial: true));
        DashboardRoutes.Get(g, "/servers", async (HttpContext http, DashboardStore store) =>
            await DashboardView.PageAsync<ServersPage>(http, "Серверы", new Dictionary<string, object?>
            {
                [nameof(ServersPage.Data)] = await store.GetServersAsync(http.RequestAborted),
            }));
        DashboardRoutes.Get(g, "/types", async (HttpContext http, DashboardStore store) =>
            await DashboardView.PageAsync<JobTypesPage>(http, "Типы задач", new Dictionary<string, object?>
            {
                [nameof(JobTypesPage.Types)] = await store.ListJobTypesAsync(http.RequestAborted),
                [nameof(JobTypesPage.Context)] = DashboardContext.Of(http),
            }));
    }

    /// <summary>
    /// Строки экрана очередей: снимок, настройки (через кэш менеджера, если он включён) и живое число взятых строк — только у
    /// очередей со строкой настроек. queue — только эта очередь (ordinal): пусто, если её больше нет в списке.
    /// </summary>
    internal static async Task<IReadOnlyList<QueueLine>> QueueLinesAsync(DashboardStore store, IQueueSettingsManager settings,
        string? queue, CancellationToken ct)
    {
        var stats = await store.ListQueuesAsync(ct);
        if (queue is not null)
            stats = [.. stats.Where(s => s.Name == queue)];
        if (stats.Count == 0)
            return [];
        var byQueue = queue is null
            ? (await settings.ListAsync(ct)).ToDictionary(s => s.Queue, StringComparer.Ordinal)
            : await settings.GetAsync(queue, ct) is { } one ? new Dictionary<string, QueueSettings> { [queue] = one } : [];
        var withSettings = stats.Select(s => s.Name).Where(byQueue.ContainsKey).ToArray();
        var leased = withSettings.Length == 0
            ? new Dictionary<string, int>()
            : await store.CountLeasedAsync(withSettings, ct);
        return [.. stats.Select(s => byQueue.TryGetValue(s.Name, out var x)
            ? new QueueLine(s, x, leased.GetValueOrDefault(s.Name))
            : new QueueLine(s, null, null))];
    }
}
