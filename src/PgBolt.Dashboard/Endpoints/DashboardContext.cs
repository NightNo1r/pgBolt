using System.ComponentModel;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Postgres;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: данные запроса для оболочки и страниц.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record DashboardContext(string BasePath, bool CanAct, TimeSpan RefreshInterval, TimeSpan? EventsRetention,
    string CssUrl, string HtmxUrl, string ScriptUrl)
{
    /// <summary>
    /// Контекст из HttpContext.Items (кладёт фильтр доступа); если его нет — собирает сам и кеширует в Items. Запасной путь
    /// не вызывает IAuthorizationService: при заданной ActionsPolicy действия скрыты.
    /// </summary>
    internal static DashboardContext Of(HttpContext http)
    {
        if (http.Items.TryGetValue(typeof(DashboardContext), out var cached) && cached is DashboardContext existing)
            return existing;
        var services = http.RequestServices;
        var options = services.GetRequiredService<PgBoltDashboardOptions>();
        var assets = services.GetRequiredService<DashboardAssets>();
        var context = new DashboardContext(
            DashboardPaths.BasePath(http),
            CanAct: !options.ReadOnly && options.ActionsPolicy is null,
            options.RefreshInterval,
            services.GetRequiredService<PgBoltOptions>().EventsRetention,
            assets.RelativeUrl("pgbolt.css"),
            assets.RelativeUrl("htmx.min.js"),
            assets.RelativeUrl("pgbolt.js"));
        http.Items[typeof(DashboardContext)] = context;
        return context;
    }

    /// <summary>Контекст запроса с CanAct: не ReadOnly и (ActionsPolicy не задана или выполнена); кладёт его в Items.</summary>
    internal static async Task<DashboardContext> ResolveAsync(HttpContext http)
    {
        var context = Of(http);
        var options = http.RequestServices.GetRequiredService<PgBoltDashboardOptions>();
        if (options is { ReadOnly: false, ActionsPolicy: { } policy })
        {
            var result = await http.RequestServices.GetRequiredService<IAuthorizationService>().AuthorizeAsync(http.User, policy);
            http.Items[typeof(DashboardContext)] = context = context with { CanAct = result.Succeeded };
        }
        return context;
    }
}
