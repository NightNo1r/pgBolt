using System.ComponentModel;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Postgres;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>Internal dashboard model carrying request data for the shell and pages.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record DashboardContext(string BasePath, bool CanAct, TimeSpan RefreshInterval, TimeSpan? EventsRetention,
    string CssUrl, string HtmxUrl, string ScriptUrl)
{
    /// <summary>
    /// Gets the access filter's context from HttpContext.Items, or builds and caches it. The fallback does not call
    /// IAuthorizationService and hides actions whenever ActionsPolicy is set.
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

    /// <summary>Stores the request context in Items. CanAct requires write access and either no ActionsPolicy or a successful policy check.</summary>
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
