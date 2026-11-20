using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Dashboard;
using PgBolt.Dashboard.Endpoints;
using PgBolt.Dashboard.Security;
using PgBolt.Postgres;

namespace Microsoft.AspNetCore.Builder;

public static class DashboardEndpointRouteBuilderExtensions
{
    /// <summary>Endpoints dashboard PgBolt в одной группе; авторизацию просмотра задаёт хост (<c>.RequireAuthorization(...)</c>).</summary>
    /// <remarks>
    /// Хост за TLS-прокси применяет <c>ForwardedHeaders</c> (<c>X-Forwarded-Proto</c>, <c>X-Forwarded-Host</c>): иначе схема и
    /// хост запроса не совпадут с <c>Origin</c>, и браузеры без <c>Sec-Fetch-Site</c> получат 403 на действиях (CSRF-проверка).
    /// </remarks>
    public static RouteGroupBuilder MapPgBoltDashboard(this IEndpointRouteBuilder endpoints, string prefix = "/pgbolt")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        var services = endpoints.ServiceProvider;
        if (services.GetService<PgBoltOptions>() is null)
            throw new InvalidOperationException("PgBolt dashboard requires PgBolt core: call services.AddPgBolt(...) before MapPgBoltDashboard.");
        var options = services.GetService<PgBoltDashboardOptions>()
            ?? throw new InvalidOperationException("PgBolt dashboard is not registered: call services.AddPgBoltDashboard(...) before MapPgBoltDashboard.");

        if (options.ActionsPolicy is not null && services.GetService<IAuthorizationService>() is null)
            throw new InvalidOperationException("PgBolt dashboard: ActionsPolicy requires services.AddAuthorization().");

        // Фильтр доступа — внешний: отказ 403 не проходит через фильтр ошибок. Метка группы — проверка при старте только своих endpoint'ов.
        var marker = new object();
        var group = endpoints.MapGroup(prefix).DisableAntiforgery().WithMetadata(marker)
            .AddEndpointFilter<DashboardAccessFilter>()
            .AddEndpointFilter<DashboardErrorFilter>();
        DashboardRoutes.Get(group, "/assets/{hash}/{file}", (string hash, string file, DashboardAssets assets, HttpContext http) =>
        {
            if (!assets.TryGet(hash, file, out var asset))
                return Results.NotFound();
            http.Response.Headers.CacheControl = "public, max-age=31536000, immutable";
            return Results.Bytes(asset.Content, asset.ContentType);
        }, asset: true);
        OverviewEndpoints.Map(group);
        JobEndpoints.Map(group);
        BatchEndpoints.Map(group);
        RecurringEndpoints.Map(group);
        ActionEndpoints.Map(group);
        EnqueueEndpoints.Map(group);
        DashboardStartupCheck.Register(services, marker);
        return group;
    }
}
