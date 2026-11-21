using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Dashboard.Endpoints;

namespace PgBolt.Dashboard.Security;

/// <summary>
/// Stores the context in Items, checks locality for unauthenticated groups, then checks action policy and CSRF for
/// POST (§5.3). Assets undergo the locality check but skip ResolveAsync.
/// </summary>
internal sealed class DashboardAccessFilter(PgBoltDashboardOptions options) : IEndpointFilter
{
    public async ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        var http = context.HttpContext;
        var metadata = http.GetEndpoint()!.Metadata;
        var dashboard = metadata.GetMetadata<DashboardEndpoint>() is { Asset: true } ? null : await DashboardContext.ResolveAsync(http);
        // Хост без AddAuthorization (CreateSlimBuilder) не регистрирует провайдер политик.
        var policyProvider = http.RequestServices.GetService<IAuthorizationPolicyProvider>();
        var hasFallback = policyProvider is not null && await policyProvider.GetFallbackPolicyAsync() is not null;
        if (!options.AllowAnonymousRemote && !DashboardAccess.HasAuthorization(metadata, hasFallback) && !DashboardAccess.IsLocal(http))
            return await DashboardView.ErrorAsync(http, 403, "Local requests only. Configure authorization on the dashboard route group.");
        if (dashboard is null)
            return await next(context);   // ассет: только GET, CanAct не нужен
        if (HttpMethods.IsPost(http.Request.Method))
        {
            if (!dashboard.CanAct)
                return await DashboardView.ErrorAsync(http, 403, "Actions are unavailable in read-only mode or without ActionsPolicy permission");
            if (!DashboardAccess.IsAllowedPost(http.Request))
                return await DashboardView.ErrorAsync(http, 403, "Request rejected by CSRF protection");
        }
        return await next(context);
    }
}
