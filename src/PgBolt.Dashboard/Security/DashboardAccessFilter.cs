using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Dashboard.Endpoints;

namespace PgBolt.Dashboard.Security;

/// <summary>
/// §5.3: контекст запроса в Items, затем локальность без авторизации, политика действий и CSRF для POST. Ассеты проходят
/// проверку локальности, но политика действий для них не вычисляется (ResolveAsync пропускается).
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
            return await DashboardView.ErrorAsync(http, 403, "доступ только с локальных запросов: настройте авторизацию группы dashboard");
        if (dashboard is null)
            return await next(context);   // ассет: только GET, CanAct не нужен
        if (HttpMethods.IsPost(http.Request.Method))
        {
            if (!dashboard.CanAct)
                return await DashboardView.ErrorAsync(http, 403, "действия недоступны: режим только для чтения или нет прав ActionsPolicy");
            if (!DashboardAccess.IsAllowedPost(http.Request))
                return await DashboardView.ErrorAsync(http, 403, "запрос отклонён проверкой CSRF");
        }
        return await next(context);
    }
}
