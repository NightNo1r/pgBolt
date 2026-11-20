using System.Net;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;

namespace PgBolt.Dashboard.Security;

internal static class DashboardAccess
{
    /// <summary>§5.3: авторизация на endpoint'е — IAuthorizeData / AuthorizationPolicy / IAuthorizationRequirementData или fallback-политика; IAllowAnonymous отменяет.</summary>
    public static bool HasAuthorization(EndpointMetadataCollection metadata, bool hasFallbackPolicy)
    {
        if (metadata.GetMetadata<IAllowAnonymous>() is not null)
            return false;
        return hasFallbackPolicy
               || metadata.GetMetadata<IAuthorizeData>() is not null
               || metadata.GetMetadata<AuthorizationPolicy>() is not null
               || metadata.GetMetadata<IAuthorizationRequirementData>() is not null;
    }

    /// <summary>Loopback (включая IPv4-mapped) без заголовков reverse proxy; null-адрес — нелокальный.</summary>
    public static bool IsLocal(HttpContext http)
    {
        var a = http.Connection.RemoteIpAddress;
        var headers = http.Request.Headers;
        return a is not null
               && IPAddress.IsLoopback(a.IsIPv4MappedToIPv6 ? a.MapToIPv4() : a)
               && !headers.ContainsKey("Forwarded")
               && !headers.ContainsKey("X-Forwarded-For")
               && !headers.ContainsKey("X-Real-IP");
    }

    /// <summary>CSRF: HX-Request и Sec-Fetch-Site: same-origin, а без него — Origin, совпадающий со схемой, хостом и портом запроса.</summary>
    public static bool IsAllowedPost(HttpRequest request)
    {
        if (request.Headers["HX-Request"] != "true")
            return false;
        if (request.Headers.TryGetValue("Sec-Fetch-Site", out var site))
            return site == "same-origin";
        if (!Uri.TryCreate(request.Headers.Origin.ToString(), UriKind.Absolute, out var origin))
            return false;
        var https = string.Equals(request.Scheme, "https", StringComparison.OrdinalIgnoreCase);
        return string.Equals(origin.Scheme, request.Scheme, StringComparison.OrdinalIgnoreCase)
               && string.Equals(origin.Host, request.Host.Host, StringComparison.OrdinalIgnoreCase)
               && origin.Port == (request.Host.Port ?? (https ? 443 : 80));
    }
}
