using System.Net;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;

namespace PgBolt.Dashboard.Security;

internal static class DashboardAccess
{
    /// <summary>Checks endpoint authorization via IAuthorizeData, AuthorizationPolicy, IAuthorizationRequirementData or a fallback policy. IAllowAnonymous overrides it (§5.3).</summary>
    public static bool HasAuthorization(EndpointMetadataCollection metadata, bool hasFallbackPolicy)
    {
        if (metadata.GetMetadata<IAllowAnonymous>() is not null)
            return false;
        return hasFallbackPolicy
               || metadata.GetMetadata<IAuthorizeData>() is not null
               || metadata.GetMetadata<AuthorizationPolicy>() is not null
               || metadata.GetMetadata<IAuthorizationRequirementData>() is not null;
    }

    /// <summary>Tests for loopback, including IPv4-mapped addresses, without reverse proxy headers. A missing address counts as remote.</summary>
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

    /// <summary>CSRF check: requires HX-Request and Sec-Fetch-Site: same-origin. If Sec-Fetch-Site is absent, Origin must match the request scheme, host and port.</summary>
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
