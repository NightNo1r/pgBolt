using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace PgBolt.Dashboard.Endpoints;

internal static class DashboardPaths
{
    /// <summary>
    /// Базовый путь dashboard без хвостового «/»: PathBase + первые N сегментов пути запроса, где N — число сегментов шаблона
    /// endpoint'а минус число сегментов его локального шаблона (работает при UsePathBase и при вложении в группу хоста).
    /// </summary>
    public static string BasePath(HttpContext http)
    {
        var endpoint = (RouteEndpoint)http.GetEndpoint()!;
        var local = endpoint.Metadata.GetMetadata<DashboardEndpoint>()?.LocalSegments ?? throw new InvalidOperationException(
            $"PgBolt dashboard: endpoint '{endpoint.DisplayName}' is not a dashboard endpoint; map host endpoints outside the dashboard group.");
        var keep = endpoint.RoutePattern.PathSegments.Count - local;
        var pathBase = http.Request.PathBase.Value ?? "";
        if (keep == 0)
            return pathBase;
        var segments = (http.Request.Path.Value ?? "").Split('/', StringSplitOptions.RemoveEmptyEntries);
        return pathBase + "/" + string.Join('/', segments[..keep]);
    }
}
