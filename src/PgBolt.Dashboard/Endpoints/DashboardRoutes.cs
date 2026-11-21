using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>
/// Endpoint marker and local template segment count used to resolve the base path. Asset endpoints check access but
/// skip the action policy.
/// </summary>
internal sealed record DashboardEndpoint(int LocalSegments, bool Asset = false);

internal static class DashboardRoutes
{
    public static RouteHandlerBuilder Get(RouteGroupBuilder g, string pattern, Delegate handler, bool asset = false) =>
        g.MapGet(pattern, handler).WithMetadata(new DashboardEndpoint(Segments(pattern), asset));

    public static RouteHandlerBuilder Post(RouteGroupBuilder g, string pattern, Delegate handler) =>
        g.MapPost(pattern, handler).WithMetadata(new DashboardEndpoint(Segments(pattern)));

    /// <summary>Parses an optional enum query parameter. Empty means null and an unknown value returns 400.</summary>
    public static T? OptionalEnum<T>(string? value, string what) where T : struct, Enum =>
        string.IsNullOrEmpty(value) ? null : ParseEnum<T>(value) ?? throw new DashboardBadRequestException($"invalid {what}: {value}");

    /// <summary>Looks up an enum member by case-insensitive name. Unknown names and undefined numeric values return null.</summary>
    public static T? ParseEnum<T>(string value) where T : struct, Enum =>
        Enum.TryParse<T>(value, ignoreCase: true, out var parsed) && Enum.IsDefined(parsed) ? parsed : null;

    private static int Segments(string pattern) => pattern.Split('/', StringSplitOptions.RemoveEmptyEntries).Length;
}
